/*-------------------------------------------------------------------------
 *
 * jam.c
 *	  Implementation of just access method.
 *
 * Portions Copyright (c) 2024-2024, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/backend/access/jam/jam.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/jam.h"
#include "commands/vacuum.h"
#include "utils/index_selfuncs.h"
#include "catalog/pg_opclass.h"
#include "catalog/pg_opfamily.h"
#include "utils/catcache.h"
#include "access/amvalidate.h"
#include "utils/syscache.h"
#include "access/htup_details.h"
#include "catalog/pg_amproc.h"
#include "catalog/index.h"
#include "catalog/pg_amop.h"
#include "utils/regproc.h"
#include "storage/bufmgr.h"
#include "access/tableam.h"
#include "access/reloptions.h"
#include "utils/memutils.h"
#include "utils/fmgrprotos.h"

/*
 * Jam handler function: return IndexAmRoutine with access method parameters
 * and callbacks.
 */
Datum
jamhandler(PG_FUNCTION_ARGS)
{
	IndexAmRoutine *amroutine = makeNode(IndexAmRoutine);

	amroutine->amstrategies = JAM_NSTRATEGIES;
	amroutine->amsupport = JAM_NPROC;
	amroutine->amoptsprocnum = JAM_OPTIONS_PROC;
	amroutine->amcanorder = false;
	amroutine->amcanorderbyop = false;
	amroutine->amcanbackward = false;
	amroutine->amcanunique = false;
	amroutine->amcanmulticol = true;
	amroutine->amoptionalkey = true;
	amroutine->amsearcharray = false;
	amroutine->amsearchnulls = false;
	amroutine->amstorage = false;
	amroutine->amclusterable = false;
	amroutine->ampredlocks = false;
	amroutine->amcanparallel = false;
	amroutine->amcanbuildparallel = false;
	amroutine->amcaninclude = true;
	amroutine->amusemaintenanceworkmem = false;
	amroutine->amparallelvacuumoptions =
			VACUUM_OPTION_PARALLEL_BULKDEL | VACUUM_OPTION_PARALLEL_CLEANUP;
	amroutine->amkeytype = InvalidOid;

	amroutine->ambuild = jambuild;
	amroutine->ambuildempty = jambuildempty;
	amroutine->aminsert = jaminsert;
	amroutine->aminsertcleanup = NULL;
	amroutine->ambulkdelete = jambulkdelete;
	amroutine->amvacuumcleanup = jamvacuumcleanup;
	amroutine->amcanreturn = NULL;
	amroutine->amcostestimate = jamcostestimate;
	amroutine->amoptions = jamoptions;
	amroutine->amproperty = NULL;
	amroutine->ambuildphasename = NULL;
	amroutine->amvalidate = jamvalidate;
	amroutine->amadjustmembers = NULL;
	amroutine->ambeginscan = jambeginscan;
	amroutine->amrescan = jamrescan;
	amroutine->amgettuple = NULL;
	amroutine->amgetbitmap = NULL;
	amroutine->amendscan = jamendscan;
	amroutine->ammarkpos = NULL;
	amroutine->amrestrpos = NULL;
	amroutine->amestimateparallelscan = NULL;
	amroutine->aminitparallelscan = NULL;
	amroutine->amparallelrescan = NULL;

	PG_RETURN_POINTER(amroutine);
}

bool
jamvalidate(Oid opclassoid)
{
	bool result = true;
	HeapTuple classtup;
	Form_pg_opclass classform;
	Oid opfamilyoid;
	HeapTuple familytup;
	Form_pg_opfamily familyform;
	char *opfamilyname;
	CatCList *proclist,
			*oprlist;
	int i;

	/* Fetch opclass information */
	classtup = SearchSysCache1(CLAOID, ObjectIdGetDatum(opclassoid));
	if (!HeapTupleIsValid(classtup))
		elog(ERROR, "cache lookup failed for operator class %u", opclassoid);
	classform = (Form_pg_opclass) GETSTRUCT(classtup);

	opfamilyoid = classform->opcfamily;


	/* Fetch opfamily information */
	familytup = SearchSysCache1(OPFAMILYOID, ObjectIdGetDatum(opfamilyoid));
	if (!HeapTupleIsValid(familytup))
		elog(ERROR, "cache lookup failed for operator family %u", opfamilyoid);
	familyform = (Form_pg_opfamily) GETSTRUCT(familytup);

	opfamilyname = NameStr(familyform->opfname);

	/* Fetch all operators and support functions of the opfamily */
	oprlist = SearchSysCacheList1(AMOPSTRATEGY, ObjectIdGetDatum(opfamilyoid));
	proclist = SearchSysCacheList1(AMPROCNUM, ObjectIdGetDatum(opfamilyoid));

	/* Check individual operators */
	for (i = 0; i < oprlist->n_members; i++)
	{
		HeapTuple oprtup = &oprlist->members[i]->tuple;
		Form_pg_amop oprform = (Form_pg_amop) GETSTRUCT(oprtup);

		/* Check it's allowed strategy for jam */
		if (oprform->amopstrategy < 1 ||
			oprform->amopstrategy > JAM_NSTRATEGIES)
		{
			ereport(INFO,
					(errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
							errmsg("jam opfamily %s contains operator %s with invalid strategy number %d",
								   opfamilyname,
								   format_operator(oprform->amopopr),
								   oprform->amopstrategy)));
			result = false;
		}

		/* jam doesn't support ORDER BY operators */
		if (oprform->amoppurpose != AMOP_SEARCH ||
			OidIsValid(oprform->amopsortfamily))
		{
			ereport(INFO,
					(errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
							errmsg("jam opfamily %s contains invalid ORDER BY specification for operator %s",
								   opfamilyname,
								   format_operator(oprform->amopopr))));
			result = false;
		}

		/* Check operator signature --- same for all jam strategies */
		if (!check_amop_signature(oprform->amopopr, BOOLOID,
								  oprform->amoplefttype,
								  oprform->amoprighttype))
		{
			ereport(INFO,
					(errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
							errmsg("jam opfamily %s contains operator %s with wrong signature",
								   opfamilyname,
								   format_operator(oprform->amopopr))));
			result = false;
		}
	}


	ReleaseCatCacheList(proclist);
	ReleaseCatCacheList(oprlist);
	ReleaseSysCache(familytup);
	ReleaseSysCache(classtup);

	return result;
}


void
JamFillMetapage(Relation index, Page metaPage)
{
	JamMetaPageData *metadata;

	JamInitPage(metaPage, JAM_META);
	metadata = JamPageGetMeta(metaPage);
	memset(metadata, 0, sizeof(JamMetaPageData));
	metadata->magickNumber = JAM_MAGICK_NUMBER;
	((PageHeader) metaPage)->pd_lower += sizeof(JamMetaPageData);
}

void
JamInitMetapage(Relation index, ForkNumber forknum)
{
	Buffer metaBuffer;
	Page metaPage;
	GenericXLogState *state;

	/*
	 * Make a new page; since it is first page it should be associated with
	 * block number 0 (JAM_METAPAGE_BLKNO).  No need to hold the extension
	 * lock because there cannot be concurrent inserters yet.
	 */
	metaBuffer = ReadBufferExtended(index, forknum, P_NEW, RBM_NORMAL, NULL);
	LockBuffer(metaBuffer, BUFFER_LOCK_EXCLUSIVE);
	Assert(BufferGetBlockNumber(metaBuffer) == JAM_METAPAGE_BLKNO);

	/* Initialize contents of meta page */
	state = GenericXLogStart(index);
	metaPage = GenericXLogRegisterBuffer(state, metaBuffer,
										 GENERIC_XLOG_FULL_IMAGE);
	JamFillMetapage(index, metaPage);
	GenericXLogFinish(state);

	UnlockReleaseBuffer(metaBuffer);
}

/*
 * Initialize any page of a jam index.
 */
void
JamInitPage(Page page, uint16 flags)
{
	JamPageOpaque opaque;

	PageInit(page, BLCKSZ, sizeof(JamPageOpaqueData));

	opaque = JamPageGetOpaque(page);
	opaque->flags = flags;
	opaque->jam_page_id = JAM_PAGE_ID;
}

static bool
JamPageAddItem(Page page, JamTuple *tuple)
{
	JamTuple *itup;
	JamPageOpaque opaque;
	Pointer ptr;

	/* We shouldn't be pointed to an invalid page */
	Assert(!PageIsNew(page));

	/* Does new tuple fit on the page? */
	if (JamPageGetFreeSpace(state, page) < sizeof(JamTuple))
		return false;

	/* Copy new tuple to the end of page */
	opaque = JamPageGetOpaque(page);
	itup = JamPageGetTuple(page, opaque->maxoff + 1);
	memcpy((Pointer) itup, (Pointer) tuple, sizeof(JamTuple));

	/* Adjust maxoff and pd_lower */
	opaque->maxoff++;
	ptr = (Pointer) JamPageGetTuple(page, opaque->maxoff + 1);
	((PageHeader) page)->pd_lower = ptr - page;

	/* Assert we didn't overrun available space */
	Assert(((PageHeader) page)->pd_lower <= ((PageHeader) page)->pd_upper);
	return true;
}

bool
jaminsert(Relation index, Datum *values, bool *isnull,
		  ItemPointer ht_ctid, Relation heapRel,
		  IndexUniqueCheck checkUnique,
		  bool indexUnchanged,
		  struct IndexInfo *indexInfo)
{
	JamTuple *itup;
	MemoryContext oldCtx;
	MemoryContext insertCtx;
	JamMetaPageData *metaData;
	Buffer buffer,
			metaBuffer;
	Page page;
	GenericXLogState *state;
	uint16 prevBlkNo;
	Assert(indexInfo->ii_Auxiliary);

	insertCtx = AllocSetContextCreate(CurrentMemoryContext,
									  "Jam insert temporary context",
									  ALLOCSET_DEFAULT_SIZES);

	oldCtx = MemoryContextSwitchTo(insertCtx);

	itup = (JamTuple *) palloc0(sizeof(JamTuple));
	itup->heapPtr = *ht_ctid;

	metaBuffer = ReadBuffer(index, JAM_METAPAGE_BLKNO);

	for (;;)
	{
		LockBuffer(metaBuffer, BUFFER_LOCK_SHARE);
		metaData = JamPageGetMeta(BufferGetPage(metaBuffer));
		prevBlkNo = metaData->lastBlkNo;

		if (metaData->lastBlkNo > 0)
		{
			BlockNumber blkno = metaData->lastBlkNo;
			/* Don't hold metabuffer lock while doing insert */
			LockBuffer(metaBuffer, BUFFER_LOCK_UNLOCK);

			buffer = ReadBuffer(index, blkno);
			LockBuffer(buffer, BUFFER_LOCK_EXCLUSIVE);

			state = GenericXLogStart(index);
			page = GenericXLogRegisterBuffer(state, buffer, 0);

			if (PageIsNew(page))
				JamInitPage(page, 0);

			if (JamPageAddItem(page, itup))
			{
				/* Success!  Apply the change, clean up, and exit */
				GenericXLogFinish(state);
				UnlockReleaseBuffer(buffer);
				ReleaseBuffer(metaBuffer);
				MemoryContextSwitchTo(oldCtx);
				MemoryContextDelete(insertCtx);
				return false;
			}

			/* Didn't fit, must try other pages */
			GenericXLogAbort(state);
			UnlockReleaseBuffer(buffer);
		} else
		{
			/* No entries in notFullPage */
			LockBuffer(metaBuffer, BUFFER_LOCK_UNLOCK);
		}

		LockBuffer(metaBuffer, BUFFER_LOCK_EXCLUSIVE);
		metaData = JamPageGetMeta(BufferGetPage(metaBuffer));
		if (prevBlkNo != metaData->lastBlkNo)
		{
			// someone else inserted the new page into the index, lets try again
			LockBuffer(metaBuffer, BUFFER_LOCK_UNLOCK);
			continue;
		} else
		{
			state = GenericXLogStart(index);
			/* Must extend the file */
			buffer = ExtendBufferedRel(BMR_REL(index), MAIN_FORKNUM, NULL,
									   EB_LOCK_FIRST);

			page = GenericXLogRegisterBuffer(state, buffer, GENERIC_XLOG_FULL_IMAGE);
			JamInitPage(page, 0);

			if (!JamPageAddItem(page, itup))
			{
				/* We shouldn't be here since we're inserting to an empty page */
				elog(ERROR, "could not add new jam tuple to empty page");
			}
			metaData->lastBlkNo = BufferGetBlockNumber(buffer);
			GenericXLogFinish(state);

			UnlockReleaseBuffer(buffer);
			UnlockReleaseBuffer(metaBuffer);

			MemoryContextSwitchTo(oldCtx);
			MemoryContextDelete(insertCtx);

			return false;
		}
	}
}

IndexScanDesc jambeginscan(Relation r, int nkeys, int norderbys)
{
	ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("\"%s\" is not a not implemented", __func__)));
}

void
jamrescan(IndexScanDesc scan, ScanKey scankey, int nscankeys,
		  ScanKey orderbys, int norderbys)
{
	ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("\"%s\" is not a not implemented", __func__)));
}

void jamendscan(IndexScanDesc scan)
{
	ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("\"%s\" is not a not implemented", __func__)));
}

IndexBuildResult *jambuild(Relation heap, Relation index,
						   struct IndexInfo *indexInfo)
{
	IndexBuildResult *result;

	Assert(indexInfo->ii_Auxiliary);
	JamInitMetapage(index, MAIN_FORKNUM);

	result = (IndexBuildResult *) palloc(sizeof(IndexBuildResult));
	result->heap_tuples = 0;
	result->index_tuples = 0;
	return result;
}

void jambuildempty(Relation index)
{
	JamInitMetapage(index, INIT_FORKNUM);
}

IndexBulkDeleteResult *jambulkdelete(IndexVacuumInfo *info,
									 IndexBulkDeleteResult *stats,
									 IndexBulkDeleteCallback callback,
									 void *callback_state)
{
	Relation index = info->index;
	BlockNumber blkno, npages;
	Buffer buffer;
	Page page;

	if (!info->validate_index)
	{
		ereport(WARNING, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				errmsg("\"%s\" is not a not implemented, seems like this index need to be dropped", __func__)));
		return NULL;
	}

	if (stats == NULL)
		stats = (IndexBulkDeleteResult *) palloc0(sizeof(IndexBulkDeleteResult));

	/*
	 * Iterate over the pages. We don't care about concurrently added pages,
	 * because TODO
	 */
	npages = RelationGetNumberOfBlocks(index);
	for (blkno = JAM_HEAD_BLKNO; blkno < npages; blkno++)
	{
		JamTuple *itup, *itupEnd;

		vacuum_delay_point();

		buffer = ReadBufferExtended(index, MAIN_FORKNUM, blkno,
									RBM_NORMAL, info->strategy);

		LockBuffer(buffer, BUFFER_LOCK_SHARE);
		page = BufferGetPage(buffer);

		if (PageIsNew(page))
		{
			UnlockReleaseBuffer(buffer);
			continue;
		}

		itup = JamPageGetTuple(page, FirstOffsetNumber);
		itupEnd = JamPageGetTuple(page, OffsetNumberNext(JamPageGetMaxOffset(page)));
		while (itup < itupEnd)
		{
			/* Do we have to delete this tuple? */
			if (callback(&itup->heapPtr, callback_state))
			{
				ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("we never delete in jam")));
			}

			itup = JamPageGetNextTuple(itup);
		}

		UnlockReleaseBuffer(buffer);
	}

	return stats;
}

IndexBulkDeleteResult *jamvacuumcleanup(IndexVacuumInfo *info,
										IndexBulkDeleteResult *stats)
{
	ereport(WARNING, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			errmsg("\"%s\" is not a not implemented, seems like this index need to be dropped", __func__)));
	return NULL;
}

bytea *jamoptions(Datum reloptions, bool validate)
{
	return NULL;
}

void jamcostestimate(PlannerInfo *root, IndexPath *path,
					 double loop_count, Cost *indexStartupCost,
					 Cost *indexTotalCost, Selectivity *indexSelectivity,
					 double *indexCorrelation, double *indexPages)
{
	ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("\"%s\" is not a not implemented", __func__)));
}
