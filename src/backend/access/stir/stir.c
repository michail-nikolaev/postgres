/*-------------------------------------------------------------------------
 *
 * stir.c
 *	  Implementation of Short-Term Index Replacement.
 *
 * STIR is a specialized access method type designed for temporary storage
 * of TID values during concurernt index build operations.
 *
 * The typical lifecycle of a STIR index is:
 * 1. created as an auxiliary index for CIC/RIC
 * 2. accepts inserts for a period
 * 3. stirbulkdelete called during index validation phase
 * 5. gets dropped
 *
 * Portions Copyright (c) 2025, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/backend/access/stir/stir.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/stir.h"
#include "miscadmin.h"
#include "access/amvalidate.h"
#include "access/htup_details.h"
#include "access/tableam.h"
#include "catalog/index.h"
#include "catalog/pg_amop.h"
#include "catalog/pg_opclass.h"
#include "catalog/pg_opfamily.h"
#include "commands/vacuum.h"
#include "storage/bufmgr.h"
#include "utils/catcache.h"
#include "utils/fmgrprotos.h"
#include "utils/index_selfuncs.h"
#include "utils/memutils.h"
#include "utils/regproc.h"
#include "utils/syscache.h"

/*
 * Stir handler function: return IndexAmRoutine with access method parameters
 * and callbacks.
 */
Datum
stirhandler(PG_FUNCTION_ARGS)
{
	IndexAmRoutine *amroutine = makeNode(IndexAmRoutine);

	/* Set STIR-specific strategy and procedure numbers */
	amroutine->amstrategies = STIR_NSTRATEGIES;
	amroutine->amsupport = STIR_NPROC;
	amroutine->amoptsprocnum = STIR_OPTIONS_PROC;

	/* STIR doesn't support most index operations */
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

	/* Set up function callbacks */
	amroutine->ambuild = stirbuild;
	amroutine->ambuildempty = stirbuildempty;
	amroutine->aminsert = stirinsert;
	amroutine->aminsertcleanup = NULL;
	amroutine->ambulkdelete = stirbulkdelete;
	amroutine->amvacuumcleanup = stirvacuumcleanup;
	amroutine->amcanreturn = NULL;
	amroutine->amcostestimate = stircostestimate;
	amroutine->amoptions = stiroptions;
	amroutine->amproperty = NULL;
	amroutine->ambuildphasename = NULL;
	amroutine->amvalidate = stirvalidate;
	amroutine->amadjustmembers = NULL;
	amroutine->ambeginscan = stirbeginscan;
	amroutine->amrescan = stirrescan;
	amroutine->amgettuple = NULL;
	amroutine->amgetbitmap = NULL;
	amroutine->amendscan = stirendscan;
	amroutine->ammarkpos = NULL;
	amroutine->amrestrpos = NULL;
	amroutine->amestimateparallelscan = NULL;
	amroutine->aminitparallelscan = NULL;
	amroutine->amparallelrescan = NULL;

	PG_RETURN_POINTER(amroutine);
}

/*
 * Validates operator class for STIR index.
 *
 * STIR is not an real index, so validatio may be skipped.
 * But we do it just for consistency.
 */
bool
stirvalidate(Oid opclassoid)
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

		/* Check it's allowed strategy for stir */
		if (oprform->amopstrategy < 1 ||
			oprform->amopstrategy > STIR_NSTRATEGIES)
		{
			ereport(INFO,
					(errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
							errmsg("stir opfamily %s contains operator %s with invalid strategy number %d",
								   opfamilyname,
								   format_operator(oprform->amopopr),
								   oprform->amopstrategy)));
			result = false;
		}

		/* stir doesn't support ORDER BY operators */
		if (oprform->amoppurpose != AMOP_SEARCH ||
			OidIsValid(oprform->amopsortfamily))
		{
			ereport(INFO,
					(errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
							errmsg("stir opfamily %s contains invalid ORDER BY specification for operator %s",
								   opfamilyname,
								   format_operator(oprform->amopopr))));
			result = false;
		}

		/* Check operator signature --- same for all stir strategies */
		if (!check_amop_signature(oprform->amopopr, BOOLOID,
								  oprform->amoplefttype,
								  oprform->amoprighttype))
		{
			ereport(INFO,
					(errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
							errmsg("stir opfamily %s contains operator %s with wrong signature",
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


/*
 * Initialize metapage of a STIR index.
 * The skipInserts flag determines if new inserts will be accepted or skipped.
 */
void
StirFillMetapage(Relation index, Page metaPage, bool skipInserts)
{
	StirMetaPageData *metadata;

	StirInitPage(metaPage, STIR_META);
	metadata = StirPageGetMeta(metaPage);
	memset(metadata, 0, sizeof(StirMetaPageData));
	metadata->magickNumber = STIR_MAGICK_NUMBER;
	metadata->skipInserts = skipInserts;
	((PageHeader) metaPage)->pd_lower += sizeof(StirMetaPageData);
}

/*
 * Create and initialize the metapage for a STIR index.
 * This is called during index creation.
 */
void
StirInitMetapage(Relation index, ForkNumber forknum)
{
	Buffer metaBuffer;
	Page metaPage;

	Assert(!RelationNeedsWAL(index));
	/*
	 * Make a new page; since it is first page it should be associated with
	 * block number 0 (STIR_METAPAGE_BLKNO).  No need to hold the extension
	 * lock because there cannot be concurrent inserters yet.
	 */
	metaBuffer = ReadBufferExtended(index, forknum, P_NEW, RBM_NORMAL, NULL);
	START_CRIT_SECTION();
	LockBuffer(metaBuffer, BUFFER_LOCK_EXCLUSIVE);
	Assert(BufferGetBlockNumber(metaBuffer) == STIR_METAPAGE_BLKNO);

	metaPage =  BufferGetPage(metaBuffer);
	StirFillMetapage(index, metaPage, forknum == INIT_FORKNUM);

	MarkBufferDirty(metaBuffer);
	END_CRIT_SECTION();
	UnlockReleaseBuffer(metaBuffer);
}

/*
 * Initialize any page of a stir index.
 */
void
StirInitPage(Page page, uint16 flags)
{
	StirPageOpaque opaque;

	PageInit(page, BLCKSZ, sizeof(StirPageOpaqueData));

	opaque = StirPageGetOpaque(page);
	opaque->flags = flags;
	opaque->stir_page_id = STIR_PAGE_ID;
}

/*
 * Add a tuple to a STIR page. Returns false if tuple doesn't fit.
 * The tuple is added to the end of the page.
 */
static bool
StirPageAddItem(Page page, StirTuple *tuple)
{
	StirTuple *itup;
	StirPageOpaque opaque;
	Pointer ptr;

	/* We shouldn't be pointed to an invalid page */
	Assert(!PageIsNew(page));

	/* Does new tuple fit on the page? */
	if (StirPageGetFreeSpace(state, page) < sizeof(StirTuple))
		return false;

	/* Copy new tuple to the end of page */
	opaque = StirPageGetOpaque(page);
	itup = StirPageGetTuple(page, opaque->maxoff + 1);
	memcpy((Pointer) itup, (Pointer) tuple, sizeof(StirTuple));

	/* Adjust maxoff and pd_lower */
	opaque->maxoff++;
	ptr = (Pointer) StirPageGetTuple(page, opaque->maxoff + 1);
	((PageHeader) page)->pd_lower = ptr - page;

	/* Assert we didn't overrun available space */
	Assert(((PageHeader) page)->pd_lower <= ((PageHeader) page)->pd_upper);
	return true;
}

/*
 * Insert a new tuple into a STIR index.
 */
bool
stirinsert(Relation index, Datum *values, bool *isnull,
		  ItemPointer ht_ctid, Relation heapRel,
		  IndexUniqueCheck checkUnique,
		  bool indexUnchanged,
		  struct IndexInfo *indexInfo)
{
	StirTuple *itup;
	MemoryContext oldCtx;
	MemoryContext insertCtx;
	StirMetaPageData *metaData;
	Buffer buffer,
			metaBuffer;
	Page page;
	uint16 blkNo;

	/* Create temporary context for insert operation */
	insertCtx = AllocSetContextCreate(CurrentMemoryContext,
									  "Stir insert temporary context",
									  ALLOCSET_DEFAULT_SIZES);

	oldCtx = MemoryContextSwitchTo(insertCtx);

	/* Create new tuple with heap pointer */
	itup = (StirTuple *) palloc0(sizeof(StirTuple));
	itup->heapPtr = *ht_ctid;

	Assert(!RelationNeedsWAL(index));
	metaBuffer = ReadBuffer(index, STIR_METAPAGE_BLKNO);

	for (;;)
	{
		LockBuffer(metaBuffer, BUFFER_LOCK_SHARE);
		metaData = StirPageGetMeta(BufferGetPage(metaBuffer));
		/* Check if inserts are allowed */
		if (metaData->skipInserts)
		{
			UnlockReleaseBuffer(metaBuffer);
			return false;
		}
		blkNo = metaData->lastBlkNo;
		/* Don't hold metabuffer lock while doing insert */
		LockBuffer(metaBuffer, BUFFER_LOCK_UNLOCK);

		if (blkNo > 0)
		{
			buffer = ReadBuffer(index, blkNo);
			LockBuffer(buffer, BUFFER_LOCK_EXCLUSIVE);
			START_CRIT_SECTION();

			page = BufferGetPage(buffer);

			Assert(!PageIsNew(page));

			/* Try to add tuple to existing page */
			if (StirPageAddItem(page, itup))
			{
				/* Success!  Apply the change, clean up, and exit */
				MarkBufferDirty(buffer);
				END_CRIT_SECTION();

				UnlockReleaseBuffer(buffer);
				ReleaseBuffer(metaBuffer);
				MemoryContextSwitchTo(oldCtx);
				MemoryContextDelete(insertCtx);
				return false;
			}

			END_CRIT_SECTION();
			UnlockReleaseBuffer(buffer);
		}

		/* Need to add new page - get exclusive lock on meta page */
		LockBuffer(metaBuffer, BUFFER_LOCK_EXCLUSIVE);

		metaData = StirPageGetMeta(BufferGetPage(metaBuffer));
		/* Check if another backend already extended the index */

		if (blkNo != metaData->lastBlkNo)
		{
			Assert(blkNo < metaData->lastBlkNo);
			/* Someone else inserted the new page into the index, lets try again */
			LockBuffer(metaBuffer, BUFFER_LOCK_UNLOCK);
			continue;
		}
		else
		{
			/* Must extend the file */
			buffer = ExtendBufferedRel(BMR_REL(index), MAIN_FORKNUM, NULL,
									   EB_LOCK_FIRST);
			page = BufferGetPage(buffer);
			START_CRIT_SECTION();

			StirInitPage(page, 0);

			if (!StirPageAddItem(page, itup))
			{
				/* We shouldn't be here since we're inserting to an empty page */
				elog(ERROR, "could not add new stir tuple to empty page");
			}

			/* Update meta page with new last block number */
			metaData->lastBlkNo = BufferGetBlockNumber(buffer);

			MarkBufferDirty(metaBuffer);
			MarkBufferDirty(buffer);

			END_CRIT_SECTION();

			UnlockReleaseBuffer(buffer);
			UnlockReleaseBuffer(metaBuffer);

			MemoryContextSwitchTo(oldCtx);
			MemoryContextDelete(insertCtx);

			return false;
		}
	}
}

/*
 * STIR doesn't support scans - these functions all error out
 */
IndexScanDesc
stirbeginscan(Relation r, int nkeys, int norderbys)
{
	ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("\"%s\" is not a not implemented", __func__)));
}

void
stirrescan(IndexScanDesc scan, ScanKey scankey, int nscankeys,
		  ScanKey orderbys, int norderbys)
{
	ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("\"%s\" is not a not implemented", __func__)));
}

void stirendscan(IndexScanDesc scan)
{
	ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("\"%s\" is not a not implemented", __func__)));
}

/*
 * Build a STIR index - only allowed for auxiliary indexes.
 * Just initializes the meta page without any heap scans.
 */
IndexBuildResult *
stirbuild(Relation heap, Relation index,
						   struct IndexInfo *indexInfo)
{
	IndexBuildResult *result;

	if (!indexInfo->ii_Auxiliary)
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("STIR indexes are not supported to be built")));

	StirInitMetapage(index, MAIN_FORKNUM);

	result = (IndexBuildResult *) palloc(sizeof(IndexBuildResult));
	result->heap_tuples = 0;
	result->index_tuples = 0;
	return result;
}

void stirbuildempty(Relation index)
{
	StirInitMetapage(index, INIT_FORKNUM);
}

IndexBulkDeleteResult *
stirbulkdelete(IndexVacuumInfo *info,
									 IndexBulkDeleteResult *stats,
									 IndexBulkDeleteCallback callback,
									 void *callback_state)
{
	Relation index = info->index;
	BlockNumber blkno, npages;
	Buffer buffer;
	Page page;

	/* For normal VACUUM, mark to skip inserts and warn about index drop needed */
	if (!info->validate_index)
	{
		StirMarkAsSkipInserts(index);

		ereport(WARNING, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				errmsg("\"%s\" is not a not implemented, seems like this index need to be dropped", __func__)));
		return NULL;
	}

	if (stats == NULL)
		stats = (IndexBulkDeleteResult *) palloc0(sizeof(IndexBulkDeleteResult));

	/*
	 * Iterate over the pages. We don't care about concurrently added pages,
	 * because index is marked as not-ready for that momment and index not
	 * used for insert.
	 */
	npages = RelationGetNumberOfBlocks(index);
	for (blkno = STIR_HEAD_BLKNO; blkno < npages; blkno++)
	{
		StirTuple *itup, *itupEnd;

		vacuum_delay_point(false);

		buffer = ReadBufferExtended(index, MAIN_FORKNUM, blkno,
									RBM_NORMAL, info->strategy);

		LockBuffer(buffer, BUFFER_LOCK_SHARE);
		page = BufferGetPage(buffer);

		if (PageIsNew(page))
		{
			UnlockReleaseBuffer(buffer);
			continue;
		}

		itup = StirPageGetTuple(page, FirstOffsetNumber);
		itupEnd = StirPageGetTuple(page, OffsetNumberNext(StirPageGetMaxOffset(page)));
		while (itup < itupEnd)
		{
			/* Do we have to delete this tuple? */
			if (callback(&itup->heapPtr, callback_state))
			{
				ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("we never delete in stir")));
			}

			itup = StirPageGetNextTuple(itup);
		}

		UnlockReleaseBuffer(buffer);
	}

	return stats;
}

/*
 * Mark a STIR index to skip future inserts
 */
void
StirMarkAsSkipInserts(Relation index)
{
	StirMetaPageData *metaData;
	Buffer metaBuffer;
	Page metaPage;

	Assert(!RelationNeedsWAL(index));
	metaBuffer = ReadBuffer(index, STIR_METAPAGE_BLKNO);
	LockBuffer(metaBuffer, BUFFER_LOCK_EXCLUSIVE);
	START_CRIT_SECTION();

	metaPage = BufferGetPage(metaBuffer);
	metaData = StirPageGetMeta(metaPage);

	if (!metaData->skipInserts)
	{
		metaData->skipInserts = true;
		MarkBufferDirty(metaBuffer);
	}
	END_CRIT_SECTION();
	UnlockReleaseBuffer(metaBuffer);
}

IndexBulkDeleteResult *
stirvacuumcleanup(IndexVacuumInfo *info,
										IndexBulkDeleteResult *stats)
{
	StirMarkAsSkipInserts(info->index);
	ereport(WARNING, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			errmsg("\"%s\" is not a not implemented, seems like this index need to be dropped", __func__)));
	return NULL;
}

bytea *
stiroptions(Datum reloptions, bool validate)
{
	return NULL;
}

void
stircostestimate(PlannerInfo *root, IndexPath *path,
					 double loop_count, Cost *indexStartupCost,
					 Cost *indexTotalCost, Selectivity *indexSelectivity,
					 double *indexCorrelation, double *indexPages)
{
	ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("\"%s\" is not a not implemented", __func__)));
}
