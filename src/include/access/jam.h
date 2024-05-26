/*-------------------------------------------------------------------------
 *
 * jam.h
 *	  header file for postgres jam access method implementation.
 *
 *
 * Portions Copyright (c) 2024-2024, PostgreSQL Global Development Group
 *
 * src/include/access/jam.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef _JAM_H_
#define _JAM_H_

#include "amapi.h"
#include "xlog.h"
#include "generic_xlog.h"
#include "itup.h"
#include "fmgr.h"
#include "nodes/pathnodes.h"

/* Support procedures numbers */
#define JAM_NPROC				0

/* Scan strategies */
#define JAM_NSTRATEGIES		1

#define JAM_OPTIONS_PROC				0

/* Macros for accessing bloom page structures */
#define JamPageGetOpaque(page) ((JamPageOpaque) PageGetSpecialPointer(page))
#define JamPageGetMaxOffset(page) (JamPageGetOpaque(page)->maxoff)
#define JamPageIsMeta(page) \
	((JamPageGetOpaque(page)->flags & BLOOM_META) != 0)
#define JamPageGetData(page)		((JamTuple *)PageGetContents(page))
#define JamPageGetTuple(page, offset) \
	((JamTuple *)(PageGetContents(page) \
		+ sizeof(JamTuple) * ((offset) - 1)))
#define JamPageGetNextTuple(tuple) \
	((JamTuple *)((Pointer)(tuple) + sizeof(JamTuple)))



/* Preserved page numbers */
#define JAM_METAPAGE_BLKNO	(0)
#define JAM_HEAD_BLKNO		(1) /* first data page */


/* Opaque for jam pages */
typedef struct JamPageOpaqueData
{
	OffsetNumber maxoff;		/* number of index tuples on page */
	uint16		flags;			/* see bit definitions below */
	uint16		unused;			/* placeholder to force maxaligning of size of
								 * JamPageOpaqueData and to place
								 * jam_page_id exactly at the end of page */
	uint16		jam_page_id;	/* for identification of JAM indexes */
} JamPageOpaqueData;

/* Jam page flags */
#define JAM_META		(1<<0)

typedef JamPageOpaqueData *JamPageOpaque;

#define JAM_PAGE_ID		0xFF84

/* Metadata of jam index */
typedef struct JamMetaPageData
{
	uint32		magickNumber;
	uint16		lastBlkNo;
} JamMetaPageData;

/* Magic number to distinguish jam pages from others */
#define JAM_MAGICK_NUMBER (0xDBAC0DEF)

#define JamPageGetMeta(page)	((JamMetaPageData *) PageGetContents(page))

typedef struct JamTuple
{
	ItemPointerData heapPtr;
} JamTuple;

#define JamPageGetFreeSpace(state, page) \
	(BLCKSZ - MAXALIGN(SizeOfPageHeaderData) \
		- JamPageGetMaxOffset(page) * (sizeof(JamTuple)) \
		- MAXALIGN(sizeof(JamPageOpaqueData)))

extern void JamFillMetapage(Relation index, Page metaPage);
extern void JamInitMetapage(Relation index, ForkNumber forknum);
extern void JamInitPage(Page page, uint16 flags);

/* index access method interface functions */
extern bool jamvalidate(Oid opclassoid);
extern bool jaminsert(Relation index, Datum *values, bool *isnull,
					 ItemPointer ht_ctid, Relation heapRel,
					 IndexUniqueCheck checkUnique,
					 bool indexUnchanged,
					 struct IndexInfo *indexInfo);
extern IndexScanDesc jambeginscan(Relation r, int nkeys, int norderbys);
extern void jamrescan(IndexScanDesc scan, ScanKey scankey, int nscankeys,
					 ScanKey orderbys, int norderbys);
extern void jamendscan(IndexScanDesc scan);
extern IndexBuildResult *jambuild(Relation heap, Relation index,
								 struct IndexInfo *indexInfo);
extern void jambuildempty(Relation index);
extern IndexBulkDeleteResult *jambulkdelete(IndexVacuumInfo *info,
										   IndexBulkDeleteResult *stats, IndexBulkDeleteCallback callback,
										   void *callback_state);
extern IndexBulkDeleteResult *jamvacuumcleanup(IndexVacuumInfo *info,
											  IndexBulkDeleteResult *stats);
extern bytea *jamoptions(Datum reloptions, bool validate);

#endif
