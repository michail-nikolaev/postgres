/*-------------------------------------------------------------------------
 *
 * snapmgr.h
 *	  POSTGRES snapshot manager
 *
 * Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/utils/snapmgr.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef SNAPMGR_H
#define SNAPMGR_H

#include "access/transam.h"
#include "utils/relcache.h"
#include "utils/resowner.h"
#include "utils/snapshot.h"


extern PGDLLIMPORT bool FirstSnapshotSet;

extern PGDLLIMPORT TransactionId TransactionXmin;
extern PGDLLIMPORT TransactionId RecentXmin;

/* Variables representing various special snapshot semantics */
extern PGDLLIMPORT SnapshotData SnapshotSelfData;
extern PGDLLIMPORT SnapshotData SnapshotAnyData;
extern PGDLLIMPORT SnapshotData SnapshotToastData;

#define SnapshotSelf		(&SnapshotSelfData)
#define SnapshotAny			(&SnapshotAnyData)

/* Use get_toast_snapshot() for the TOAST snapshot */

/*
 * We don't provide a static SnapshotDirty variable because it would be
 * non-reentrant.  Instead, users of that snapshot type should declare a
 * local variable of type SnapshotData, and initialize it with this macro.
 */
#define InitDirtySnapshot(snapshotdata)  \
	((snapshotdata).snapshot_type = SNAPSHOT_DIRTY)

/*
 * Similarly, some initialization is required for a NonVacuumable snapshot.
 * The caller must supply the visibility cutoff state to use (c.f.
 * GlobalVisTestFor()).
 */
#define InitNonVacuumableSnapshot(snapshotdata, vistestp)  \
	((snapshotdata).snapshot_type = SNAPSHOT_NON_VACUUMABLE, \
	 (snapshotdata).vistest = (vistestp))

/* This macro encodes the knowledge of which snapshots are MVCC-safe */
#define IsMVCCSnapshot(snapshot)  \
	((snapshot)->snapshot_type == SNAPSHOT_MVCC || \
	 (snapshot)->snapshot_type == SNAPSHOT_HISTORIC_MVCC)

#define IsHistoricMVCCSnapshot(snapshot)  \
	((snapshot)->snapshot_type == SNAPSHOT_HISTORIC_MVCC)

extern Snapshot GetTransactionSnapshot(void);
extern Snapshot GetLatestSnapshot(void);
extern void SnapshotSetCommandId(CommandId curcid);

extern Snapshot GetCatalogSnapshot(Oid relid);
extern Snapshot GetNonHistoricCatalogSnapshot(Oid relid);
extern void InvalidateCatalogSnapshot(void);
extern void InvalidateCatalogSnapshotConditionally(void);

extern void PushActiveSnapshot(Snapshot snapshot);
extern void PushActiveSnapshotWithLevel(Snapshot snapshot, int snap_level);
extern void PushCopiedSnapshot(Snapshot snapshot);
extern void UpdateActiveSnapshotCommandId(void);
extern void PopActiveSnapshot(void);
extern Snapshot GetActiveSnapshot(void);
extern bool ActiveSnapshotSet(void);

extern Snapshot RegisterSnapshot(Snapshot snapshot);
extern void UnregisterSnapshot(Snapshot snapshot);
extern Snapshot RegisterSnapshotOnOwner(Snapshot snapshot, ResourceOwner owner);
extern void UnregisterSnapshotFromOwner(Snapshot snapshot, ResourceOwner owner);

extern void AtSubCommit_Snapshot(int level);
extern void AtSubAbort_Snapshot(int level);
extern void AtEOXact_Snapshot(bool isCommit, bool resetXmin);

extern void ImportSnapshot(const char *idstr);
extern bool XactHasExportedSnapshots(void);
extern void DeleteAllExportedSnapshotFiles(void);
extern void WaitForOlderSnapshots(TransactionId limitXmin, bool progress);
extern bool ThereAreNoPriorRegisteredSnapshots(void);
extern bool HaveRegisteredOrActiveSnapshot(void);

extern char *ExportSnapshot(Snapshot snapshot);

/*
 * State for the GlobalVisTest* family of functions. Those functions can
 * e.g. be used to decide if a deleted row can be removed without violating
 * MVCC semantics: If the deleted row's xmax is not considered to be running
 * by anyone, the row can be removed.
 *
 * To avoid slowing down GetSnapshotData(), we don't calculate a precise
 * cutoff XID while building a snapshot (looking at the frequently changing
 * xmins scales badly). Instead we compute two boundaries while building the
 * snapshot:
 *
 * 1) definitely_needed, indicating that rows deleted by XIDs >=
 *    definitely_needed are definitely still visible.
 *
 * 2) maybe_needed, indicating that rows deleted by XIDs < maybe_needed can
 *    definitely be removed
 *
 * When testing an XID that falls in between the two (i.e. XID >= maybe_needed
 * && XID < definitely_needed), the boundaries can be recomputed (using
 * ComputeXidHorizons()) to get a more accurate answer. This is cheaper than
 * maintaining an accurate value all the time.
 *
 * As it is not cheap to compute accurate boundaries, we limit the number of
 * times that happens in short succession. See GlobalVisTestShouldUpdate().
 *
 *
 * There are three backend lifetime instances of this struct, optimized for
 * different types of relations. As e.g. a normal user defined table in one
 * database is inaccessible to backends connected to another database, a test
 * specific to a relation can be more aggressive than a test for a shared
 * relation.  Currently we track four different states:
 *
 * 1) GlobalVisSharedRels, which only considers an XID's
 *    effects visible-to-everyone if neither snapshots in any database, nor a
 *    replication slot's xmin, nor a replication slot's catalog_xmin might
 *    still consider XID as running.
 *
 * 2) GlobalVisCatalogRels, which only considers an XID's
 *    effects visible-to-everyone if neither snapshots in the current
 *    database, nor a replication slot's xmin, nor a replication slot's
 *    catalog_xmin might still consider XID as running.
 *
 *    I.e. the difference to GlobalVisSharedRels is that
 *    snapshot in other databases are ignored.
 *
 * 3) GlobalVisDataRels, which only considers an XID's
 *    effects visible-to-everyone if neither snapshots in the current
 *    database, nor a replication slot's xmin consider XID as running.
 *
 *    I.e. the difference to GlobalVisCatalogRels is that
 *    replication slot's catalog_xmin is not taken into account.
 *
 * 4) GlobalVisTempRels, which only considers the current session, as temp
 *    tables are not visible to other sessions.
 *
 * GlobalVisTestFor(relation) returns the appropriate state
 * for the relation.
 *
 * The boundaries are FullTransactionIds instead of TransactionIds to avoid
 * wraparound dangers. There e.g. would otherwise exist no procarray state to
 * prevent maybe_needed to become old enough after the GetSnapshotData()
 * call.
 *
 * The typedef is in the header.
 */
typedef struct GlobalVisState
{
	/* XIDs >= are considered running by some backend */
	FullTransactionId definitely_needed;

	/* XIDs < are not considered to be running by any backend */
	FullTransactionId maybe_needed;
} GlobalVisState;

extern GlobalVisState *GlobalVisTestFor(Relation rel);
extern bool GlobalVisTestIsRemovableXid(GlobalVisState *state, TransactionId xid);
extern bool GlobalVisTestIsRemovableFullXid(GlobalVisState *state, FullTransactionId fxid);
extern bool GlobalVisCheckRemovableXid(Relation rel, TransactionId xid);
extern bool GlobalVisCheckRemovableFullXid(Relation rel, FullTransactionId fxid);

/*
 * Utility functions for implementing visibility routines in table AMs.
 */
extern bool XidInMVCCSnapshot(TransactionId xid, Snapshot snapshot);

/* Support for catalog timetravel for logical decoding */
struct HTAB;
extern struct HTAB *HistoricSnapshotGetTupleCids(void);
extern void SetupHistoricSnapshot(Snapshot historic_snapshot, struct HTAB *tuplecids);
extern void TeardownHistoricSnapshot(bool is_error);
extern bool HistoricSnapshotActive(void);

extern Size EstimateSnapshotSpace(Snapshot snapshot);
extern void SerializeSnapshot(Snapshot snapshot, char *start_address);
extern Snapshot RestoreSnapshot(char *start_address);
extern void RestoreTransactionSnapshot(Snapshot snapshot, void *source_pgproc);

#endif							/* SNAPMGR_H */
