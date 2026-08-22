/*-------------------------------------------------------------------------
 *
 * test_slot_snapshot.c
 *		Use the initial snapshot of a logical replication slot from SQL.
 *
 * A walsender exports the snapshot built during logical decoding slot
 * creation with
 *
 *	  CREATE_REPLICATION_SLOT ... LOGICAL ... (SNAPSHOT 'export')
 *
 * and the client then runs its initial table copy in another session with
 * SET TRANSACTION SNAPSHOT.  That is exactly what tablesync does, but the
 * isolation tester cannot speak the replication protocol, so it has no way
 * to reach the code that builds that snapshot.
 *
 * This module closes that gap: it hooks ProcessUtility() and gives
 * SET TRANSACTION SNAPSHOT a magic snapshot name of the form
 *
 *	  SET TRANSACTION SNAPSHOT 'logical-slot:<slotname>'
 *
 * which creates a temporary logical slot named <slotname>, waits for the
 * snapshot builder to reach a consistent state, and installs the resulting
 * snapshot as the transaction snapshot of the current transaction.  The
 * effect is the same as importing an exported slot snapshot, only reachable
 * over an ordinary query connection.
 *
 * SET TRANSACTION SNAPSHOT is used rather than a plain function because
 * SnapBuildInitialSnapshot() insists on being called before the transaction
 * has acquired any snapshot, and VariableSetStmt is one of the few utility
 * statements for which no snapshot is taken (see PlannedStmtRequiresSnapshot).
 *
 * Copyright (c) 2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/test/modules/test_slot_snapshot/test_slot_snapshot.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/xact.h"
#include "access/xlogutils.h"
#include "miscadmin.h"
#include "nodes/parsenodes.h"
#include "replication/logical.h"
#include "replication/logicalctl.h"
#include "replication/slot.h"
#include "replication/snapbuild.h"
#include "storage/proc.h"
#include "tcop/utility.h"
#include "utils/snapmgr.h"

PG_MODULE_MAGIC;

/* Snapshot names starting with this are handled by this module */
#define SLOT_SNAPSHOT_PREFIX	"logical-slot:"

/* Output plugin used for the temporary slot; its callbacks are never run */
#define SLOT_SNAPSHOT_PLUGIN	"pgoutput"

static ProcessUtility_hook_type prev_ProcessUtility = NULL;

static void test_slot_snapshot_utility(PlannedStmt *pstmt,
									   const char *queryString,
									   bool readOnlyTree,
									   ProcessUtilityContext context,
									   ParamListInfo params,
									   QueryEnvironment *queryEnv,
									   DestReceiver *dest,
									   QueryCompletion *qc);
static void import_slot_snapshot(const char *slotname);

/*
 * Return the slot name if "stmt" is SET TRANSACTION SNAPSHOT with a snapshot
 * name this module is responsible for, NULL otherwise.
 */
static const char *
slot_snapshot_name(Node *parsetree)
{
	VariableSetStmt *stmt;
	A_Const    *con;
	const char *idstr;

	if (parsetree == NULL || !IsA(parsetree, VariableSetStmt))
		return NULL;

	stmt = (VariableSetStmt *) parsetree;

	if (stmt->kind != VAR_SET_MULTI ||
		stmt->name == NULL ||
		strcmp(stmt->name, "TRANSACTION SNAPSHOT") != 0 ||
		list_length(stmt->args) != 1)
		return NULL;

	con = (A_Const *) linitial(stmt->args);
	if (!IsA(con, A_Const) || con->isnull || !IsA(&con->val, String))
		return NULL;

	idstr = strVal(&con->val);
	if (strncmp(idstr, SLOT_SNAPSHOT_PREFIX, strlen(SLOT_SNAPSHOT_PREFIX)) != 0)
		return NULL;

	return idstr + strlen(SLOT_SNAPSHOT_PREFIX);
}

static void
test_slot_snapshot_utility(PlannedStmt *pstmt, const char *queryString,
						   bool readOnlyTree,
						   ProcessUtilityContext context,
						   ParamListInfo params,
						   QueryEnvironment *queryEnv,
						   DestReceiver *dest, QueryCompletion *qc)
{
	const char *slotname = slot_snapshot_name(pstmt->utilityStmt);

	if (slotname != NULL)
	{
		import_slot_snapshot(slotname);
		if (qc)
			SetQueryCompletion(qc, CMDTAG_SET, 0);
		return;
	}

	if (prev_ProcessUtility)
		prev_ProcessUtility(pstmt, queryString, readOnlyTree, context,
							params, queryEnv, dest, qc);
	else
		standard_ProcessUtility(pstmt, queryString, readOnlyTree, context,
								params, queryEnv, dest, qc);
}

/*
 * Create a logical slot and use its initial snapshot as the transaction
 * snapshot.  Mirrors CreateReplicationSlot() with the CRS_USE_SNAPSHOT option.
 */
static void
import_slot_snapshot(const char *slotname)
{
	LogicalDecodingContext *ctx;
	Snapshot	snap;

	/* Same restrictions as ImportSnapshot() */
	if (!IsTransactionBlock())
		ereport(ERROR,
				(errcode(ERRCODE_ACTIVE_SQL_TRANSACTION),
				 errmsg("SET TRANSACTION SNAPSHOT must be called before any query")));
	if (FirstSnapshotSet)
		ereport(ERROR,
				(errcode(ERRCODE_ACTIVE_SQL_TRANSACTION),
				 errmsg("SET TRANSACTION SNAPSHOT must be called before any query")));
	if (!IsolationUsesXactSnapshot())
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("a snapshot-importing transaction must have isolation level SERIALIZABLE or REPEATABLE READ")));

	CheckLogicalDecodingRequirements(false);

	/*
	 * The slot is only a vehicle for building the snapshot, so make it
	 * ephemeral: ReplicationSlotRelease() then drops it for us, and the
	 * session can do this more than once.
	 */
	ReplicationSlotCreate(slotname, true, RS_EPHEMERAL, false, false, false,
						  false);

	/*
	 * Make sure logical decoding is enabled before initializing the decoding
	 * context.
	 */
	EnsureLogicalDecodingEnabled();

	ctx = CreateInitDecodingContext(SLOT_SNAPSHOT_PLUGIN, NIL,
									true,	/* need_full_snapshot */
									false,	/* for_repack */
									InvalidXLogRecPtr,
									XL_ROUTINE(.page_read = read_local_xlog_page,
											   .segment_open = wal_segment_open,
											   .segment_close = wal_segment_close),
									NULL, NULL, NULL);

	/* Build the initial snapshot; this can take a while. */
	DecodingContextFindStartpoint(ctx);

	snap = SnapBuildInitialSnapshot(ctx->snapshot_builder);

	/*
	 * Install it before releasing the decoding context, which is what the
	 * snapshot's memory belongs to.  RestoreTransactionSnapshot() copies
	 * everything it needs into the transaction snapshot.
	 */
	RestoreTransactionSnapshot(snap, MyProc);

	FreeDecodingContext(ctx);

	/*
	 * Drop the slot.  What keeps the snapshot usable from here on is our own
	 * MyProc->xmin, which SnapBuildInitialSnapshot() advertised and
	 * RestoreTransactionSnapshot() re-installed, just like for any other
	 * imported snapshot.
	 */
	ReplicationSlotRelease();
}

void
_PG_init(void)
{
	prev_ProcessUtility = ProcessUtility_hook;
	ProcessUtility_hook = test_slot_snapshot_utility;
}
