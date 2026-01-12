/*-------------------------------------------------------------------------
 *
 * cluster.c
 *		Implementation of REPACK [CONCURRENTLY], also known as CLUSTER and
 *		VACUUM FULL.
 *
 * There are two somewhat different ways to rewrite a table.  In non-
 * concurrent mode, it's easy: take AccessExclusiveLock, create a new
 * transient relation, copy the tuples over to the relfilenode of the new
 * relation, swap the relfilenodes, then drop the old relation.
 *
 * In concurrent mode, we lock the table with only ShareUpdateExclusiveLock,
 * then do an initial copy as above.  However, while the tuples are being
 * copied, concurrent transactions could modify the table. To cope with those
 * changes, we rely on logical decoding to obtain them from WAL.  A bgworker
 * consumes WAL while the initial copy is ongoing (to prevent excessive WAL
 * from being reserved), and accumulates the changes in a file.  Once the
 * initial copy is complete, we read the changes from the file and re-apply
 * them on the new heap.  Then we upgrade our ShareUpdateExclusiveLock to
 * AccessExclusiveLock and swap the relfilenodes.  This way, the time we hold
 * a strong lock on the table is much reduced, and the bloat is eliminated.
 *
 * There is hardly anything left of Paul Brown's original implementation...
 *
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994-5, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/commands/cluster.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/amapi.h"
#include "access/heapam.h"
#include "access/multixact.h"
#include "access/relscan.h"
#include "access/tableam.h"
#include "access/toast_internals.h"
#include "access/transam.h"
#include "access/xact.h"
#include "access/xlog.h"
#include "access/xlog_internal.h"
#include "access/xloginsert.h"
#include "access/xlogutils.h"
#include "access/xlogwait.h"
#include "catalog/catalog.h"
#include "catalog/dependency.h"
#include "catalog/heap.h"
#include "catalog/index.h"
#include "catalog/namespace.h"
#include "catalog/objectaccess.h"
#include "catalog/pg_am.h"
#include "catalog/pg_control.h"
#include "catalog/pg_inherits.h"
#include "catalog/toasting.h"
#include "commands/cluster.h"
#include "commands/defrem.h"
#include "commands/progress.h"
#include "commands/tablecmds.h"
#include "commands/vacuum.h"
#include "executor/executor.h"
#include "libpq/pqformat.h"
#include "libpq/pqmq.h"
#include "miscadmin.h"
#include "optimizer/optimizer.h"
#include "pgstat.h"
#include "replication/decode.h"
#include "replication/logical.h"
#include "replication/snapbuild.h"
#include "storage/bufmgr.h"
#include "storage/ipc.h"
#include "storage/lmgr.h"
#include "storage/predicate.h"
#include "storage/procsignal.h"
#include "tcop/tcopprot.h"
#include "utils/acl.h"
#include "utils/fmgroids.h"
#include "utils/guc.h"
#include "utils/injection_point.h"
#include "utils/inval.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/pg_rusage.h"
#include "utils/relmapper.h"
#include "utils/snapmgr.h"
#include "utils/syscache.h"

/*
 * This struct is used to pass around the information on tables to be
 * clustered. We need this so we can make a list of them when invoked without
 * a specific table/index pair.
 */
typedef struct
{
	Oid			tableOid;
	Oid			indexOid;
} RelToCluster;

/*
 * The following definitions are used for concurrent processing.
 */

/*
 * The locators are used to avoid logical decoding of data that we do not need
 * for our table.
 */
static RelFileLocator repacked_rel_locator = {.relNumber = InvalidOid};
static RelFileLocator repacked_rel_toast_locator = {.relNumber = InvalidOid};

/*
 * Everything we need to call ExecInsertIndexTuples().
 */
typedef struct IndexInsertState
{
	ResultRelInfo *rri;
	EState	   *estate;
} IndexInsertState;

/* The WAL segment being decoded. */
static XLogSegNo repack_current_segment = 0;

/*
 * The first file exported by the decoding worker must contain a snapshot, the
 * following ones contain the data changes.
 */
#define WORKER_FILE_SNAPSHOT	0

/*
 * Information needed to apply concurrent data changes.
 */
typedef struct ChangeDest
{
	/* The relation the changes are applied to. */
	Relation	rel;

	/*
	 * The following is needed to find the existing tuple if the change is
	 * UPDATE or DELETE. 'ident_key' should have all the fields except for
	 * 'sk_argument' initialized.
	 */
	Relation	ident_index;
	ScanKey		ident_key;
	int			ident_key_nentries;

	/* Needed to update indexes of rel_dst. */
	IndexInsertState *iistate;

	/*
	 * Sequential number of the file containing the changes.
	 *
	 * TODO This field makes the structure name less descriptive. Should we
	 * rename it, e.g. to ChangeApplyInfo?
	 */
	int		file_seq;
} ChangeDest;

/*
 * Layout of shared memory used for communication between backend and the
 * worker that performs logical decoding of data changes
 */
typedef struct DecodingWorkerShared
{
	/* Is the decoding initialized? */
	bool		initialized;

	/*
	 * Once the worker has reached this LSN, it should close the current
	 * output file and either create a new one or exit, according to the field
	 * 'done'. If the value is InvalidXLogRecPtr, the worker should decode all
	 * the WAL available and keep checking this field. It is ok if the worker
	 * had already decoded records whose LSN is >= lsn_upto before this field
	 * has been set.
	 */
	XLogRecPtr	lsn_upto;

	/* Exit after closing the current file? */
	bool		done;

	/* The output is stored here. */
	SharedFileSet sfs;

	/* Number of the last file exported by the worker. */
	int			last_exported;

	/* Synchronize access to the fields above. */
	slock_t		mutex;

	/* Database to connect to. */
	Oid			dbid;

	/* Role to connect as. */
	Oid			roleid;

	/* Decode data changes of this relation. */
	Oid			relid;

	/* The backend uses this to wait for the worker. */
	ConditionVariable cv;

	/* Info to signal the backend. */
	PGPROC	   *backend_proc;
	pid_t		backend_pid;
	ProcNumber	backend_proc_number;

	/* Error queue. */
	shm_mq	   *error_mq;

	/*
	 * Memory the queue is located int.
	 *
	 * For considerations on the value see the comments of
	 * PARALLEL_ERROR_QUEUE_SIZE.
	 */
#define REPACK_ERROR_QUEUE_SIZE			16384
	char		error_queue[FLEXIBLE_ARRAY_MEMBER];
} DecodingWorkerShared;

/*
 * Generate worker's output file name. If relations of the same 'relid' happen
 * to be processed at the same time, they must be from different databases and
 * therefore different backends must be involved. (PID is already present in
 * the fileset name.)
 */
static inline void
DecodingWorkerFileName(char *fname, Oid relid, uint32 seq)
{
	snprintf(fname, MAXPGPATH, "%u-%u", relid, seq);
}

/*
 * Backend-local information to control the decoding worker.
 */
typedef struct DecodingWorker
{
	/* The worker. */
	BackgroundWorkerHandle *handle;

	/* DecodingWorkerShared is in this segment. */
	dsm_segment *seg;

	/* Handle of the error queue. */
	shm_mq_handle *error_mqh;
} DecodingWorker;

/* Pointer to currently running decoding worker. */
static DecodingWorker *decoding_worker = NULL;

/*
 * Is there a message sent by a repack worker that the backend needs to
 * receive?
 */
volatile sig_atomic_t RepackMessagePending = false;

static bool cluster_rel_recheck(RepackCommand cmd, Relation OldHeap,
								Oid indexOid, Oid userid, LOCKMODE lmode,
								int options);
static void check_repack_concurrently_requirements(Relation rel);
static void rebuild_relation(Relation OldHeap, Relation index, bool verbose,
							 bool concurrent);
static void copy_table_data(Relation NewHeap, Relation OldHeap, Relation OldIndex,
							Snapshot snapshot,
							bool verbose,
							bool *pSwapToastByContent,
							TransactionId *pFreezeXid,
							MultiXactId *pCutoffMulti);
static List *get_tables_to_repack(RepackCommand cmd, bool usingindex,
								  MemoryContext permcxt);
static List *get_tables_to_repack_partitioned(RepackCommand cmd,
											  Oid relid, bool rel_is_index,
											  MemoryContext permcxt);
static bool cluster_is_permitted_for_relation(RepackCommand cmd,
											  Oid relid, Oid userid);

static LogicalDecodingContext *setup_logical_decoding(Oid relid);
static bool decode_concurrent_changes(LogicalDecodingContext *ctx,
									  DecodingWorkerShared *shared);
static void apply_concurrent_changes(BufFile *file, ChangeDest *dest);
static void apply_concurrent_insert(Relation rel, HeapTuple tup,
									IndexInsertState *iistate,
									TupleTableSlot *index_slot);
static void apply_concurrent_update(Relation rel, HeapTuple tup,
									HeapTuple tup_target,
									IndexInsertState *iistate,
									TupleTableSlot *index_slot);
static void apply_concurrent_delete(Relation rel, HeapTuple tup_target);
static HeapTuple find_target_tuple(Relation rel, ChangeDest *dest,
								   HeapTuple tup_key,
								   TupleTableSlot *ident_slot);
static void process_concurrent_changes(XLogRecPtr end_of_wal,
									   ChangeDest *dest,
									   bool done);
static IndexInsertState *get_index_insert_state(Relation relation,
												Oid ident_index_id,
												Relation *ident_index_p);
static ScanKey build_identity_key(Oid ident_idx_oid, Relation rel_src,
								  int *nentries);
static void free_index_insert_state(IndexInsertState *iistate);
static void cleanup_logical_decoding(LogicalDecodingContext *ctx);
static void rebuild_relation_finish_concurrent(Relation NewHeap, Relation OldHeap,
											   TransactionId frozenXid,
											   MultiXactId cutoffMulti);
static List *build_new_indexes(Relation NewHeap, Relation OldHeap, List *OldIndexes);
static Relation process_single_relation(RepackStmt *stmt,
										LOCKMODE lockmode,
										bool isTopLevel,
										ClusterParams *params);
static Oid	determine_clustered_index(Relation rel, bool usingindex,
									  const char *indexname);
static void start_decoding_worker(Oid relid);
static void stop_decoding_worker(void);
static void repack_worker_internal(dsm_segment *seg);
static void export_initial_snapshot(Snapshot snapshot,
									DecodingWorkerShared *shared);
static Snapshot get_initial_snapshot(DecodingWorker *worker);
static void ProcessRepackMessage(StringInfo msg);
static const char *RepackCommandAsString(RepackCommand cmd);


#define REPL_PLUGIN_NAME   "pgoutput_repack"

/*
 * The repack code allows for processing multiple tables at once. Because
 * of this, we cannot just run everything on a single transaction, or we
 * would be forced to acquire exclusive locks on all the tables being
 * clustered, simultaneously --- very likely leading to deadlock.
 *
 * To solve this we follow a similar strategy to VACUUM code, processing each
 * relation in a separate transaction. For this to work, we need to:
 *
 *	- provide a separate memory context so that we can pass information in
 *	  a way that survives across transactions
 *	- start a new transaction every time a new relation is clustered
 *	- check for validity of the information on to-be-clustered relations,
 *	  as someone might have deleted a relation behind our back, or
 *	  clustered one on a different index
 *	- end the transaction
 *
 * The single-relation case does not have any such overhead.
 *
 * We also allow a relation to be repacked following an index, but without
 * naming a specific one.  In that case, the indisclustered bit will be
 * looked up, and an ERROR will be thrown if no so-marked index is found.
 */
void
ExecRepack(ParseState *pstate, RepackStmt *stmt, bool isTopLevel)
{
	ClusterParams params = {0};
	Relation	rel = NULL;
	MemoryContext repack_context;
	LOCKMODE	lockmode;
	List	   *rtcs;

	/* Parse option list */
	foreach_node(DefElem, opt, stmt->params)
	{
		if (strcmp(opt->defname, "verbose") == 0)
			params.options |= defGetBoolean(opt) ? CLUOPT_VERBOSE : 0;
		else if (strcmp(opt->defname, "analyze") == 0 ||
				 strcmp(opt->defname, "analyse") == 0)
			params.options |= defGetBoolean(opt) ? CLUOPT_ANALYZE : 0;
		else if (strcmp(opt->defname, "concurrently") == 0 &&
				 defGetBoolean(opt))
		{
			if (stmt->command != REPACK_COMMAND_REPACK)
				ereport(ERROR,
						errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						errmsg("CONCURRENTLY option not supported for %s",
							   RepackCommandAsString(stmt->command)));
			params.options |= CLUOPT_CONCURRENT;
		}
		else
			ereport(ERROR,
					errcode(ERRCODE_SYNTAX_ERROR),
					errmsg("unrecognized %s option \"%s\"",
						   RepackCommandAsString(stmt->command),
						   opt->defname),
					parser_errposition(pstate, opt->location));
	}

	/*
	 * Determine the lock mode expected by cluster_rel().
	 *
	 * In the exclusive case, we obtain AccessExclusiveLock right away to
	 * avoid lock-upgrade hazard in the single-transaction case. In the
	 * CONCURRENTLY case, the AccessExclusiveLock will only be used at the end
	 * of processing, supposedly for very short time. Until then, we'll have
	 * to unlock the relation temporarily, so there's no lock-upgrade hazard.
	 */
	lockmode = (params.options & CLUOPT_CONCURRENT) == 0 ?
		AccessExclusiveLock : ShareUpdateExclusiveLock;

	/*
	 * If a single relation is specified, process it and we're done ... unless
	 * the relation is a partitioned table, in which case we fall through.
	 */
	if (stmt->relation != NULL)
	{
		rel = process_single_relation(stmt, lockmode, isTopLevel, &params);
		if (rel == NULL)
			return;				/* all done */
	}

	/*
	 * Don't allow ANALYZE in the multiple-relation case for now.  Maybe we
	 * can add support for this later.
	 */
	if (params.options & CLUOPT_ANALYZE)
		ereport(ERROR,
				errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				errmsg("cannot %s multiple tables", "REPACK (ANALYZE)"));

	/*
	 * By here, we know we are in a multi-table situation.
	 *
	 * Concurrent processing is currently considered rather special (e.g. in
	 * terms of resources consumed) so it is not performed in bulk.
	 */
	if (params.options & CLUOPT_CONCURRENT)
	{
		if (rel != NULL)
		{
			Assert(rel->rd_rel->relkind == RELKIND_PARTITIONED_TABLE);
			ereport(ERROR,
					errmsg("REPACK CONCURRENTLY not supported for partitioned tables"),
					errhint("Consider running the command for individual partitions."));
		}
		else
			ereport(ERROR,
					errmsg("REPACK CONCURRENTLY requires explicit table name"));
	}

	/*
	 * In order to avoid holding locks for too long, we want to process each
	 * table in its own transaction.  This forces us to disallow running
	 * inside a user transaction block.
	 */
	PreventInTransactionBlock(isTopLevel, RepackCommandAsString(stmt->command));

	/* Also, we need a memory context to hold our list of relations */
	repack_context = AllocSetContextCreate(PortalContext,
										   "Repack",
										   ALLOCSET_DEFAULT_SIZES);

	params.options |= CLUOPT_RECHECK;

	/*
	 * If we don't have a relation yet, determine a relation list.  If we do,
	 * then it must be a partitioned table, and we want to process its
	 * partitions.
	 */
	if (rel == NULL)
	{
		Assert(stmt->indexname == NULL);
		rtcs = get_tables_to_repack(stmt->command, stmt->usingindex,
									repack_context);
		params.options |= CLUOPT_RECHECK_ISCLUSTERED;
	}
	else
	{
		Oid			relid;
		bool		rel_is_index;

		Assert(rel->rd_rel->relkind == RELKIND_PARTITIONED_TABLE);

		/*
		 * If USING INDEX was specified, resolve the index name now and pass
		 * it down.
		 */
		if (stmt->usingindex)
		{
			/*
			 * If no index name was specified when repacking a partitioned
			 * table, punt for now.  Maybe we can improve this later.
			 */
			if (!stmt->indexname)
				ereport(ERROR,
						errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
						errmsg("there is no previously clustered index for table \"%s\"",
							   RelationGetRelationName(rel)));

			relid = determine_clustered_index(rel, stmt->usingindex,
											  stmt->indexname);
			if (!OidIsValid(relid))
				elog(ERROR, "unable to determine index to cluster on");
			/* XXX is this the right place for this check? */
			check_index_is_clusterable(rel, relid, AccessExclusiveLock);
			rel_is_index = true;
		}
		else
		{
			relid = RelationGetRelid(rel);
			rel_is_index = false;
		}

		rtcs = get_tables_to_repack_partitioned(stmt->command,
												relid, rel_is_index,
												repack_context);

		/* close parent relation, releasing lock on it */
		table_close(rel, AccessExclusiveLock);
		rel = NULL;
	}

	/* Commit to get out of starting transaction */
	PopActiveSnapshot();
	CommitTransactionCommand();

	/* Cluster the tables, each in a separate transaction */
	Assert(rel == NULL);
	foreach_ptr(RelToCluster, rtc, rtcs)
	{
		/* Start a new transaction for each relation. */
		StartTransactionCommand();

		/*
		 * Open the target table, coping with the case where it has been
		 * dropped.
		 */
		rel = try_table_open(rtc->tableOid, lockmode);
		if (rel == NULL)
		{
			CommitTransactionCommand();
			continue;
		}

		/* functions in indexes may want a snapshot set */
		PushActiveSnapshot(GetTransactionSnapshot());

		/* Process this table */
		cluster_rel(stmt->command, rel, rtc->indexOid, &params, isTopLevel);
		/* cluster_rel closes the relation, but keeps lock */

		PopActiveSnapshot();
		CommitTransactionCommand();
	}

	/* Start a new transaction for the cleanup work. */
	StartTransactionCommand();

	/* Clean up working storage */
	MemoryContextDelete(repack_context);
}

/*
 * cluster_rel
 *
 * This clusters the table by creating a new, clustered table and
 * swapping the relfilenumbers of the new table and the old table, so
 * the OID of the original table is preserved.  Thus we do not lose
 * GRANT, inheritance nor references to this table.
 *
 * Indexes are rebuilt too, via REINDEX. Since we are effectively bulk-loading
 * the new table, it's better to create the indexes afterwards than to fill
 * them incrementally while we load the table.
 *
 * If indexOid is InvalidOid, the table will be rewritten in physical order
 * instead of index order.
 *
 * Note that, in the concurrent case, the function releases the lock at some
 * point, in order to get AccessExclusiveLock for the final steps (i.e. to
 * swap the relation files). To make things simpler, the caller should expect
 * OldHeap to be closed on return, regardless CLUOPT_CONCURRENT. (The
 * AccessExclusiveLock is kept till the end of the transaction.)
 *
 * 'cmd' indicates which command is being executed, to be used for error
 * messages.
 */
void
cluster_rel(RepackCommand cmd, Relation OldHeap, Oid indexOid,
			ClusterParams *params, bool isTopLevel)
{
	Oid			tableOid = RelationGetRelid(OldHeap);
	Relation	index;
	LOCKMODE	lmode;
	Oid			save_userid;
	int			save_sec_context;
	int			save_nestlevel;
	bool		verbose = ((params->options & CLUOPT_VERBOSE) != 0);
	bool		recheck = ((params->options & CLUOPT_RECHECK) != 0);
	bool		concurrent = ((params->options & CLUOPT_CONCURRENT) != 0);

	/*
	 * The lock mode is AccessExclusiveLock for normal processing and
	 * ShareUpdateExclusiveLock for concurrent processing (so that SELECT,
	 * INSERT, UPDATE and DELETE commands work, but cluster_rel() cannot be
	 * called concurrently for the same relation).
	 */
	lmode = !concurrent ? AccessExclusiveLock : ShareUpdateExclusiveLock;

	/* There are specific requirements on concurrent processing. */
	if (concurrent)
	{
		/*
		 * Make sure we have no XID assigned, otherwise call of
		 * setup_logical_decoding() can cause a deadlock.
		 *
		 * The existence of transaction block actually does not imply that XID
		 * was already assigned, but it very likely is. We might want to check
		 * the result of GetCurrentTransactionIdIfAny() instead, but that
		 * would be less clear from user's perspective.
		 */
		PreventInTransactionBlock(isTopLevel, "REPACK (CONCURRENTLY)");

		check_repack_concurrently_requirements(OldHeap);
	}

	/* Check for user-requested abort. */
	CHECK_FOR_INTERRUPTS();

	pgstat_progress_start_command(PROGRESS_COMMAND_REPACK, tableOid);
	pgstat_progress_update_param(PROGRESS_REPACK_COMMAND, cmd);

	/*
	 * Switch to the table owner's userid, so that any index functions are run
	 * as that user.  Also lock down security-restricted operations and
	 * arrange to make GUC variable changes local to this command.
	 */
	GetUserIdAndSecContext(&save_userid, &save_sec_context);
	SetUserIdAndSecContext(OldHeap->rd_rel->relowner,
						   save_sec_context | SECURITY_RESTRICTED_OPERATION);
	save_nestlevel = NewGUCNestLevel();
	RestrictSearchPath();

	/*
	 * Since we may open a new transaction for each relation, we have to check
	 * that the relation still is what we think it is.
	 *
	 * If this is a single-transaction CLUSTER, we can skip these tests. We
	 * *must* skip the one on indisclustered since it would reject an attempt
	 * to cluster a not-previously-clustered index.
	 *
	 * XXX move [some of] these comments to where the RECHECK flag is
	 * determined?
	 */
	if (recheck &&
		!cluster_rel_recheck(cmd, OldHeap, indexOid, save_userid,
							 lmode, params->options))
		goto out;

	/*
	 * We allow repacking shared catalogs only when not using an index. It
	 * would work to use an index in most respects, but the index would only
	 * get marked as indisclustered in the current database, leading to
	 * unexpected behavior if CLUSTER were later invoked in another database.
	 */
	if (OidIsValid(indexOid) && OldHeap->rd_rel->relisshared)
		ereport(ERROR,
				errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				errmsg("cannot run %s on a shared catalog",
					   RepackCommandAsString(cmd)));

	/*
	 * The CONCURRENTLY case should have been rejected earlier because it does
	 * not support system catalogs.
	 */
	Assert(!(OldHeap->rd_rel->relisshared && concurrent));

	/*
	 * Don't process temp tables of other backends ... their local buffer
	 * manager is not going to cope.
	 */
	if (RELATION_IS_OTHER_TEMP(OldHeap))
		ereport(ERROR,
				errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				errmsg("cannot run %s on temporary tables of other sessions",
					   RepackCommandAsString(cmd)));

	/*
	 * Also check for active uses of the relation in the current transaction,
	 * including open scans and pending AFTER trigger events.
	 */
	CheckTableNotInUse(OldHeap, RepackCommandAsString(cmd));

	/* Check heap and index are valid to cluster on */
	if (OidIsValid(indexOid))
	{
		/* verify the index is good and lock it */
		check_index_is_clusterable(OldHeap, indexOid, lmode);
		/* also open it */
		index = index_open(indexOid, NoLock);
	}
	else
		index = NULL;

	/*
	 * When allow_system_table_mods is turned off, we disallow repacking a
	 * catalog on a particular index unless that's already the clustered index
	 * for that catalog.
	 *
	 * XXX We don't check for this in CLUSTER, because it's historically been
	 * allowed.
	 */
	if (cmd != REPACK_COMMAND_CLUSTER &&
		!allowSystemTableMods && OidIsValid(indexOid) &&
		IsCatalogRelation(OldHeap) && !index->rd_index->indisclustered)
		ereport(ERROR,
				errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				errmsg("permission denied: \"%s\" is a system catalog",
					   RelationGetRelationName(OldHeap)),
				errdetail("System catalogs can only be clustered by the index they're already clustered on, if any, unless \"%s\" is enabled.",
						  "allow_system_table_mods"));

	/*
	 * Quietly ignore the request if this is a materialized view which has not
	 * been populated from its query. No harm is done because there is no data
	 * to deal with, and we don't want to throw an error if this is part of a
	 * multi-relation request -- for example, CLUSTER was run on the entire
	 * database.
	 */
	if (OldHeap->rd_rel->relkind == RELKIND_MATVIEW &&
		!RelationIsPopulated(OldHeap))
	{
		if (index)
			index_close(index, lmode);
		relation_close(OldHeap, lmode);
		goto out;
	}

	Assert(OldHeap->rd_rel->relkind == RELKIND_RELATION ||
		   OldHeap->rd_rel->relkind == RELKIND_MATVIEW ||
		   OldHeap->rd_rel->relkind == RELKIND_TOASTVALUE);

	/*
	 * All predicate locks on the tuples or pages are about to be made
	 * invalid, because we move tuples around.  Promote them to relation
	 * locks.  Predicate locks on indexes will be promoted when they are
	 * reindexed.
	 *
	 * During concurrent processing, the heap as well as its indexes stay in
	 * operation, so we postpone this step until they are locked using
	 * AccessExclusiveLock near the end of the processing.
	 */
	if (!concurrent)
		TransferPredicateLocksToHeapRelation(OldHeap);

	/* rebuild_relation does all the dirty work */
	PG_TRY();
	{
		rebuild_relation(OldHeap, index, verbose, concurrent);
	}
	PG_FINALLY();
	{
		if (concurrent)
		{
			/*
			 * Since during normal operation the worker was already asked to
			 * exit, stopping it explicitly is especially important on ERROR.
			 * However it still seems a good practice to make sure that the
			 * worker never survives the REPACK command.
			 */
			stop_decoding_worker();
		}
	}
	PG_END_TRY();

	/* rebuild_relation closes OldHeap, and index if valid */

out:
	/* Roll back any GUC changes executed by index functions */
	AtEOXact_GUC(false, save_nestlevel);

	/* Restore userid and security context */
	SetUserIdAndSecContext(save_userid, save_sec_context);

	pgstat_progress_end_command();
}

/*
 * Check if the table (and its index) still meets the requirements of
 * cluster_rel().
 */
static bool
cluster_rel_recheck(RepackCommand cmd, Relation OldHeap, Oid indexOid,
					Oid userid, LOCKMODE lmode, int options)
{
	Oid			tableOid = RelationGetRelid(OldHeap);

	/* Check that the user still has privileges for the relation */
	if (!cluster_is_permitted_for_relation(cmd, tableOid, userid))
	{
		relation_close(OldHeap, lmode);
		return false;
	}

	/*
	 * Silently skip a temp table for a remote session.  Only doing this check
	 * in the "recheck" case is appropriate (which currently means somebody is
	 * executing a database-wide CLUSTER or on a partitioned table), because
	 * there is another check in cluster() which will stop any attempt to
	 * cluster remote temp tables by name.  There is another check in
	 * cluster_rel which is redundant, but we leave it for extra safety.
	 */
	if (RELATION_IS_OTHER_TEMP(OldHeap))
	{
		relation_close(OldHeap, lmode);
		return false;
	}

	if (OidIsValid(indexOid))
	{
		/*
		 * Check that the index still exists
		 */
		if (!SearchSysCacheExists1(RELOID, ObjectIdGetDatum(indexOid)))
		{
			relation_close(OldHeap, lmode);
			return false;
		}

		/*
		 * Check that the index is still the one with indisclustered set, if
		 * needed.
		 */
		if ((options & CLUOPT_RECHECK_ISCLUSTERED) != 0 &&
			!get_index_isclustered(indexOid))
		{
			relation_close(OldHeap, lmode);
			return false;
		}
	}

	return true;
}

/*
 * Verify that the specified heap and index are valid to cluster on
 *
 * Side effect: obtains lock on the index.  The caller may
 * in some cases already have a lock of the same strength on the table, but
 * not in all cases so we can't rely on the table-level lock for
 * protection here.
 */
void
check_index_is_clusterable(Relation OldHeap, Oid indexOid, LOCKMODE lockmode)
{
	Relation	OldIndex;

	OldIndex = index_open(indexOid, lockmode);

	/*
	 * Check that index is in fact an index on the given relation
	 */
	if (OldIndex->rd_index == NULL ||
		OldIndex->rd_index->indrelid != RelationGetRelid(OldHeap))
		ereport(ERROR,
				(errcode(ERRCODE_WRONG_OBJECT_TYPE),
				 errmsg("\"%s\" is not an index for table \"%s\"",
						RelationGetRelationName(OldIndex),
						RelationGetRelationName(OldHeap))));

	/* Index AM must allow clustering */
	if (!OldIndex->rd_indam->amclusterable)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("cannot cluster on index \"%s\" because access method does not support clustering",
						RelationGetRelationName(OldIndex))));

	/*
	 * Disallow clustering on incomplete indexes (those that might not index
	 * every row of the relation).  We could relax this by making a separate
	 * seqscan pass over the table to copy the missing rows, but that seems
	 * expensive and tedious.
	 */
	if (!heap_attisnull(OldIndex->rd_indextuple, Anum_pg_index_indpred, NULL))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("cannot cluster on partial index \"%s\"",
						RelationGetRelationName(OldIndex))));

	/*
	 * Disallow if index is left over from a failed CREATE INDEX CONCURRENTLY;
	 * it might well not contain entries for every heap row, or might not even
	 * be internally consistent.  (But note that we don't check indcheckxmin;
	 * the worst consequence of following broken HOT chains would be that we
	 * might put recently-dead tuples out-of-order in the new table, and there
	 * is little harm in that.)
	 */
	if (!OldIndex->rd_index->indisvalid)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("cannot cluster on invalid index \"%s\"",
						RelationGetRelationName(OldIndex))));

	/* Drop relcache refcnt on OldIndex, but keep lock */
	index_close(OldIndex, NoLock);
}

/*
 * mark_index_clustered: mark the specified index as the one clustered on
 *
 * With indexOid == InvalidOid, will mark all indexes of rel not-clustered.
 */
void
mark_index_clustered(Relation rel, Oid indexOid, bool is_internal)
{
	HeapTuple	indexTuple;
	Form_pg_index indexForm;
	Relation	pg_index;
	ListCell   *index;

	/* Disallow applying to a partitioned table */
	if (rel->rd_rel->relkind == RELKIND_PARTITIONED_TABLE)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("cannot mark index clustered in partitioned table")));

	/*
	 * If the index is already marked clustered, no need to do anything.
	 */
	if (OidIsValid(indexOid))
	{
		if (get_index_isclustered(indexOid))
			return;
	}

	/*
	 * Check each index of the relation and set/clear the bit as needed.
	 */
	pg_index = table_open(IndexRelationId, RowExclusiveLock);

	foreach(index, RelationGetIndexList(rel))
	{
		Oid			thisIndexOid = lfirst_oid(index);

		indexTuple = SearchSysCacheCopy1(INDEXRELID,
										 ObjectIdGetDatum(thisIndexOid));
		if (!HeapTupleIsValid(indexTuple))
			elog(ERROR, "cache lookup failed for index %u", thisIndexOid);
		indexForm = (Form_pg_index) GETSTRUCT(indexTuple);

		/*
		 * Unset the bit if set.  We know it's wrong because we checked this
		 * earlier.
		 */
		if (indexForm->indisclustered)
		{
			indexForm->indisclustered = false;
			CatalogTupleUpdate(pg_index, &indexTuple->t_self, indexTuple);
		}
		else if (thisIndexOid == indexOid)
		{
			/* this was checked earlier, but let's be real sure */
			if (!indexForm->indisvalid)
				elog(ERROR, "cannot cluster on invalid index %u", indexOid);
			indexForm->indisclustered = true;
			CatalogTupleUpdate(pg_index, &indexTuple->t_self, indexTuple);
		}

		InvokeObjectPostAlterHookArg(IndexRelationId, thisIndexOid, 0,
									 InvalidOid, is_internal);

		heap_freetuple(indexTuple);
	}

	table_close(pg_index, RowExclusiveLock);
}

/*
 * Check if the CONCURRENTLY option is legal for the relation.
 */
static void
check_repack_concurrently_requirements(Relation rel)
{
	char		relpersistence,
				replident;
	Oid			ident_idx;

	/* Data changes in system relations are not logically decoded. */
	if (IsCatalogRelation(rel))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("cannot repack relation \"%s\"",
						RelationGetRelationName(rel)),
				 errhint("REPACK CONCURRENTLY is not supported for catalog relations.")));

	/*
	 * reorderbuffer.c does not seem to handle processing of TOAST relation
	 * alone.
	 */
	if (IsToastRelation(rel))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("cannot repack relation \"%s\"",
						RelationGetRelationName(rel)),
				 errhint("REPACK CONCURRENTLY is not supported for TOAST relations, unless the main relation is repacked too.")));

	relpersistence = rel->rd_rel->relpersistence;
	if (relpersistence != RELPERSISTENCE_PERMANENT)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("cannot repack relation \"%s\"",
						RelationGetRelationName(rel)),
				 errhint("REPACK CONCURRENTLY is only allowed for permanent relations.")));

	/* With NOTHING, WAL does not contain the old tuple. */
	replident = rel->rd_rel->relreplident;
	if (replident == REPLICA_IDENTITY_NOTHING)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("cannot repack relation \"%s\"",
						RelationGetRelationName(rel)),
				 errhint("Relation \"%s\" has insufficient replication identity.",
						 RelationGetRelationName(rel))));

	/*
	 * Identity index is not set if the replica identity is FULL, but PK might
	 * exist in such a case.
	 */
	ident_idx = RelationGetReplicaIndex(rel);
	if (!OidIsValid(ident_idx) && OidIsValid(rel->rd_pkindex))
		ident_idx = rel->rd_pkindex;
	if (!OidIsValid(ident_idx))
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("cannot process relation \"%s\"",
						RelationGetRelationName(rel)),
				 errhint("Relation \"%s\" has no identity index.",
						 RelationGetRelationName(rel))));
}


/*
 * rebuild_relation: rebuild an existing relation in index or physical order
 *
 * OldHeap: table to rebuild.  See cluster_rel() for comments on the required
 * lock strength.
 *
 * index: index to cluster by, or NULL to rewrite in physical order.
 *
 * On entry, heap and index (if one is given) must be open, and the
 * appropriate lock held on them -- AccessExclusiveLock for exclusive
 * processing and ShareUpdateExclusiveLock for concurrent processing.
 *
 * On exit, they are closed, but still locked with AccessExclusiveLock.
 * (The function handles the lock upgrade if 'concurrent' is true.)
 */
static void
rebuild_relation(Relation OldHeap, Relation index, bool verbose, bool concurrent)
{
	Oid			tableOid = RelationGetRelid(OldHeap);
	Oid			accessMethod = OldHeap->rd_rel->relam;
	Oid			tableSpace = OldHeap->rd_rel->reltablespace;
	Oid			OIDNewHeap;
	Relation	NewHeap;
	char		relpersistence;
	bool		swap_toast_by_content;
	TransactionId frozenXid;
	MultiXactId cutoffMulti;
	Snapshot	snapshot = NULL;
#if USE_ASSERT_CHECKING
	LOCKMODE	lmode;

	lmode = concurrent ? ShareUpdateExclusiveLock : AccessExclusiveLock;

	Assert(CheckRelationLockedByMe(OldHeap, lmode, false));
	Assert(index == NULL || CheckRelationLockedByMe(index, lmode, false));
#endif

	if (concurrent)
	{
		/*
		 * The worker needs to be member of the locking group we're the leader
		 * of. We ought to become the leader before the worker starts. The
		 * worker will join the group as soon as it starts.
		 *
		 * This is to make sure that the deadlock described below is
		 * detectable by deadlock.c: if the worker waits for a transaction to
		 * complete and we are waiting for the worker output, then effectively
		 * we (i.e. this backend) are waiting for that transaction.
		 */
		BecomeLockGroupLeader();

		/*
		 * Start the worker that decodes data changes applied while we're
		 * copying the table contents.
		 *
		 * Note that the worker has to wait for all transactions with XID
		 * already assigned to finish. If some of those transactions is
		 * waiting for a lock conflicting with ShareUpdateExclusiveLock on our
		 * table (e.g.  it runs CREATE INDEX), we can end up in a deadlock.
		 * Not sure this risk is worth unlocking/locking the table (and its
		 * clustering index) and checking again if its still eligible for
		 * REPACK CONCURRENTLY.
		 */
		start_decoding_worker(tableOid);

		/*
		 * Wait until the worker has the initial snapshot and retrieve it.
		 */
		snapshot = get_initial_snapshot(decoding_worker);

		PushActiveSnapshot(snapshot);
	}

	/* for CLUSTER or REPACK USING INDEX, mark the index as the one to use */
	if (index != NULL)
		mark_index_clustered(OldHeap, RelationGetRelid(index), true);

	/* Remember info about rel before closing OldHeap */
	relpersistence = OldHeap->rd_rel->relpersistence;

	/*
	 * Create the transient table that will receive the re-ordered data.
	 *
	 * OldHeap is already locked, so no need to lock it again.  make_new_heap
	 * obtains AccessExclusiveLock on the new heap and its toast table.
	 */
	OIDNewHeap = make_new_heap(tableOid, tableSpace,
							   accessMethod,
							   relpersistence,
							   NoLock);
	Assert(CheckRelationOidLockedByMe(OIDNewHeap, AccessExclusiveLock, false));
	NewHeap = table_open(OIDNewHeap, NoLock);

	/* Copy the heap data into the new table in the desired order */
	copy_table_data(NewHeap, OldHeap, index, snapshot, verbose,
					&swap_toast_by_content, &frozenXid, &cutoffMulti);

	/* The historic snapshot won't be needed anymore. */
	if (snapshot)
	{
		PopActiveSnapshot();
		UpdateActiveSnapshotCommandId();
	}

	if (concurrent)
	{
		Assert(!swap_toast_by_content);

		/*
		 * Close the index, but keep the lock. Both heaps will be closed by
		 * the following call.
		 */
		if (index)
			index_close(index, NoLock);

		rebuild_relation_finish_concurrent(NewHeap, OldHeap, frozenXid,
										   cutoffMulti);

		pgstat_progress_update_param(PROGRESS_REPACK_PHASE,
									 PROGRESS_REPACK_PHASE_FINAL_CLEANUP);
	}
	else
	{
		bool		is_system_catalog = IsSystemRelation(OldHeap);

		/* Close relcache entries, but keep lock until transaction commit */
		table_close(OldHeap, NoLock);
		if (index)
			index_close(index, NoLock);

		/*
		 * Close the new relation so it can be dropped as soon as the storage
		 * is swapped. The relation is not visible to others, so no need to
		 * unlock it explicitly.
		 */
		table_close(NewHeap, NoLock);

		/*
		 * Swap the physical files of the target and transient tables, then
		 * rebuild the target's indexes and throw away the transient table.
		 */
		finish_heap_swap(tableOid, OIDNewHeap, is_system_catalog,
						 swap_toast_by_content, false, true, true,
						 frozenXid, cutoffMulti,
						 relpersistence);
	}
}


/*
 * Create the transient table that will be filled with new data during
 * CLUSTER, ALTER TABLE, and similar operations.  The transient table
 * duplicates the logical structure of the OldHeap; but will have the
 * specified physical storage properties NewTableSpace, NewAccessMethod, and
 * relpersistence.
 *
 * After this, the caller should load the new heap with transferred/modified
 * data, then call finish_heap_swap to complete the operation.
 */
Oid
make_new_heap(Oid OIDOldHeap, Oid NewTableSpace, Oid NewAccessMethod,
			  char relpersistence, LOCKMODE lockmode)
{
	TupleDesc	OldHeapDesc;
	char		NewHeapName[NAMEDATALEN];
	Oid			OIDNewHeap;
	Oid			toastid;
	Relation	OldHeap;
	HeapTuple	tuple;
	Datum		reloptions;
	bool		isNull;
	Oid			namespaceid;

	OldHeap = table_open(OIDOldHeap, lockmode);
	OldHeapDesc = RelationGetDescr(OldHeap);

	/*
	 * Note that the NewHeap will not receive any of the defaults or
	 * constraints associated with the OldHeap; we don't need 'em, and there's
	 * no reason to spend cycles inserting them into the catalogs only to
	 * delete them.
	 */

	/*
	 * But we do want to use reloptions of the old heap for new heap.
	 */
	tuple = SearchSysCache1(RELOID, ObjectIdGetDatum(OIDOldHeap));
	if (!HeapTupleIsValid(tuple))
		elog(ERROR, "cache lookup failed for relation %u", OIDOldHeap);
	reloptions = SysCacheGetAttr(RELOID, tuple, Anum_pg_class_reloptions,
								 &isNull);
	if (isNull)
		reloptions = (Datum) 0;

	if (relpersistence == RELPERSISTENCE_TEMP)
		namespaceid = LookupCreationNamespace("pg_temp");
	else
		namespaceid = RelationGetNamespace(OldHeap);

	/*
	 * Create the new heap, using a temporary name in the same namespace as
	 * the existing table.  NOTE: there is some risk of collision with user
	 * relnames.  Working around this seems more trouble than it's worth; in
	 * particular, we can't create the new heap in a different namespace from
	 * the old, or we will have problems with the TEMP status of temp tables.
	 *
	 * Note: the new heap is not a shared relation, even if we are rebuilding
	 * a shared rel.  However, we do make the new heap mapped if the source is
	 * mapped.  This simplifies swap_relation_files, and is absolutely
	 * necessary for rebuilding pg_class, for reasons explained there.
	 */
	snprintf(NewHeapName, sizeof(NewHeapName), "pg_temp_%u", OIDOldHeap);

	OIDNewHeap = heap_create_with_catalog(NewHeapName,
										  namespaceid,
										  NewTableSpace,
										  InvalidOid,
										  InvalidOid,
										  InvalidOid,
										  OldHeap->rd_rel->relowner,
										  NewAccessMethod,
										  OldHeapDesc,
										  NIL,
										  RELKIND_RELATION,
										  relpersistence,
										  false,
										  RelationIsMapped(OldHeap),
										  ONCOMMIT_NOOP,
										  reloptions,
										  false,
										  true,
										  true,
										  OIDOldHeap,
										  NULL);
	Assert(OIDNewHeap != InvalidOid);

	ReleaseSysCache(tuple);

	/*
	 * Advance command counter so that the newly-created relation's catalog
	 * tuples will be visible to table_open.
	 */
	CommandCounterIncrement();

	/*
	 * If necessary, create a TOAST table for the new relation.
	 *
	 * If the relation doesn't have a TOAST table already, we can't need one
	 * for the new relation.  The other way around is possible though: if some
	 * wide columns have been dropped, NewHeapCreateToastTable can decide that
	 * no TOAST table is needed for the new table.
	 *
	 * Note that NewHeapCreateToastTable ends with CommandCounterIncrement, so
	 * that the TOAST table will be visible for insertion.
	 */
	toastid = OldHeap->rd_rel->reltoastrelid;
	if (OidIsValid(toastid))
	{
		/* keep the existing toast table's reloptions, if any */
		tuple = SearchSysCache1(RELOID, ObjectIdGetDatum(toastid));
		if (!HeapTupleIsValid(tuple))
			elog(ERROR, "cache lookup failed for relation %u", toastid);
		reloptions = SysCacheGetAttr(RELOID, tuple, Anum_pg_class_reloptions,
									 &isNull);
		if (isNull)
			reloptions = (Datum) 0;

		NewHeapCreateToastTable(OIDNewHeap, reloptions, lockmode, toastid);

		ReleaseSysCache(tuple);
	}

	table_close(OldHeap, NoLock);

	return OIDNewHeap;
}

/*
 * Do the physical copying of table data.
 *
 * 'snapshot' and 'decoding_ctx': see table_relation_copy_for_cluster(). Pass
 * iff concurrent processing is required.
 *
 * There are three output parameters:
 * *pSwapToastByContent is set true if toast tables must be swapped by content.
 * *pFreezeXid receives the TransactionId used as freeze cutoff point.
 * *pCutoffMulti receives the MultiXactId used as a cutoff point.
 */
static void
copy_table_data(Relation NewHeap, Relation OldHeap, Relation OldIndex,
				Snapshot snapshot, bool verbose, bool *pSwapToastByContent,
				TransactionId *pFreezeXid, MultiXactId *pCutoffMulti)
{
	Relation	relRelation;
	HeapTuple	reltup;
	Form_pg_class relform;
	TupleDesc	oldTupDesc PG_USED_FOR_ASSERTS_ONLY;
	TupleDesc	newTupDesc PG_USED_FOR_ASSERTS_ONLY;
	VacuumParams params;
	struct VacuumCutoffs cutoffs;
	bool		use_sort;
	double		num_tuples = 0,
				tups_vacuumed = 0,
				tups_recently_dead = 0;
	BlockNumber num_pages;
	int			elevel = verbose ? INFO : DEBUG2;
	PGRUsage	ru0;
	char	   *nspname;
	bool		concurrent = snapshot != NULL;
	LOCKMODE	lmode;

	lmode = concurrent ? ShareUpdateExclusiveLock : AccessExclusiveLock;

	pg_rusage_init(&ru0);

	/* Store a copy of the namespace name for logging purposes */
	nspname = get_namespace_name(RelationGetNamespace(OldHeap));

	/*
	 * Their tuple descriptors should be exactly alike, but here we only need
	 * assume that they have the same number of columns.
	 */
	oldTupDesc = RelationGetDescr(OldHeap);
	newTupDesc = RelationGetDescr(NewHeap);
	Assert(newTupDesc->natts == oldTupDesc->natts);

	/*
	 * If the OldHeap has a toast table, get lock on the toast table to keep
	 * it from being vacuumed.  This is needed because autovacuum processes
	 * toast tables independently of their main tables, with no lock on the
	 * latter.  If an autovacuum were to start on the toast table after we
	 * compute our OldestXmin below, it would use a later OldestXmin, and then
	 * possibly remove as DEAD toast tuples belonging to main tuples we think
	 * are only RECENTLY_DEAD.  Then we'd fail while trying to copy those
	 * tuples.
	 *
	 * We don't need to open the toast relation here, just lock it.  The lock
	 * will be held till end of transaction.
	 */
	if (OldHeap->rd_rel->reltoastrelid)
		LockRelationOid(OldHeap->rd_rel->reltoastrelid, lmode);

	/*
	 * If both tables have TOAST tables, perform toast swap by content.  It is
	 * possible that the old table has a toast table but the new one doesn't,
	 * if toastable columns have been dropped.  In that case we have to do
	 * swap by links.  This is okay because swap by content is only essential
	 * for system catalogs, and we don't support schema changes for them.
	 */
	if (OldHeap->rd_rel->reltoastrelid && NewHeap->rd_rel->reltoastrelid &&
		!concurrent)
	{
		*pSwapToastByContent = true;

		/*
		 * When doing swap by content, any toast pointers written into NewHeap
		 * must use the old toast table's OID, because that's where the toast
		 * data will eventually be found.  Set this up by setting rd_toastoid.
		 * This also tells toast_save_datum() to preserve the toast value
		 * OIDs, which we want so as not to invalidate toast pointers in
		 * system catalog caches, and to avoid making multiple copies of a
		 * single toast value.
		 *
		 * Note that we must hold NewHeap open until we are done writing data,
		 * since the relcache will not guarantee to remember this setting once
		 * the relation is closed.  Also, this technique depends on the fact
		 * that no one will try to read from the NewHeap until after we've
		 * finished writing it and swapping the rels --- otherwise they could
		 * follow the toast pointers to the wrong place.  (It would actually
		 * work for values copied over from the old toast table, but not for
		 * any values that we toast which were previously not toasted.)
		 *
		 * This would not work with CONCURRENTLY because we may need to delete
		 * TOASTed tuples from the new heap. With this hack, we'd delete them
		 * from the old heap.
		 */
		NewHeap->rd_toastoid = OldHeap->rd_rel->reltoastrelid;
	}
	else
		*pSwapToastByContent = false;

	/*
	 * Compute xids used to freeze and weed out dead tuples and multixacts.
	 * Since we're going to rewrite the whole table anyway, there's no reason
	 * not to be aggressive about this.
	 */
	memset(&params, 0, sizeof(VacuumParams));
	vacuum_get_cutoffs(OldHeap, params, &cutoffs);

	/*
	 * FreezeXid will become the table's new relfrozenxid, and that mustn't go
	 * backwards, so take the max.
	 */
	{
		TransactionId relfrozenxid = OldHeap->rd_rel->relfrozenxid;

		if (TransactionIdIsValid(relfrozenxid) &&
			TransactionIdPrecedes(cutoffs.FreezeLimit, relfrozenxid))
			cutoffs.FreezeLimit = relfrozenxid;
	}

	/*
	 * MultiXactCutoff, similarly, shouldn't go backwards either.
	 */
	{
		MultiXactId relminmxid = OldHeap->rd_rel->relminmxid;

		if (MultiXactIdIsValid(relminmxid) &&
			MultiXactIdPrecedes(cutoffs.MultiXactCutoff, relminmxid))
			cutoffs.MultiXactCutoff = relminmxid;
	}

	/*
	 * Decide whether to use an indexscan or seqscan-and-optional-sort to scan
	 * the OldHeap.  We know how to use a sort to duplicate the ordering of a
	 * btree index, and will use seqscan-and-sort for that case if the planner
	 * tells us it's cheaper.  Otherwise, always indexscan if an index is
	 * provided, else plain seqscan.
	 */
	if (OldIndex != NULL && OldIndex->rd_rel->relam == BTREE_AM_OID)
		use_sort = plan_cluster_use_sort(RelationGetRelid(OldHeap),
										 RelationGetRelid(OldIndex));
	else
		use_sort = false;

	/* Log what we're doing */
	if (OldIndex != NULL && !use_sort)
		ereport(elevel,
				errmsg("repacking \"%s.%s\" using index scan on \"%s\"",
					   nspname,
					   RelationGetRelationName(OldHeap),
					   RelationGetRelationName(OldIndex)));
	else if (use_sort)
		ereport(elevel,
				errmsg("repacking \"%s.%s\" using sequential scan and sort",
					   nspname,
					   RelationGetRelationName(OldHeap)));
	else
		ereport(elevel,
				errmsg("repacking \"%s.%s\" in physical order",
					   nspname,
					   RelationGetRelationName(OldHeap)));

	/*
	 * Hand off the actual copying to AM specific function, the generic code
	 * cannot know how to deal with visibility across AMs. Note that this
	 * routine is allowed to set FreezeXid / MultiXactCutoff to different
	 * values (e.g. because the AM doesn't use freezing).
	 */
	table_relation_copy_for_cluster(OldHeap, NewHeap, OldIndex, use_sort,
									cutoffs.OldestXmin, snapshot,
									&cutoffs.FreezeLimit,
									&cutoffs.MultiXactCutoff,
									&num_tuples, &tups_vacuumed,
									&tups_recently_dead);

	/* return selected values to caller, get set as relfrozenxid/minmxid */
	*pFreezeXid = cutoffs.FreezeLimit;
	*pCutoffMulti = cutoffs.MultiXactCutoff;

	/*
	 * Reset rd_toastoid just to be tidy --- it shouldn't be looked at again.
	 * In the CONCURRENTLY case, we need to set it again before applying the
	 * concurrent changes.
	 */
	NewHeap->rd_toastoid = InvalidOid;

	num_pages = RelationGetNumberOfBlocks(NewHeap);

	/* Log what we did */
	ereport(elevel,
			(errmsg("\"%s.%s\": found %.0f removable, %.0f nonremovable row versions in %u pages",
					nspname,
					RelationGetRelationName(OldHeap),
					tups_vacuumed, num_tuples,
					RelationGetNumberOfBlocks(OldHeap)),
			 errdetail("%.0f dead row versions cannot be removed yet.\n"
					   "%s.",
					   tups_recently_dead,
					   pg_rusage_show(&ru0))));

	/* Update pg_class to reflect the correct values of pages and tuples. */
	relRelation = table_open(RelationRelationId, RowExclusiveLock);

	reltup = SearchSysCacheCopy1(RELOID,
								 ObjectIdGetDatum(RelationGetRelid(NewHeap)));
	if (!HeapTupleIsValid(reltup))
		elog(ERROR, "cache lookup failed for relation %u",
			 RelationGetRelid(NewHeap));
	relform = (Form_pg_class) GETSTRUCT(reltup);

	relform->relpages = num_pages;
	relform->reltuples = num_tuples;

	/* Don't update the stats for pg_class.  See swap_relation_files. */
	if (RelationGetRelid(OldHeap) != RelationRelationId)
		CatalogTupleUpdate(relRelation, &reltup->t_self, reltup);
	else
		CacheInvalidateRelcacheByTuple(reltup);

	/* Clean up. */
	heap_freetuple(reltup);
	table_close(relRelation, RowExclusiveLock);

	/* Make the update visible */
	CommandCounterIncrement();
}

/*
 * Swap the physical files of two given relations.
 *
 * We swap the physical identity (reltablespace, relfilenumber) while keeping
 * the same logical identities of the two relations.  relpersistence is also
 * swapped, which is critical since it determines where buffers live for each
 * relation.
 *
 * We can swap associated TOAST data in either of two ways: recursively swap
 * the physical content of the toast tables (and their indexes), or swap the
 * TOAST links in the given relations' pg_class entries.  The former is needed
 * to manage rewrites of shared catalogs (where we cannot change the pg_class
 * links) while the latter is the only way to handle cases in which a toast
 * table is added or removed altogether.
 *
 * Additionally, the first relation is marked with relfrozenxid set to
 * frozenXid.  It seems a bit ugly to have this here, but the caller would
 * have to do it anyway, so having it here saves a heap_update.  Note: in
 * the swap-toast-links case, we assume we don't need to change the toast
 * table's relfrozenxid: the new version of the toast table should already
 * have relfrozenxid set to RecentXmin, which is good enough.
 *
 * Lastly, if r2 and its toast table and toast index (if any) are mapped,
 * their OIDs are emitted into mapped_tables[].  This is hacky but beats
 * having to look the information up again later in finish_heap_swap.
 */
static void
swap_relation_files(Oid r1, Oid r2, bool target_is_pg_class,
					bool swap_toast_by_content,
					bool is_internal,
					TransactionId frozenXid,
					MultiXactId cutoffMulti,
					Oid *mapped_tables)
{
	Relation	relRelation;
	HeapTuple	reltup1,
				reltup2;
	Form_pg_class relform1,
				relform2;
	RelFileNumber relfilenumber1,
				relfilenumber2;
	RelFileNumber swaptemp;
	char		swptmpchr;
	Oid			relam1,
				relam2;

	/* We need writable copies of both pg_class tuples. */
	relRelation = table_open(RelationRelationId, RowExclusiveLock);

	reltup1 = SearchSysCacheCopy1(RELOID, ObjectIdGetDatum(r1));
	if (!HeapTupleIsValid(reltup1))
		elog(ERROR, "cache lookup failed for relation %u", r1);
	relform1 = (Form_pg_class) GETSTRUCT(reltup1);

	reltup2 = SearchSysCacheCopy1(RELOID, ObjectIdGetDatum(r2));
	if (!HeapTupleIsValid(reltup2))
		elog(ERROR, "cache lookup failed for relation %u", r2);
	relform2 = (Form_pg_class) GETSTRUCT(reltup2);

	relfilenumber1 = relform1->relfilenode;
	relfilenumber2 = relform2->relfilenode;
	relam1 = relform1->relam;
	relam2 = relform2->relam;

	if (RelFileNumberIsValid(relfilenumber1) &&
		RelFileNumberIsValid(relfilenumber2))
	{
		/*
		 * Normal non-mapped relations: swap relfilenumbers, reltablespaces,
		 * relpersistence
		 */
		Assert(!target_is_pg_class);

		swaptemp = relform1->relfilenode;
		relform1->relfilenode = relform2->relfilenode;
		relform2->relfilenode = swaptemp;

		swaptemp = relform1->reltablespace;
		relform1->reltablespace = relform2->reltablespace;
		relform2->reltablespace = swaptemp;

		swaptemp = relform1->relam;
		relform1->relam = relform2->relam;
		relform2->relam = swaptemp;

		swptmpchr = relform1->relpersistence;
		relform1->relpersistence = relform2->relpersistence;
		relform2->relpersistence = swptmpchr;

		/* Also swap toast links, if we're swapping by links */
		if (!swap_toast_by_content)
		{
			swaptemp = relform1->reltoastrelid;
			relform1->reltoastrelid = relform2->reltoastrelid;
			relform2->reltoastrelid = swaptemp;
		}
	}
	else
	{
		/*
		 * Mapped-relation case.  Here we have to swap the relation mappings
		 * instead of modifying the pg_class columns.  Both must be mapped.
		 */
		if (RelFileNumberIsValid(relfilenumber1) ||
			RelFileNumberIsValid(relfilenumber2))
			elog(ERROR, "cannot swap mapped relation \"%s\" with non-mapped relation",
				 NameStr(relform1->relname));

		/*
		 * We can't change the tablespace nor persistence of a mapped rel, and
		 * we can't handle toast link swapping for one either, because we must
		 * not apply any critical changes to its pg_class row.  These cases
		 * should be prevented by upstream permissions tests, so these checks
		 * are non-user-facing emergency backstop.
		 */
		if (relform1->reltablespace != relform2->reltablespace)
			elog(ERROR, "cannot change tablespace of mapped relation \"%s\"",
				 NameStr(relform1->relname));
		if (relform1->relpersistence != relform2->relpersistence)
			elog(ERROR, "cannot change persistence of mapped relation \"%s\"",
				 NameStr(relform1->relname));
		if (relform1->relam != relform2->relam)
			elog(ERROR, "cannot change access method of mapped relation \"%s\"",
				 NameStr(relform1->relname));
		if (!swap_toast_by_content &&
			(relform1->reltoastrelid || relform2->reltoastrelid))
			elog(ERROR, "cannot swap toast by links for mapped relation \"%s\"",
				 NameStr(relform1->relname));

		/*
		 * Fetch the mappings --- shouldn't fail, but be paranoid
		 */
		relfilenumber1 = RelationMapOidToFilenumber(r1, relform1->relisshared);
		if (!RelFileNumberIsValid(relfilenumber1))
			elog(ERROR, "could not find relation mapping for relation \"%s\", OID %u",
				 NameStr(relform1->relname), r1);
		relfilenumber2 = RelationMapOidToFilenumber(r2, relform2->relisshared);
		if (!RelFileNumberIsValid(relfilenumber2))
			elog(ERROR, "could not find relation mapping for relation \"%s\", OID %u",
				 NameStr(relform2->relname), r2);

		/*
		 * Send replacement mappings to relmapper.  Note these won't actually
		 * take effect until CommandCounterIncrement.
		 */
		RelationMapUpdateMap(r1, relfilenumber2, relform1->relisshared, false);
		RelationMapUpdateMap(r2, relfilenumber1, relform2->relisshared, false);

		/* Pass OIDs of mapped r2 tables back to caller */
		*mapped_tables++ = r2;
	}

	/*
	 * Recognize that rel1's relfilenumber (swapped from rel2) is new in this
	 * subtransaction. The rel2 storage (swapped from rel1) may or may not be
	 * new.
	 */
	{
		Relation	rel1,
					rel2;

		rel1 = relation_open(r1, NoLock);
		rel2 = relation_open(r2, NoLock);
		rel2->rd_createSubid = rel1->rd_createSubid;
		rel2->rd_newRelfilelocatorSubid = rel1->rd_newRelfilelocatorSubid;
		rel2->rd_firstRelfilelocatorSubid = rel1->rd_firstRelfilelocatorSubid;
		RelationAssumeNewRelfilelocator(rel1);
		relation_close(rel1, NoLock);
		relation_close(rel2, NoLock);
	}

	/*
	 * In the case of a shared catalog, these next few steps will only affect
	 * our own database's pg_class row; but that's okay, because they are all
	 * noncritical updates.  That's also an important fact for the case of a
	 * mapped catalog, because it's possible that we'll commit the map change
	 * and then fail to commit the pg_class update.
	 */

	/* set rel1's frozen Xid and minimum MultiXid */
	if (relform1->relkind != RELKIND_INDEX)
	{
		Assert(!TransactionIdIsValid(frozenXid) ||
			   TransactionIdIsNormal(frozenXid));
		relform1->relfrozenxid = frozenXid;
		relform1->relminmxid = cutoffMulti;
	}

	/* swap size statistics too, since new rel has freshly-updated stats */
	{
		int32		swap_pages;
		float4		swap_tuples;
		int32		swap_allvisible;
		int32		swap_allfrozen;

		swap_pages = relform1->relpages;
		relform1->relpages = relform2->relpages;
		relform2->relpages = swap_pages;

		swap_tuples = relform1->reltuples;
		relform1->reltuples = relform2->reltuples;
		relform2->reltuples = swap_tuples;

		swap_allvisible = relform1->relallvisible;
		relform1->relallvisible = relform2->relallvisible;
		relform2->relallvisible = swap_allvisible;

		swap_allfrozen = relform1->relallfrozen;
		relform1->relallfrozen = relform2->relallfrozen;
		relform2->relallfrozen = swap_allfrozen;
	}

	/*
	 * Update the tuples in pg_class --- unless the target relation of the
	 * swap is pg_class itself.  In that case, there is zero point in making
	 * changes because we'd be updating the old data that we're about to throw
	 * away.  Because the real work being done here for a mapped relation is
	 * just to change the relation map settings, it's all right to not update
	 * the pg_class rows in this case. The most important changes will instead
	 * performed later, in finish_heap_swap() itself.
	 */
	if (!target_is_pg_class)
	{
		CatalogIndexState indstate;

		indstate = CatalogOpenIndexes(relRelation);
		CatalogTupleUpdateWithInfo(relRelation, &reltup1->t_self, reltup1,
								   indstate);
		CatalogTupleUpdateWithInfo(relRelation, &reltup2->t_self, reltup2,
								   indstate);
		CatalogCloseIndexes(indstate);
	}
	else
	{
		/* no update ... but we do still need relcache inval */
		CacheInvalidateRelcacheByTuple(reltup1);
		CacheInvalidateRelcacheByTuple(reltup2);
	}

	/*
	 * Now that pg_class has been updated with its relevant information for
	 * the swap, update the dependency of the relations to point to their new
	 * table AM, if it has changed.
	 */
	if (relam1 != relam2)
	{
		if (changeDependencyFor(RelationRelationId,
								r1,
								AccessMethodRelationId,
								relam1,
								relam2) != 1)
			elog(ERROR, "could not change access method dependency for relation \"%s.%s\"",
				 get_namespace_name(get_rel_namespace(r1)),
				 get_rel_name(r1));
		if (changeDependencyFor(RelationRelationId,
								r2,
								AccessMethodRelationId,
								relam2,
								relam1) != 1)
			elog(ERROR, "could not change access method dependency for relation \"%s.%s\"",
				 get_namespace_name(get_rel_namespace(r2)),
				 get_rel_name(r2));
	}

	/*
	 * Post alter hook for modified relations. The change to r2 is always
	 * internal, but r1 depends on the invocation context.
	 */
	InvokeObjectPostAlterHookArg(RelationRelationId, r1, 0,
								 InvalidOid, is_internal);
	InvokeObjectPostAlterHookArg(RelationRelationId, r2, 0,
								 InvalidOid, true);

	/*
	 * If we have toast tables associated with the relations being swapped,
	 * deal with them too.
	 */
	if (relform1->reltoastrelid || relform2->reltoastrelid)
	{
		if (swap_toast_by_content)
		{
			if (relform1->reltoastrelid && relform2->reltoastrelid)
			{
				/* Recursively swap the contents of the toast tables */
				swap_relation_files(relform1->reltoastrelid,
									relform2->reltoastrelid,
									target_is_pg_class,
									swap_toast_by_content,
									is_internal,
									frozenXid,
									cutoffMulti,
									mapped_tables);
			}
			else
			{
				/* caller messed up */
				elog(ERROR, "cannot swap toast files by content when there's only one");
			}
		}
		else
		{
			/*
			 * We swapped the ownership links, so we need to change dependency
			 * data to match.
			 *
			 * NOTE: it is possible that only one table has a toast table.
			 *
			 * NOTE: at present, a TOAST table's only dependency is the one on
			 * its owning table.  If more are ever created, we'd need to use
			 * something more selective than deleteDependencyRecordsFor() to
			 * get rid of just the link we want.
			 */
			ObjectAddress baseobject,
						toastobject;
			long		count;

			/*
			 * We disallow this case for system catalogs, to avoid the
			 * possibility that the catalog we're rebuilding is one of the
			 * ones the dependency changes would change.  It's too late to be
			 * making any data changes to the target catalog.
			 */
			if (IsSystemClass(r1, relform1))
				elog(ERROR, "cannot swap toast files by links for system catalogs");

			/* Delete old dependencies */
			if (relform1->reltoastrelid)
			{
				count = deleteDependencyRecordsFor(RelationRelationId,
												   relform1->reltoastrelid,
												   false);
				if (count != 1)
					elog(ERROR, "expected one dependency record for TOAST table, found %ld",
						 count);
			}
			if (relform2->reltoastrelid)
			{
				count = deleteDependencyRecordsFor(RelationRelationId,
												   relform2->reltoastrelid,
												   false);
				if (count != 1)
					elog(ERROR, "expected one dependency record for TOAST table, found %ld",
						 count);
			}

			/* Register new dependencies */
			baseobject.classId = RelationRelationId;
			baseobject.objectSubId = 0;
			toastobject.classId = RelationRelationId;
			toastobject.objectSubId = 0;

			if (relform1->reltoastrelid)
			{
				baseobject.objectId = r1;
				toastobject.objectId = relform1->reltoastrelid;
				recordDependencyOn(&toastobject, &baseobject,
								   DEPENDENCY_INTERNAL);
			}

			if (relform2->reltoastrelid)
			{
				baseobject.objectId = r2;
				toastobject.objectId = relform2->reltoastrelid;
				recordDependencyOn(&toastobject, &baseobject,
								   DEPENDENCY_INTERNAL);
			}
		}
	}

	/*
	 * If we're swapping two toast tables by content, do the same for their
	 * valid index. The swap can actually be safely done only if the relations
	 * have indexes.
	 */
	if (swap_toast_by_content &&
		relform1->relkind == RELKIND_TOASTVALUE &&
		relform2->relkind == RELKIND_TOASTVALUE)
	{
		Oid			toastIndex1,
					toastIndex2;

		/* Get valid index for each relation */
		toastIndex1 = toast_get_valid_index(r1,
											AccessExclusiveLock);
		toastIndex2 = toast_get_valid_index(r2,
											AccessExclusiveLock);

		swap_relation_files(toastIndex1,
							toastIndex2,
							target_is_pg_class,
							swap_toast_by_content,
							is_internal,
							InvalidTransactionId,
							InvalidMultiXactId,
							mapped_tables);
	}

	/* Clean up. */
	heap_freetuple(reltup1);
	heap_freetuple(reltup2);

	table_close(relRelation, RowExclusiveLock);
}

/*
 * Remove the transient table that was built by make_new_heap, and finish
 * cleaning up (including rebuilding all indexes on the old heap).
 */
void
finish_heap_swap(Oid OIDOldHeap, Oid OIDNewHeap,
				 bool is_system_catalog,
				 bool swap_toast_by_content,
				 bool check_constraints,
				 bool is_internal,
				 bool reindex,
				 TransactionId frozenXid,
				 MultiXactId cutoffMulti,
				 char newrelpersistence)
{
	ObjectAddress object;
	Oid			mapped_tables[4];
	int			i;

	/* Report that we are now swapping relation files */
	pgstat_progress_update_param(PROGRESS_REPACK_PHASE,
								 PROGRESS_REPACK_PHASE_SWAP_REL_FILES);

	/* Zero out possible results from swapped_relation_files */
	memset(mapped_tables, 0, sizeof(mapped_tables));

	/*
	 * Swap the contents of the heap relations (including any toast tables).
	 * Also set old heap's relfrozenxid to frozenXid.
	 */
	swap_relation_files(OIDOldHeap, OIDNewHeap,
						(OIDOldHeap == RelationRelationId),
						swap_toast_by_content, is_internal,
						frozenXid, cutoffMulti, mapped_tables);

	/*
	 * If it's a system catalog, queue a sinval message to flush all catcaches
	 * on the catalog when we reach CommandCounterIncrement.
	 */
	if (is_system_catalog)
		CacheInvalidateCatalog(OIDOldHeap);

	if (reindex)
	{
		int			reindex_flags;
		ReindexParams reindex_params = {0};

		/*
		 * Rebuild each index on the relation (but not the toast table, which
		 * is all-new at this point).  It is important to do this before the
		 * DROP step because if we are processing a system catalog that will
		 * be used during DROP, we want to have its indexes available.  There
		 * is no advantage to the other order anyway because this is all
		 * transactional, so no chance to reclaim disk space before commit. We
		 * do not need a final CommandCounterIncrement() because
		 * reindex_relation does it.
		 *
		 * Note: because index_build is called via reindex_relation, it will
		 * never set indcheckxmin true for the indexes.  This is OK even
		 * though in some sense we are building new indexes rather than
		 * rebuilding existing ones, because the new heap won't contain any
		 * HOT chains at all, let alone broken ones, so it can't be necessary
		 * to set indcheckxmin.
		 */
		reindex_flags = REINDEX_REL_SUPPRESS_INDEX_USE;
		if (check_constraints)
			reindex_flags |= REINDEX_REL_CHECK_CONSTRAINTS;

		/*
		 * Ensure that the indexes have the same persistence as the parent
		 * relation.
		 */
		if (newrelpersistence == RELPERSISTENCE_UNLOGGED)
			reindex_flags |= REINDEX_REL_FORCE_INDEXES_UNLOGGED;
		else if (newrelpersistence == RELPERSISTENCE_PERMANENT)
			reindex_flags |= REINDEX_REL_FORCE_INDEXES_PERMANENT;

		/* Report that we are now reindexing relations */
		pgstat_progress_update_param(PROGRESS_REPACK_PHASE,
									 PROGRESS_REPACK_PHASE_REBUILD_INDEX);

		reindex_relation(NULL, OIDOldHeap, reindex_flags, &reindex_params);
	}

	/* Report that we are now doing clean up */
	pgstat_progress_update_param(PROGRESS_REPACK_PHASE,
								 PROGRESS_REPACK_PHASE_FINAL_CLEANUP);

	/*
	 * If the relation being rebuilt is pg_class, swap_relation_files()
	 * couldn't update pg_class's own pg_class entry (check comments in
	 * swap_relation_files()), thus relfrozenxid was not updated. That's
	 * annoying because a potential reason for doing a VACUUM FULL is a
	 * imminent or actual anti-wraparound shutdown.  So, now that we can
	 * access the new relation using its indices, update relfrozenxid.
	 * pg_class doesn't have a toast relation, so we don't need to update the
	 * corresponding toast relation. Not that there's little point moving all
	 * relfrozenxid updates here since swap_relation_files() needs to write to
	 * pg_class for non-mapped relations anyway.
	 */
	if (OIDOldHeap == RelationRelationId)
	{
		Relation	relRelation;
		HeapTuple	reltup;
		Form_pg_class relform;

		relRelation = table_open(RelationRelationId, RowExclusiveLock);

		reltup = SearchSysCacheCopy1(RELOID, ObjectIdGetDatum(OIDOldHeap));
		if (!HeapTupleIsValid(reltup))
			elog(ERROR, "cache lookup failed for relation %u", OIDOldHeap);
		relform = (Form_pg_class) GETSTRUCT(reltup);

		relform->relfrozenxid = frozenXid;
		relform->relminmxid = cutoffMulti;

		CatalogTupleUpdate(relRelation, &reltup->t_self, reltup);

		table_close(relRelation, RowExclusiveLock);
	}

	/* Destroy new heap with old filenumber */
	object.classId = RelationRelationId;
	object.objectId = OIDNewHeap;
	object.objectSubId = 0;

	if (!reindex)
	{
		/*
		 * Make sure the changes in pg_class are visible. This is especially
		 * important if !swap_toast_by_content, so that the correct TOAST
		 * relation is dropped. (reindex_relation() above did not help in this
		 * case))
		 */
		CommandCounterIncrement();
	}

	/*
	 * The new relation is local to our transaction and we know nothing
	 * depends on it, so DROP_RESTRICT should be OK.
	 */
	performDeletion(&object, DROP_RESTRICT, PERFORM_DELETION_INTERNAL);

	/* performDeletion does CommandCounterIncrement at end */

	/*
	 * Now we must remove any relation mapping entries that we set up for the
	 * transient table, as well as its toast table and toast index if any. If
	 * we fail to do this before commit, the relmapper will complain about new
	 * permanent map entries being added post-bootstrap.
	 */
	for (i = 0; OidIsValid(mapped_tables[i]); i++)
		RelationMapRemoveMapping(mapped_tables[i]);

	/*
	 * At this point, everything is kosher except that, if we did toast swap
	 * by links, the toast table's name corresponds to the transient table.
	 * The name is irrelevant to the backend because it's referenced by OID,
	 * but users looking at the catalogs could be confused.  Rename it to
	 * prevent this problem.
	 *
	 * Note no lock required on the relation, because we already hold an
	 * exclusive lock on it.
	 */
	if (!swap_toast_by_content)
	{
		Relation	newrel;

		newrel = table_open(OIDOldHeap, NoLock);
		if (OidIsValid(newrel->rd_rel->reltoastrelid))
		{
			Oid			toastidx;
			char		NewToastName[NAMEDATALEN];

			/* Get the associated valid index to be renamed */
			toastidx = toast_get_valid_index(newrel->rd_rel->reltoastrelid,
											 AccessExclusiveLock);

			/* rename the toast table ... */
			snprintf(NewToastName, NAMEDATALEN, "pg_toast_%u",
					 OIDOldHeap);
			RenameRelationInternal(newrel->rd_rel->reltoastrelid,
								   NewToastName, true, false);

			/* ... and its valid index too. */
			snprintf(NewToastName, NAMEDATALEN, "pg_toast_%u_index",
					 OIDOldHeap);

			RenameRelationInternal(toastidx,
								   NewToastName, true, true);

			/*
			 * Reset the relrewrite for the toast. The command-counter
			 * increment is required here as we are about to update the tuple
			 * that is updated as part of RenameRelationInternal.
			 */
			CommandCounterIncrement();
			ResetRelRewrite(newrel->rd_rel->reltoastrelid);
		}
		relation_close(newrel, NoLock);
	}

	/* if it's not a catalog table, clear any missing attribute settings */
	if (!is_system_catalog)
	{
		Relation	newrel;

		newrel = table_open(OIDOldHeap, NoLock);
		RelationClearMissing(newrel);
		relation_close(newrel, NoLock);
	}
}

/*
 * Determine which relations to process, when REPACK/CLUSTER is called
 * without specifying a table name.  The exact process depends on whether
 * USING INDEX was given or not, and in any case we only return tables and
 * materialized views that the current user has privileges to repack/cluster.
 *
 * If USING INDEX was given, we scan pg_index to find those that have
 * indisclustered set; if it was not given, scan pg_class and return all
 * tables.
 *
 * Return it as a list of RelToCluster in the given memory context.
 */
static List *
get_tables_to_repack(RepackCommand cmd, bool usingindex, MemoryContext permcxt)
{
	Relation	catalog;
	TableScanDesc scan;
	HeapTuple	tuple;
	List	   *rtcs = NIL;

	if (usingindex)
	{
		ScanKeyData entry;

		catalog = table_open(IndexRelationId, AccessShareLock);
		ScanKeyInit(&entry,
					Anum_pg_index_indisclustered,
					BTEqualStrategyNumber, F_BOOLEQ,
					BoolGetDatum(true));
		scan = table_beginscan_catalog(catalog, 1, &entry);
		while ((tuple = heap_getnext(scan, ForwardScanDirection)) != NULL)
		{
			RelToCluster *rtc;
			Form_pg_index index;
			MemoryContext oldcxt;

			index = (Form_pg_index) GETSTRUCT(tuple);

			/*
			 * Try to obtain a light lock on the index's table, to ensure it
			 * doesn't go away while we collect the list.  If we cannot, just
			 * disregard it.
			 */
			if (!ConditionalLockRelationOid(index->indrelid, AccessShareLock))
				continue;

			/* Verify that the table still exists */
			if (!SearchSysCacheExists1(RELOID, ObjectIdGetDatum(index->indrelid)))
			{
				/* Release useless lock */
				UnlockRelationOid(index->indrelid, AccessShareLock);
				continue;
			}

			if (!cluster_is_permitted_for_relation(cmd, index->indrelid,
												   GetUserId()))
				continue;

			/* Use a permanent memory context for the result list */
			oldcxt = MemoryContextSwitchTo(permcxt);
			rtc = palloc_object(RelToCluster);
			rtc->tableOid = index->indrelid;
			rtc->indexOid = index->indexrelid;
			rtcs = lappend(rtcs, rtc);
			MemoryContextSwitchTo(oldcxt);
		}
	}
	else
	{
		catalog = table_open(RelationRelationId, AccessShareLock);
		scan = table_beginscan_catalog(catalog, 0, NULL);

		while ((tuple = heap_getnext(scan, ForwardScanDirection)) != NULL)
		{
			RelToCluster *rtc;
			Form_pg_class class;
			MemoryContext oldcxt;

			class = (Form_pg_class) GETSTRUCT(tuple);

			/*
			 * Try to obtain a light lock on the table, to ensure it doesn't
			 * go away while we collect the list.  If we cannot, just
			 * disregard the table.
			 */
			if (!ConditionalLockRelationOid(class->oid, AccessShareLock))
				continue;

			/* Verify that the table still exists */
			if (!SearchSysCacheExists1(RELOID, ObjectIdGetDatum(class->oid)))
			{
				/* Release useless lock */
				UnlockRelationOid(class->oid, AccessShareLock);
				continue;
			}

			/* Can only process plain tables and matviews */
			if (class->relkind != RELKIND_RELATION &&
				class->relkind != RELKIND_MATVIEW)
				continue;

			/* noisily skip rels which the user can't process */
			if (!cluster_is_permitted_for_relation(cmd, class->oid,
												   GetUserId()))
				continue;

			/* Use a permanent memory context for the result list */
			oldcxt = MemoryContextSwitchTo(permcxt);
			rtc = palloc_object(RelToCluster);
			rtc->tableOid = class->oid;
			rtc->indexOid = InvalidOid;
			rtcs = lappend(rtcs, rtc);
			MemoryContextSwitchTo(oldcxt);
		}
	}

	table_endscan(scan);
	relation_close(catalog, AccessShareLock);

	return rtcs;
}

/*
 * Given a partitioned table or its index, return a list of RelToCluster for
 * all the children leaves tables/indexes.
 *
 * Like expand_vacuum_rel, but here caller must hold AccessExclusiveLock
 * on the table containing the index.
 *
 * 'rel_is_index' tells whether 'relid' is that of an index (true) or of the
 * owning relation.
 */
static List *
get_tables_to_repack_partitioned(RepackCommand cmd, Oid relid,
								 bool rel_is_index, MemoryContext permcxt)
{
	List	   *inhoids;
	List	   *rtcs = NIL;

	/*
	 * Do not lock the children until they're processed.  Note that we do hold
	 * a lock on the parent partitioned table.
	 */
	inhoids = find_all_inheritors(relid, NoLock, NULL);
	foreach_oid(child_oid, inhoids)
	{
		Oid			table_oid,
					index_oid;
		RelToCluster *rtc;
		MemoryContext oldcxt;

		if (rel_is_index)
		{
			/* consider only leaf indexes */
			if (get_rel_relkind(child_oid) != RELKIND_INDEX)
				continue;

			table_oid = IndexGetRelation(child_oid, false);
			index_oid = child_oid;
		}
		else
		{
			/* consider only leaf relations */
			if (get_rel_relkind(child_oid) != RELKIND_RELATION)
				continue;

			table_oid = child_oid;
			index_oid = InvalidOid;
		}

		/*
		 * It's possible that the user does not have privileges to CLUSTER the
		 * leaf partition despite having them on the partitioned table.  Skip
		 * if so.
		 */
		if (!cluster_is_permitted_for_relation(cmd, table_oid, GetUserId()))
			continue;

		/* Use a permanent memory context for the result list */
		oldcxt = MemoryContextSwitchTo(permcxt);
		rtc = palloc_object(RelToCluster);
		rtc->tableOid = table_oid;
		rtc->indexOid = index_oid;
		rtcs = lappend(rtcs, rtc);
		MemoryContextSwitchTo(oldcxt);
	}

	return rtcs;
}

/*
 * Return whether userid has privileges to CLUSTER relid.  If not, this
 * function emits a WARNING.
 */
static bool
cluster_is_permitted_for_relation(RepackCommand cmd, Oid relid, Oid userid)
{
	Assert(cmd == REPACK_COMMAND_CLUSTER || cmd == REPACK_COMMAND_REPACK);

	if (pg_class_aclcheck(relid, userid, ACL_MAINTAIN) == ACLCHECK_OK)
		return true;

	ereport(WARNING,
			errmsg("permission denied to execute %s on \"%s\", skipping it",
				   RepackCommandAsString(cmd),
				   get_rel_name(relid)));

	return false;
}


/*
 * Given a RepackStmt with an indicated relation name, resolve the relation
 * name, obtain lock on it, then determine what to do based on the relation
 * type: if it's table and not partitioned, repack it as indicated (using an
 * existing clustered index, or following the given one), and return NULL.
 *
 * On the other hand, if the table is partitioned, do nothing further and
 * instead return the opened and locked relcache entry, so that caller can
 * process the partitions using the multiple-table handling code.  In this
 * case, if an index name is given, it's up to the caller to resolve it.
 */
static Relation
process_single_relation(RepackStmt *stmt, LOCKMODE lockmode, bool isTopLevel,
						ClusterParams *params)
{
	Relation	rel;
	Oid			tableOid;

	Assert(stmt->relation != NULL);
	Assert(stmt->command == REPACK_COMMAND_CLUSTER ||
		   stmt->command == REPACK_COMMAND_REPACK);

	/* Find, lock, and check permissions on the table. */
	tableOid = RangeVarGetRelidExtended(stmt->relation->relation,
										lockmode,
										0,
										RangeVarCallbackMaintainsTable,
										NULL);
	rel = table_open(tableOid, NoLock);

	/*
	 * Reject clustering a remote temp table ... their local buffer manager is
	 * not going to cope.
	 */
	if (RELATION_IS_OTHER_TEMP(rel))
		ereport(ERROR,
				errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				errmsg("cannot execute %s on temporary tables of other sessions",
					   RepackCommandAsString(stmt->command)));

	/*
	 * Make sure ANALYZE is specified if a column list is present.
	 */
	if ((params->options & CLUOPT_ANALYZE) == 0 && stmt->relation->va_cols != NIL)
		ereport(ERROR,
				errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				errmsg("ANALYZE option must be specified when a column list is provided"));

	/*
	 * For partitioned tables, let caller handle this.  Otherwise, process it
	 * here and we're done.
	 */
	if (rel->rd_rel->relkind == RELKIND_PARTITIONED_TABLE)
		return rel;
	else
	{
		Oid			indexOid = InvalidOid;

		indexOid = determine_clustered_index(rel, stmt->usingindex,
											 stmt->indexname);
		if (OidIsValid(indexOid))
			check_index_is_clusterable(rel, indexOid, lockmode);

		cluster_rel(stmt->command, rel, indexOid, params, isTopLevel);

		/* Do an analyze, if requested */
		if (params->options & CLUOPT_ANALYZE)
		{
			VacuumParams vac_params = {0};

			vac_params.options |= VACOPT_ANALYZE;
			if (params->options & CLUOPT_VERBOSE)
				vac_params.options |= VACOPT_VERBOSE;
			analyze_rel(tableOid, NULL, vac_params,
						stmt->relation->va_cols, true, NULL);
		}

		return NULL;
	}
}

/*
 * Given a relation and the usingindex/indexname options in a
 * REPACK USING INDEX or CLUSTER command, return the OID of the
 * index to use for clustering the table.
 *
 * Caller must hold lock on the relation so that the set of indexes
 * doesn't change, and must call check_index_is_clusterable.
 */
static Oid
determine_clustered_index(Relation rel, bool usingindex, const char *indexname)
{
	Oid			indexOid;

	if (indexname == NULL && usingindex)
	{
		/*
		 * If USING INDEX with no name is given, find a clustered index, or
		 * error out if none.
		 */
		indexOid = InvalidOid;
		foreach_oid(idxoid, RelationGetIndexList(rel))
		{
			if (get_index_isclustered(idxoid))
			{
				indexOid = idxoid;
				break;
			}
		}

		if (!OidIsValid(indexOid))
			ereport(ERROR,
					errcode(ERRCODE_UNDEFINED_OBJECT),
					errmsg("there is no previously clustered index for table \"%s\"",
						   RelationGetRelationName(rel)));
	}
	else if (indexname != NULL)
	{
		/* An index was specified; obtain its OID. */
		indexOid = get_relname_relid(indexname, rel->rd_rel->relnamespace);
		if (!OidIsValid(indexOid))
			ereport(ERROR,
					errcode(ERRCODE_UNDEFINED_OBJECT),
					errmsg("index \"%s\" for table \"%s\" does not exist",
						   indexname, RelationGetRelationName(rel)));
	}
	else
		indexOid = InvalidOid;

	return indexOid;
}

static const char *
RepackCommandAsString(RepackCommand cmd)
{
	switch (cmd)
	{
		case REPACK_COMMAND_REPACK:
			return "REPACK";
		case REPACK_COMMAND_VACUUMFULL:
			return "VACUUM";
		case REPACK_COMMAND_CLUSTER:
			return "CLUSTER";
	}
	return "???";	/* keep compiler quiet */
}


/*
 * Is this backend performing logical decoding on behalf of REPACK
 * (CONCURRENTLY) ?
 */
bool
am_decoding_for_repack(void)
{
	return OidIsValid(repacked_rel_locator.relNumber);
}

/*
 * Does the WAL record contain a data change that this backend does not need
 * to decode on behalf of REPACK (CONCURRENTLY)?
 */
bool
change_useless_for_repack(XLogRecordBuffer *buf)
{
	XLogReaderState *r = buf->record;
	RelFileLocator locator;

	/* TOAST locator should not be set unless the main is. */
	Assert(!OidIsValid(repacked_rel_toast_locator.relNumber) ||
		   OidIsValid(repacked_rel_locator.relNumber));

	/*
	 * Backends not involved in REPACK (CONCURRENTLY) should not do the
	 * filtering.
	 */
	if (!am_decoding_for_repack())
		return false;

	/*
	 * If the record does not contain the block 0, it's probably not INSERT /
	 * UPDATE / DELETE. In any case, we do not have enough information to
	 * filter the change out.
	 */
	if (!XLogRecGetBlockTagExtended(r, 0, &locator, NULL, NULL, NULL))
		return false;

	/*
	 * Decode the change if it belongs to the table we are repacking, or if it
	 * belongs to its TOAST relation.
	 */
	if (RelFileLocatorEquals(locator, repacked_rel_locator))
		return false;
	if (OidIsValid(repacked_rel_toast_locator.relNumber) &&
		RelFileLocatorEquals(locator, repacked_rel_toast_locator))
		return false;

	/* Filter out changes of other tables. */
	return true;
}

/*
 * This function is much like pg_create_logical_replication_slot() except that
 * the new slot is neither released (if anyone else could read changes from
 * our slot, we could miss changes other backends do while we copy the
 * existing data into temporary table), nor persisted (it's easier to handle
 * crash by restarting all the work from scratch).
 */
static LogicalDecodingContext *
setup_logical_decoding(Oid relid)
{
	Relation	rel;
	Oid			toastrelid;
	LogicalDecodingContext *ctx;
	NameData	slotname;
	RepackDecodingState *dstate;

	/*
	 * REPACK CONCURRENTLY is not allowed in a transaction block, so this
	 * should never fire.
	 */
	Assert(!TransactionIdIsValid(GetTopTransactionIdIfAny()));

	/*
	 * Check if we can use logical decoding.
	 */
	CheckSlotPermissions();
	CheckLogicalDecodingRequirements();

	/*
	 * A single backend should not execute multiple REPACK commands at a time,
	 * so use PID to make the slot unique.
	 *
	 * RS_TEMPORARY so that the slot gets cleaned up on ERROR.
	 */
	snprintf(NameStr(slotname), NAMEDATALEN, "repack_%d", MyProcPid);
	ReplicationSlotCreate(NameStr(slotname), true, RS_TEMPORARY, false, false,
						  false);

	/*
	 * Neither prepare_write nor do_write callback nor update_progress is
	 * useful for us.
	 */
	ctx = CreateInitDecodingContext(REPL_PLUGIN_NAME,
									NIL,
									true,
									InvalidXLogRecPtr,
									XL_ROUTINE(.page_read = read_local_xlog_page,
											   .segment_open = wal_segment_open,
											   .segment_close = wal_segment_close),
									NULL, NULL, NULL);

	/*
	 * We don't have control on setting fast_forward, so at least check it.
	 */
	Assert(!ctx->fast_forward);

	DecodingContextFindStartpoint(ctx);

	/*
	 * decode_concurrent_changes() needs non-blocking callback.
	 */
	ctx->reader->routine.page_read = read_local_xlog_page_no_wait;

	/*
	 * read_local_xlog_page_no_wait() needs to be able to indicate the end of
	 * WAL.
	 */
	ctx->reader->private_data = MemoryContextAllocZero(ctx->context,
													   sizeof(ReadLocalXLogPageNoWaitPrivate));


	/* Some WAL records should have been read. */
	Assert(ctx->reader->EndRecPtr != InvalidXLogRecPtr);

	/*
	 * Initialize repack_current_segment so that we can notice WAL segment
	 * boundaries.
	 */
	XLByteToSeg(ctx->reader->EndRecPtr, repack_current_segment,
				wal_segment_size);

	dstate = palloc0_object(RepackDecodingState);
	dstate->relid = relid;

	/*
	 * Tuple descriptor may be needed to flatten a tuple before we write it to
	 * a file. A copy is needed because the decoding worker invalidates system
	 * caches before it starts to do the actual work.
	 */
	rel = table_open(relid, AccessShareLock);
	dstate->tupdesc = CreateTupleDescCopy(RelationGetDescr(rel));

	/* Avoid logical decoding of other relations. */
	repacked_rel_locator = rel->rd_locator;
	toastrelid = rel->rd_rel->reltoastrelid;
	if (OidIsValid(toastrelid))
	{
		Relation	toastrel;

		/* Avoid logical decoding of other TOAST relations. */
		toastrel = table_open(toastrelid, AccessShareLock);
		repacked_rel_toast_locator = toastrel->rd_locator;
		table_close(toastrel, AccessShareLock);
	}
	table_close(rel, AccessShareLock);

	/* The file will be set as soon as we have it opened. */
	dstate->file = NULL;

	ctx->output_writer_private = dstate;

	return ctx;
}

/*
 * Decode logical changes from the WAL sequence and store them to a file.
 *
 * If true is returned, there is no more work for the worker.
 */
static bool
decode_concurrent_changes(LogicalDecodingContext *ctx,
						  DecodingWorkerShared *shared)
{
	RepackDecodingState *dstate;
	XLogRecPtr	lsn_upto;
	bool		done;
	char		fname[MAXPGPATH];

	dstate = (RepackDecodingState *) ctx->output_writer_private;

	/* Open the output file. */
	DecodingWorkerFileName(fname, shared->relid, shared->last_exported + 1);
	dstate->file = BufFileCreateFileSet(&shared->sfs.fs, fname);

	SpinLockAcquire(&shared->mutex);
	lsn_upto = shared->lsn_upto;
	done = shared->done;
	SpinLockRelease(&shared->mutex);

	while (true)
	{
		XLogRecord *record;
		XLogSegNo	segno_new;
		char	   *errm = NULL;
		XLogRecPtr	end_lsn;

		CHECK_FOR_INTERRUPTS();

		record = XLogReadRecord(ctx->reader, &errm);
		if (record)
		{
			LogicalDecodingProcessRecord(ctx, ctx->reader);

			/*
			 * If WAL segment boundary has been crossed, inform the decoding
			 * system that the catalog_xmin can advance.
			 *
			 * TODO Does it make sense to confirm more often? Segment size
			 * seems appropriate for restart_lsn (because less than a segment
			 * cannot be recycled anyway), however more frequent checks might
			 * be beneficial for catalog_xmin.
			 */
			end_lsn = ctx->reader->EndRecPtr;
			XLByteToSeg(end_lsn, segno_new, wal_segment_size);
			if (segno_new != repack_current_segment)
			{
				LogicalConfirmReceivedLocation(end_lsn);
				elog(DEBUG1, "REPACK: confirmed receive location %X/%X",
					 (uint32) (end_lsn >> 32), (uint32) end_lsn);
				repack_current_segment = segno_new;
			}
		}
		else
		{
			ReadLocalXLogPageNoWaitPrivate *priv;

			if (errm)
				ereport(ERROR, (errmsg("%s", errm)));

			/*
			 * In the decoding loop we do not want to get blocked when there
			 * is no more WAL available, otherwise the loop would become
			 * uninterruptible.
			 */
			priv = (ReadLocalXLogPageNoWaitPrivate *)
				ctx->reader->private_data;
			if (priv->end_of_wal)
				/* Do not miss the end of WAL condition next time. */
				priv->end_of_wal = false;
			else
				ereport(ERROR, (errmsg("could not read WAL record")));
		}

		/*
		 * Whether we could read new record or not, keep checking if
		 * 'lsn_upto' was specified.
		 */
		if (XLogRecPtrIsInvalid(lsn_upto))
		{
			SpinLockAcquire(&shared->mutex);
			lsn_upto = shared->lsn_upto;
			/* 'done' should be set at the same time as 'lsn_upto' */
			done = shared->done;
			SpinLockRelease(&shared->mutex);
		}
		if (!XLogRecPtrIsInvalid(lsn_upto) &&
			ctx->reader->EndRecPtr >= lsn_upto)
			break;

		if (record == NULL)
		{
			int64 timeout = 0;
			WaitLSNResult	res;

			/*
			 * Before we retry reading, wait until new WAL is flushed.
			 *
			 * There is a race condition such that the backend executing
			 * REPACK determines 'lsn_upto', but before it sets the shared
			 * variable, we reach the end of WAL. In that case we'd need to
			 * wait until the next WAL flush (unrelated to REPACK). Although
			 * that should not be a problem in a busy system, it might be
			 * noticeable in other cases, including regression tests (which
			 * are not necessarily executed in parallel). Therefore it makes
			 * sense to use timeout.
			 *
			 * If lsn_upto is valid, WAL records having LSN lower than that
			 * should already have been flushed to disk.
			 */
			if (XLogRecPtrIsInvalid(lsn_upto))
				timeout = 100L;
			res = WaitForLSN(WAIT_LSN_TYPE_PRIMARY_FLUSH,
							 ctx->reader->EndRecPtr + 1,
							 timeout);
			if (res != WAIT_LSN_RESULT_SUCCESS &&
				res != WAIT_LSN_RESULT_TIMEOUT)
				ereport(ERROR, (errmsg("waiting for WAL failed")));
		}
	}

	/*
	 * Close the file so we can make it available to the backend.
	 */
	BufFileClose(dstate->file);
	dstate->file = NULL;
	SpinLockAcquire(&shared->mutex);
	shared->lsn_upto = InvalidXLogRecPtr;
	shared->last_exported++;
	SpinLockRelease(&shared->mutex);
	ConditionVariableSignal(&shared->cv);

	return done;
}

/*
 * Apply changes stored in 'file'.
 */
static void
apply_concurrent_changes(BufFile *file, ChangeDest *dest)
{
	char		kind;
	uint32		t_len;
	Relation	rel = dest->rel;
	TupleTableSlot *index_slot,
			   *ident_slot;
	HeapTuple	tup_old = NULL;

	/* TupleTableSlot is needed to pass the tuple to ExecInsertIndexTuples(). */
	index_slot = MakeSingleTupleTableSlot(RelationGetDescr(rel),
										  &TTSOpsHeapTuple);

	/* A slot to fetch tuples from identity index. */
	ident_slot = table_slot_create(rel, NULL);

	while (true)
	{
		size_t		nread;
		HeapTuple	tup,
					tup_exist;

		CHECK_FOR_INTERRUPTS();

		nread = BufFileReadMaybeEOF(file, &kind, 1, true);
		/* Are we done with the file? */
		if (nread == 0)
			break;

		/* Read the tuple. */
		BufFileReadExact(file, &t_len, sizeof(t_len));
		tup = (HeapTuple) palloc(HEAPTUPLESIZE + t_len);
		tup->t_data = (HeapTupleHeader) ((char *) tup + HEAPTUPLESIZE);
		BufFileReadExact(file, tup->t_data, t_len);
		tup->t_len = t_len;
		ItemPointerSetInvalid(&tup->t_self);
		tup->t_tableOid = RelationGetRelid(dest->rel);

		if (kind == CHANGE_UPDATE_OLD)
		{
			Assert(tup_old == NULL);
			tup_old = tup;
		}
		else if (kind == CHANGE_INSERT)
		{
			Assert(tup_old == NULL);

			apply_concurrent_insert(rel, tup, dest->iistate, index_slot);

			pfree(tup);
		}
		else if (kind == CHANGE_UPDATE_NEW || kind == CHANGE_DELETE)
		{
			HeapTuple	tup_key;

			if (kind == CHANGE_UPDATE_NEW)
			{
				tup_key = tup_old != NULL ? tup_old : tup;
			}
			else
			{
				Assert(tup_old == NULL);
				tup_key = tup;
			}

			/*
			 * Find the tuple to be updated or deleted.
			 */
			tup_exist = find_target_tuple(rel, dest, tup_key, ident_slot);
			if (tup_exist == NULL)
				elog(ERROR, "failed to find target tuple");

			if (kind == CHANGE_UPDATE_NEW)
				apply_concurrent_update(rel, tup, tup_exist, dest->iistate,
										index_slot);
			else
				apply_concurrent_delete(rel, tup_exist);

			if (tup_old != NULL)
			{
				pfree(tup_old);
				tup_old = NULL;
			}

			pfree(tup);
		}
		else
			elog(ERROR, "unrecognized kind of change: %d", kind);

		/*
		 * If a change was applied now, increment CID for next writes and
		 * update the snapshot so it sees the changes we've applied so far.
		 */
		if (kind != CHANGE_UPDATE_OLD)
		{
			CommandCounterIncrement();
			UpdateActiveSnapshotCommandId();
		}
	}

	/* Cleanup. */
	ExecDropSingleTupleTableSlot(index_slot);
	ExecDropSingleTupleTableSlot(ident_slot);
}

static void
apply_concurrent_insert(Relation rel, HeapTuple tup, IndexInsertState *iistate,
						TupleTableSlot *index_slot)
{
	List	   *recheck;

	/*
	 * Like simple_heap_insert(), but make sure that the INSERT is not
	 * logically decoded - see reform_and_rewrite_tuple() for more
	 * information.
	 */
	heap_insert(rel, tup, GetCurrentCommandId(true), HEAP_INSERT_NO_LOGICAL,
				NULL);

	/*
	 * Update indexes.
	 *
	 * In case functions in the index need the active snapshot and caller
	 * hasn't set one.
	 */
	ExecStoreHeapTuple(tup, index_slot, false);
	recheck = ExecInsertIndexTuples(iistate->rri,
									index_slot,
									iistate->estate,
									false,	/* update */
									false,	/* noDupErr */
									NULL,	/* specConflict */
									NIL,	/* arbiterIndexes */
									false	/* onlySummarizing */
		);

	/*
	 * If recheck is required, it must have been performed on the source
	 * relation by now. (All the logical changes we process here are already
	 * committed.)
	 */
	list_free(recheck);

	pgstat_progress_incr_param(PROGRESS_REPACK_HEAP_TUPLES_INSERTED, 1);
}

static void
apply_concurrent_update(Relation rel, HeapTuple tup, HeapTuple tup_target,
						IndexInsertState *iistate, TupleTableSlot *index_slot)
{
	LockTupleMode lockmode;
	TM_FailureData tmfd;
	TU_UpdateIndexes update_indexes;
	TM_Result	res;
	List	   *recheck;

	/*
	 * Write the new tuple into the new heap. ('tup' gets the TID assigned
	 * here.)
	 *
	 * Do it like in simple_heap_update(), except for 'wal_logical' (and
	 * except for 'wait').
	 */
	res = heap_update(rel, &tup_target->t_self, tup,
					  GetCurrentCommandId(true),
					  InvalidSnapshot,
					  false,	/* no wait - only we are doing changes */
					  &tmfd, &lockmode, &update_indexes,
					  false /* wal_logical */ );
	if (res != TM_Ok)
		ereport(ERROR, (errmsg("failed to apply concurrent UPDATE")));

	ExecStoreHeapTuple(tup, index_slot, false);

	if (update_indexes != TU_None)
	{
		recheck = ExecInsertIndexTuples(iistate->rri,
										index_slot,
										iistate->estate,
										true,	/* update */
										false,	/* noDupErr */
										NULL,	/* specConflict */
										NIL,	/* arbiterIndexes */
		/* onlySummarizing */
										update_indexes == TU_Summarizing);
		list_free(recheck);
	}

	pgstat_progress_incr_param(PROGRESS_REPACK_HEAP_TUPLES_UPDATED, 1);
}

static void
apply_concurrent_delete(Relation rel, HeapTuple tup_target)
{
	TM_Result	res;
	TM_FailureData tmfd;

	/*
	 * Delete tuple from the new heap.
	 *
	 * Do it like in simple_heap_delete(), except for 'wal_logical' (and
	 * except for 'wait').
	 */
	res = heap_delete(rel, &tup_target->t_self, GetCurrentCommandId(true),
					  InvalidSnapshot, false,
					  &tmfd,
					  false,	/* no wait - only we are doing changes */
					  false /* wal_logical */ );

	if (res != TM_Ok)
		ereport(ERROR, (errmsg("failed to apply concurrent DELETE")));

	pgstat_progress_incr_param(PROGRESS_REPACK_HEAP_TUPLES_DELETED, 1);
}

/*
 * Find the tuple to be updated or deleted.
 *
 * 'tup_key' is a tuple containing the key values for the scan.
 */
static HeapTuple
find_target_tuple(Relation rel, ChangeDest *dest, HeapTuple tup_key,
				  TupleTableSlot *ident_slot)
{
	Relation	ident_index = dest->ident_index;
	IndexScanDesc scan;
	Form_pg_index ident_form;
	int2vector *ident_indkey;
	HeapTuple	result = NULL;

	/* XXX no instrumentation for now */
	scan = index_beginscan(rel, ident_index, GetActiveSnapshot(),
						   NULL, dest->ident_key_nentries, 0);

	/*
	 * Scan key is passed by caller, so it does not have to be constructed
	 * multiple times. Key entries have all fields initialized, except for
	 * sk_argument.
	 */
	index_rescan(scan, dest->ident_key, dest->ident_key_nentries, NULL, 0);

	/* Info needed to retrieve key values from heap tuple. */
	ident_form = ident_index->rd_index;
	ident_indkey = &ident_form->indkey;

	/* Use the incoming tuple to finalize the scan key. */
	for (int i = 0; i < scan->numberOfKeys; i++)
	{
		ScanKey		entry;
		bool		isnull;
		int16		attno_heap;

		entry = &scan->keyData[i];
		attno_heap = ident_indkey->values[i];
		entry->sk_argument = heap_getattr(tup_key,
										  attno_heap,
										  rel->rd_att,
										  &isnull);
		Assert(!isnull);
	}
	if (index_getnext_slot(scan, ForwardScanDirection, ident_slot))
	{
		bool		shouldFree;

		result = ExecFetchSlotHeapTuple(ident_slot, false, &shouldFree);
		/* TTSOpsBufferHeapTuple has .get_heap_tuple != NULL. */
		Assert(!shouldFree);
	}
	index_endscan(scan);

	return result;
}

/*
 * Decode and apply concurrent changes, up to (and including) the record whose
 * LSN is 'end_of_wal'.
 */
static void
process_concurrent_changes(XLogRecPtr end_of_wal, ChangeDest *dest, bool done)
{
	DecodingWorkerShared *shared;
	char		fname[MAXPGPATH];
	BufFile    *file;

	pgstat_progress_update_param(PROGRESS_REPACK_PHASE,
								 PROGRESS_REPACK_PHASE_CATCH_UP);

	/* Ask the worker for the file. */
	shared = (DecodingWorkerShared *) dsm_segment_address(decoding_worker->seg);
	SpinLockAcquire(&shared->mutex);
	shared->lsn_upto = end_of_wal;
	shared->done = done;
	SpinLockRelease(&shared->mutex);

	/*
	 * The worker needs to finish processing of the current WAL record. Even
	 * if it's idle, it'll need to close the output file. Thus we're likely to
	 * wait, so prepare for sleep.
	 */
	ConditionVariablePrepareToSleep(&shared->cv);
	for (;;)
	{
		int		last_exported;

		SpinLockAcquire(&shared->mutex);
		last_exported = shared->last_exported;
		SpinLockRelease(&shared->mutex);

		/*
		 * Has the worker exported the file we are waiting for?
		 */
		if (last_exported == dest->file_seq)
			break;

		ConditionVariableSleep(&shared->cv, WAIT_EVENT_REPACK_WORKER_EXPORT);
	}
	ConditionVariableCancelSleep();

	/* Open the file. */
	DecodingWorkerFileName(fname, shared->relid, dest->file_seq);
	file = BufFileOpenFileSet(&shared->sfs.fs, fname, O_RDONLY, false);
	apply_concurrent_changes(file, dest);

	BufFileClose(file);

	/* Get ready for the next file. */
	dest->file_seq++;
}

/*
 * Initialize IndexInsertState for index specified by ident_index_id.
 *
 * While doing that, also return the identity index in *ident_index_p.
 */
static IndexInsertState *
get_index_insert_state(Relation relation, Oid ident_index_id,
					   Relation *ident_index_p)
{
	EState	   *estate;
	int			i;
	IndexInsertState *result;
	Relation	ident_index = NULL;

	result = (IndexInsertState *) palloc0(sizeof(IndexInsertState));
	estate = CreateExecutorState();

	result->rri = (ResultRelInfo *) palloc(sizeof(ResultRelInfo));
	InitResultRelInfo(result->rri, relation, 0, 0, 0);
	ExecOpenIndices(result->rri, false);

	/*
	 * Find the relcache entry of the identity index so that we spend no extra
	 * effort to open / close it.
	 */
	for (i = 0; i < result->rri->ri_NumIndices; i++)
	{
		Relation	ind_rel;

		ind_rel = result->rri->ri_IndexRelationDescs[i];
		if (ind_rel->rd_id == ident_index_id)
			ident_index = ind_rel;
	}
	if (ident_index == NULL)
		elog(ERROR, "failed to open identity index");

	/* Only initialize fields needed by ExecInsertIndexTuples(). */
	result->estate = estate;

	*ident_index_p = ident_index;
	return result;
}

/*
 * Build scan key to process logical changes.
 */
static ScanKey
build_identity_key(Oid ident_idx_oid, Relation rel_src, int *nentries)
{
	Relation	ident_idx_rel;
	Form_pg_index ident_idx;
	int			n,
				i;
	ScanKey		result;

	Assert(OidIsValid(ident_idx_oid));
	ident_idx_rel = index_open(ident_idx_oid, AccessShareLock);
	ident_idx = ident_idx_rel->rd_index;
	n = ident_idx->indnatts;
	result = (ScanKey) palloc(sizeof(ScanKeyData) * n);
	for (i = 0; i < n; i++)
	{
		ScanKey		entry;
		int16		relattno;
		Form_pg_attribute att;
		Oid			opfamily,
					opcintype,
					opno,
					opcode;

		entry = &result[i];
		relattno = ident_idx->indkey.values[i];
		if (relattno >= 1)
		{
			TupleDesc	desc;

			desc = rel_src->rd_att;
			att = TupleDescAttr(desc, relattno - 1);
		}
		else
			elog(ERROR, "unexpected attribute number %d in index", relattno);

		opfamily = ident_idx_rel->rd_opfamily[i];
		opcintype = ident_idx_rel->rd_opcintype[i];
		opno = get_opfamily_member(opfamily, opcintype, opcintype,
								   BTEqualStrategyNumber);

		if (!OidIsValid(opno))
			elog(ERROR, "failed to find = operator for type %u", opcintype);

		opcode = get_opcode(opno);
		if (!OidIsValid(opcode))
			elog(ERROR, "failed to find = operator for operator %u", opno);

		/* Initialize everything but argument. */
		ScanKeyInit(entry,
					i + 1,
					BTEqualStrategyNumber, opcode,
					(Datum) NULL);
		entry->sk_collation = att->attcollation;
	}
	index_close(ident_idx_rel, AccessShareLock);

	*nentries = n;
	return result;
}

static void
free_index_insert_state(IndexInsertState *iistate)
{
	ExecCloseIndices(iistate->rri);
	FreeExecutorState(iistate->estate);
	pfree(iistate->rri);
	pfree(iistate);
}

static void
cleanup_logical_decoding(LogicalDecodingContext *ctx)
{
	RepackDecodingState *dstate;

	dstate = (RepackDecodingState *) ctx->output_writer_private;

	FreeTupleDesc(dstate->tupdesc);
	FreeDecodingContext(ctx);

	ReplicationSlotDropAcquired();
	pfree(dstate);
}

/*
 * The final steps of rebuild_relation() for concurrent processing.
 *
 * On entry, NewHeap is locked in AccessExclusiveLock mode. OldHeap and its
 * clustering index (if one is passed) are still locked in a mode that allows
 * concurrent data changes. On exit, both tables and their indexes are closed,
 * but locked in AccessExclusiveLock mode.
 */
static void
rebuild_relation_finish_concurrent(Relation NewHeap, Relation OldHeap,
								   TransactionId frozenXid,
								   MultiXactId cutoffMulti)
{
	LOCKMODE	lockmode_old PG_USED_FOR_ASSERTS_ONLY;
	List	   *ind_oids_new;
	Oid			old_table_oid = RelationGetRelid(OldHeap);
	Oid			new_table_oid = RelationGetRelid(NewHeap);
	List	   *ind_oids_old = RelationGetIndexList(OldHeap);
	ListCell   *lc,
			   *lc2;
	char		relpersistence;
	bool		is_system_catalog;
	Oid			ident_idx_old,
				ident_idx_new;
	XLogRecPtr	wal_insert_ptr,
				end_of_wal;
	char		dummy_rec_data = '\0';
	Relation   *ind_refs,
			   *ind_refs_p;
	int			nind;
	ChangeDest	chgdst;

	/* Like in cluster_rel(). */
	lockmode_old = ShareUpdateExclusiveLock;
	Assert(CheckRelationLockedByMe(OldHeap, lockmode_old, false));
	/* This is expected from the caller. */
	Assert(CheckRelationLockedByMe(NewHeap, AccessExclusiveLock, false));

	ident_idx_old = RelationGetReplicaIndex(OldHeap);

	/*
	 * Unlike the exclusive case, we build new indexes for the new relation
	 * rather than swapping the storage and reindexing the old relation. The
	 * point is that the index build can take some time, so we do it before we
	 * get AccessExclusiveLock on the old heap and therefore we cannot swap
	 * the heap storage yet.
	 *
	 * index_create() will lock the new indexes using AccessExclusiveLock - no
	 * need to change that. At the same time, we use ShareUpdateExclusiveLock
	 * to lock the existing indexes - that should be enough to prevent others
	 * from changing them while we're repacking the relation. The lock on
	 * table should prevent others from changing the index column list, but
	 * might not be enough for commands like ALTER INDEX ... SET ... (Those
	 * are not necessarily dangerous, but can make user confused if the
	 * changes they do get lost due to REPACK.)
	 */
	ind_oids_new = build_new_indexes(NewHeap, OldHeap, ind_oids_old);

	/*
	 * Processing shouldn't start w/o valid identity index.
	 */
	Assert(OidIsValid(ident_idx_old));

	/* Find "identity index" on the new relation. */
	ident_idx_new = InvalidOid;
	forboth(lc, ind_oids_old, lc2, ind_oids_new)
	{
		Oid			ind_old = lfirst_oid(lc);
		Oid			ind_new = lfirst_oid(lc2);

		if (ident_idx_old == ind_old)
		{
			ident_idx_new = ind_new;
			break;
		}
	}
	if (!OidIsValid(ident_idx_new))

		/*
		 * Should not happen, given our lock on the old relation.
		 */
		ereport(ERROR,
				(errmsg("identity index missing on the new relation")));

	/* Gather information to apply concurrent changes. */
	chgdst.rel = NewHeap;
	chgdst.iistate = get_index_insert_state(NewHeap, ident_idx_new,
											&chgdst.ident_index);
	chgdst.ident_key = build_identity_key(ident_idx_new, OldHeap,
										  &chgdst.ident_key_nentries);
	chgdst.file_seq = WORKER_FILE_SNAPSHOT + 1;

	/*
	 * During testing, wait for another backend to perform concurrent data
	 * changes which we will process below.
	 */
	INJECTION_POINT("repack-concurrently-before-lock", NULL);

	/*
	 * Flush all WAL records inserted so far (possibly except for the last
	 * incomplete page, see GetInsertRecPtr), to minimize the amount of data
	 * we need to flush while holding exclusive lock on the source table.
	 */
	wal_insert_ptr = GetInsertRecPtr();
	XLogFlush(wal_insert_ptr);
	end_of_wal = GetFlushRecPtr(NULL);

	/*
	 * Apply concurrent changes first time, to minimize the time we need to
	 * hold AccessExclusiveLock. (Quite some amount of WAL could have been
	 * written during the data copying and index creation.)
	 */
	process_concurrent_changes(end_of_wal, &chgdst, false);

	/*
	 * Acquire AccessExclusiveLock on the table, its TOAST relation (if there
	 * is one), all its indexes, so that we can swap the files.
	 */
	LockRelationOid(old_table_oid, AccessExclusiveLock);

	/*
	 * Lock all indexes now, not only the clustering one: all indexes need to
	 * have their files swapped. While doing that, store their relation
	 * references in an array, to handle predicate locks below.
	 */
	ind_refs_p = ind_refs = palloc_array(Relation, list_length(ind_oids_old));
	nind = 0;
	foreach_oid(ind_oid, ind_oids_old)
	{
		Relation	index;

		index = index_open(ind_oid, AccessExclusiveLock);

		/*
		 * TODO 1) Do we need to check if ALTER INDEX was executed since the
		 * new index was created in build_new_indexes()? 2) Specifically for
		 * the clustering index, should check_index_is_clusterable() be called
		 * here? (Not sure about the latter: ShareUpdateExclusiveLock on the
		 * table probably blocks all commands that affect the result of
		 * check_index_is_clusterable().)
		 */
		*ind_refs_p = index;
		ind_refs_p++;
		nind++;
	}

	/*
	 * Lock the OldHeap's TOAST relation exclusively - again, the lock is
	 * needed to swap the files.
	 */
	if (OidIsValid(OldHeap->rd_rel->reltoastrelid))
		LockRelationOid(OldHeap->rd_rel->reltoastrelid, AccessExclusiveLock);

	/*
	 * Tuples and pages of the old heap will be gone, but the heap will stay.
	 */
	TransferPredicateLocksToHeapRelation(OldHeap);
	/* The same for indexes. */
	for (int i = 0; i < nind; i++)
	{
		Relation	index = ind_refs[i];

		TransferPredicateLocksToHeapRelation(index);

		/*
		 * References to indexes on the old relation are not needed anymore,
		 * however locks stay till the end of the transaction.
		 */
		index_close(index, NoLock);
	}
	pfree(ind_refs);

	/*
	 * Flush anything we see in WAL, to make sure that all changes committed
	 * while we were waiting for the exclusive lock are available for
	 * decoding. This should not be necessary if all backends had
	 * synchronous_commit set, but we can't rely on this setting.
	 *
	 * Unfortunately, GetInsertRecPtr() may lag behind the actual insert
	 * position, and GetLastImportantRecPtr() points at the start of the last
	 * record rather than at the end. Thus the simplest way to determine the
	 * insert position is to insert a dummy record and use its LSN.
	 *
	 * XXX Consider using GetLastImportantRecPtr() and adding the size of the
	 * last record (plus the total size of all the page headers the record
	 * spans)?
	 */
	XLogBeginInsert();
	XLogRegisterData(&dummy_rec_data, 1);
	wal_insert_ptr = XLogInsert(RM_XLOG_ID, XLOG_NOOP);
	XLogFlush(wal_insert_ptr);
	end_of_wal = GetFlushRecPtr(NULL);

	/*
	 * Apply the concurrent changes again. Indicate that the decoding worker
	 * won't be needed anymore.
	 */
	process_concurrent_changes(end_of_wal, &chgdst, true);

	/* Remember info about rel before closing OldHeap */
	relpersistence = OldHeap->rd_rel->relpersistence;
	is_system_catalog = IsSystemRelation(OldHeap);

	pgstat_progress_update_param(PROGRESS_REPACK_PHASE,
								 PROGRESS_REPACK_PHASE_SWAP_REL_FILES);

	/*
	 * Even ShareUpdateExclusiveLock should have prevented others from
	 * creating / dropping indexes (even using the CONCURRENTLY option), so we
	 * do not need to check whether the lists match.
	 */
	forboth(lc, ind_oids_old, lc2, ind_oids_new)
	{
		Oid			ind_old = lfirst_oid(lc);
		Oid			ind_new = lfirst_oid(lc2);
		Oid			mapped_tables[4];

		/* Zero out possible results from swapped_relation_files */
		memset(mapped_tables, 0, sizeof(mapped_tables));

		swap_relation_files(ind_old, ind_new,
							(old_table_oid == RelationRelationId),
							false,	/* swap_toast_by_content */
							true,
							InvalidTransactionId,
							InvalidMultiXactId,
							mapped_tables);

#ifdef USE_ASSERT_CHECKING

		/*
		 * Concurrent processing is not supported for system relations, so
		 * there should be no mapped tables.
		 */
		for (int i = 0; i < 4; i++)
			Assert(mapped_tables[i] == 0);
#endif
	}

	/* The new indexes must be visible for deletion. */
	CommandCounterIncrement();

	/* Close the old heap but keep lock until transaction commit. */
	table_close(OldHeap, NoLock);
	/* Close the new heap. (We didn't have to open its indexes). */
	table_close(NewHeap, NoLock);

	/* Cleanup what we don't need anymore. (And close the identity index.) */
	pfree(chgdst.ident_key);
	free_index_insert_state(chgdst.iistate);

	/*
	 * Swap the relations and their TOAST relations and TOAST indexes. This
	 * also drops the new relation and its indexes.
	 *
	 * (System catalogs are currently not supported.)
	 */
	Assert(!is_system_catalog);
	finish_heap_swap(old_table_oid, new_table_oid,
					 is_system_catalog,
					 false,		/* swap_toast_by_content */
					 false, true, false,
					 frozenXid, cutoffMulti,
					 relpersistence);
}

/*
 * Build indexes on NewHeap according to those on OldHeap.
 *
 * OldIndexes is the list of index OIDs on OldHeap. The contained indexes end
 * up locked using ShareUpdateExclusiveLock.
 *
 * A list of OIDs of the corresponding indexes created on NewHeap is
 * returned. The order of items does match, so we can use these arrays to swap
 * index storage.
 */
static List *
build_new_indexes(Relation NewHeap, Relation OldHeap, List *OldIndexes)
{
	List	   *result = NIL;

	pgstat_progress_update_param(PROGRESS_REPACK_PHASE,
								 PROGRESS_REPACK_PHASE_REBUILD_INDEX);

	foreach_oid(ind_oid, OldIndexes)
	{
		Oid			ind_oid_new;
		char	   *newName;
		Relation	ind;

		ind = index_open(ind_oid, ShareUpdateExclusiveLock);

		newName = ChooseRelationName(get_rel_name(ind_oid),
									 NULL,
									 "repacknew",
									 get_rel_namespace(ind->rd_index->indrelid),
									 false);
		ind_oid_new = index_create_copy(NewHeap, ind_oid,
										ind->rd_rel->reltablespace, newName,
										false);
		result = lappend_oid(result, ind_oid_new);

		index_close(ind, NoLock);
	}

	return result;
}

/*
 * Try to start a background worker to perform logical decoding of data
 * changes applied to relation while REPACK CONCURRENTLY is copying its
 * contents to a new table.
 */
static void
start_decoding_worker(Oid relid)
{
	Size		size;
	dsm_segment *seg;
	DecodingWorkerShared *shared;
	shm_mq	   *mq;
	shm_mq_handle *mqh;
	BackgroundWorker bgw;

	/* Setup shared memory. */
	size = BUFFERALIGN(offsetof(DecodingWorkerShared, error_queue)) +
		BUFFERALIGN(REPACK_ERROR_QUEUE_SIZE);
	seg = dsm_create(size, 0);
	shared = (DecodingWorkerShared *) dsm_segment_address(seg);
	shared->lsn_upto = InvalidXLogRecPtr;
	shared->done = false;
	SharedFileSetInit(&shared->sfs, seg);
	shared->last_exported = -1;
	SpinLockInit(&shared->mutex);
	shared->dbid = MyDatabaseId;

	/*
	 * This is the UserId set in cluster_rel(). Security context shouldn't be
	 * needed for decoding worker.
	 */
	shared->roleid = GetUserId();
	shared->relid = relid;
	ConditionVariableInit(&shared->cv);
	shared->backend_proc = MyProc;
	shared->backend_pid = MyProcPid;
	shared->backend_proc_number = MyProcNumber;

	mq = shm_mq_create((char *) BUFFERALIGN(shared->error_queue),
					   REPACK_ERROR_QUEUE_SIZE);
	shm_mq_set_receiver(mq, MyProc);
	mqh = shm_mq_attach(mq, seg, NULL);

	memset(&bgw, 0, sizeof(bgw));
	snprintf(bgw.bgw_name, BGW_MAXLEN,
			 "REPACK decoding worker for relation \"%s\"",
			 get_rel_name(relid));
	snprintf(bgw.bgw_type, BGW_MAXLEN, "REPACK decoding worker");
	bgw.bgw_flags = BGWORKER_SHMEM_ACCESS |
		BGWORKER_BACKEND_DATABASE_CONNECTION;
	bgw.bgw_start_time = BgWorkerStart_RecoveryFinished;
	bgw.bgw_restart_time = BGW_NEVER_RESTART;
	snprintf(bgw.bgw_library_name, MAXPGPATH, "postgres");
	snprintf(bgw.bgw_function_name, BGW_MAXLEN, "RepackWorkerMain");
	bgw.bgw_main_arg = UInt32GetDatum(dsm_segment_handle(seg));
	bgw.bgw_notify_pid = MyProcPid;

	decoding_worker = palloc0_object(DecodingWorker);
	if (!RegisterDynamicBackgroundWorker(&bgw, &decoding_worker->handle))
		ereport(ERROR,
				(errcode(ERRCODE_CONFIGURATION_LIMIT_EXCEEDED),
				 errmsg("out of background worker slots"),
				 errhint("You might need to increase \"%s\".", "max_worker_processes")));

	decoding_worker->seg = seg;
	decoding_worker->error_mqh = mqh;

	/*
	 * The decoding setup must be done before the caller can have XID assigned
	 * for any reason, otherwise the worker might end up in a deadlock,
	 * waiting for the caller's transaction to end. Therefore wait here until
	 * the worker indicates that it has the logical decoding initialized.
	 */
	ConditionVariablePrepareToSleep(&shared->cv);
	for (;;)
	{
		int			initialized;

		SpinLockAcquire(&shared->mutex);
		initialized = shared->initialized;
		SpinLockRelease(&shared->mutex);

		if (initialized)
			break;

		ConditionVariableSleep(&shared->cv, WAIT_EVENT_REPACK_WORKER_EXPORT);
	}
	ConditionVariableCancelSleep();
}

/*
 * Stop the decoding worker and cleanup the related resources.
 *
 * The worker stops on its own when it knows there is no more work to do, but
 * we need to stop it explicitly at least on ERROR in the launching backend.
 */
static void
stop_decoding_worker(void)
{
	BgwHandleStatus status;

	/* Haven't reached the worker startup? */
	if (decoding_worker == NULL)
		return;

	/* Could not register the worker? */
	if (decoding_worker->handle == NULL)
		return;

	TerminateBackgroundWorker(decoding_worker->handle);
	/* The worker should really exit before the REPACK command does. */
	HOLD_INTERRUPTS();
	status = WaitForBackgroundWorkerShutdown(decoding_worker->handle);
	RESUME_INTERRUPTS();

	if (status == BGWH_POSTMASTER_DIED)
		ereport(FATAL,
				(errcode(ERRCODE_ADMIN_SHUTDOWN),
				 errmsg("postmaster exited during REPACK command")));

	shm_mq_detach(decoding_worker->error_mqh);

	/*
	 * If we could not cancel the current sleep due to ERROR, do that before
	 * we detach from the shared memory the condition variable is located in.
	 * If we did not, the bgworker ERROR handling code would try and fail
	 * badly.
	 */
	ConditionVariableCancelSleep();

	dsm_detach(decoding_worker->seg);
	pfree(decoding_worker);
	decoding_worker = NULL;
}

/* Is this process a REPACK worker? */
static bool is_repack_worker = false;

static pid_t backend_pid;
static ProcNumber backend_proc_number;

/*
 * See ParallelWorkerShutdown for details.
 */
static void
RepackWorkerShutdown(int code, Datum arg)
{
	SendProcSignal(backend_pid,
				   PROCSIG_REPACK_MESSAGE,
				   backend_proc_number);

	dsm_detach((dsm_segment *) DatumGetPointer(arg));
}

/* REPACK decoding worker entry point */
void
RepackWorkerMain(Datum main_arg)
{
	dsm_segment *seg;
	DecodingWorkerShared *shared;
	shm_mq	   *mq;
	shm_mq_handle *mqh;

	is_repack_worker = true;

	/*
	 * Override the default bgworker_die() with die() so we can use
	 * CHECK_FOR_INTERRUPTS().
	 */
	pqsignal(SIGTERM, die);
	BackgroundWorkerUnblockSignals();

	seg = dsm_attach(DatumGetUInt32(main_arg));
	if (seg == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("could not map dynamic shared memory segment")));

	shared = (DecodingWorkerShared *) dsm_segment_address(seg);

	/* Arrange to signal the leader if we exit. */
	backend_pid = shared->backend_pid;
	backend_proc_number = shared->backend_proc_number;
	before_shmem_exit(RepackWorkerShutdown, PointerGetDatum(seg));

	/*
	 * Join locking group - see the comments around the call of
	 * start_decoding_worker().
	 */
	if (!BecomeLockGroupMember(shared->backend_proc, backend_pid))
		/* The leader is not running anymore. */
		return;

	/*
	 * Setup a queue to send error messages to the backend that launched this
	 * worker.
	 */
	mq = (shm_mq *) (char *) BUFFERALIGN(shared->error_queue);
	shm_mq_set_sender(mq, MyProc);
	mqh = shm_mq_attach(mq, seg, NULL);
	pq_redirect_to_shm_mq(seg, mqh);
	pq_set_parallel_leader(shared->backend_pid,
						   shared->backend_proc_number);

	/* Connect to the database. */
	BackgroundWorkerInitializeConnectionByOid(shared->dbid, shared->roleid, 0);

	repack_worker_internal(seg);
}

static void
repack_worker_internal(dsm_segment *seg)
{
	DecodingWorkerShared *shared;
	LogicalDecodingContext *decoding_ctx;
	SharedFileSet *sfs;
	Snapshot	snapshot;

	/*
	 * Transaction is needed to open relation, and it also provides us with a
	 * resource owner.
	 */
	StartTransactionCommand();

	shared = (DecodingWorkerShared *) dsm_segment_address(seg);

	/*
	 * Not sure the spinlock is needed here - the backend should not change
	 * anything in the shared memory until we have serialized the snapshot.
	 */
	SpinLockAcquire(&shared->mutex);
	Assert(XLogRecPtrIsInvalid(shared->lsn_upto));
	sfs = &shared->sfs;
	SpinLockRelease(&shared->mutex);

	SharedFileSetAttach(sfs, seg);

	/*
	 * Prepare to capture the concurrent data changes ourselves.
	 */
	decoding_ctx = setup_logical_decoding(shared->relid);

	/* Announce that we're ready. */
	SpinLockAcquire(&shared->mutex);
	shared->initialized = true;
	SpinLockRelease(&shared->mutex);
	ConditionVariableSignal(&shared->cv);

	/* Build the initial snapshot and export it. */
	snapshot = SnapBuildInitialSnapshotForRepack(decoding_ctx->snapshot_builder);
	export_initial_snapshot(snapshot, shared);

	/*
	 * Only historic snapshots should be used now. Do not let us restrict the
	 * progress of xmin horizon.
	 */
	InvalidateCatalogSnapshot();

	while (!decode_concurrent_changes(decoding_ctx, shared))
		;

	/* Cleanup. */
	cleanup_logical_decoding(decoding_ctx);
	CommitTransactionCommand();
}

/*
 * Make snapshot available to the backend that launched the decoding worker.
 */
static void
export_initial_snapshot(Snapshot snapshot, DecodingWorkerShared *shared)
{
	char		fname[MAXPGPATH];
	BufFile    *file;
	Size		snap_size;
	char	   *snap_space;

	snap_size = EstimateSnapshotSpace(snapshot);
	snap_space = (char *) palloc(snap_size);
	SerializeSnapshot(snapshot, snap_space);
	FreeSnapshot(snapshot);

	DecodingWorkerFileName(fname, shared->relid, shared->last_exported + 1);
	file = BufFileCreateFileSet(&shared->sfs.fs, fname);
	/* To make restoration easier, write the snapshot size first. */
	BufFileWrite(file, &snap_size, sizeof(snap_size));
	BufFileWrite(file, snap_space, snap_size);
	pfree(snap_space);
	BufFileClose(file);

	/* Increase the counter to tell the backend that the file is available. */
	SpinLockAcquire(&shared->mutex);
	shared->last_exported++;
	SpinLockRelease(&shared->mutex);
	ConditionVariableSignal(&shared->cv);
}

/*
 * Get the initial snapshot from the decoding worker.
 */
static Snapshot
get_initial_snapshot(DecodingWorker *worker)
{
	DecodingWorkerShared *shared;
	char		fname[MAXPGPATH];
	BufFile    *file;
	Size		snap_size;
	char	   *snap_space;
	Snapshot	snapshot;

	shared = (DecodingWorkerShared *) dsm_segment_address(worker->seg);

	/*
	 * The worker needs to initialize the logical decoding, which usually
	 * takes some time. Therefore it makes sense to prepare for the sleep
	 * first.
	 */
	ConditionVariablePrepareToSleep(&shared->cv);
	for (;;)
	{
		int		last_exported;

		SpinLockAcquire(&shared->mutex);
		last_exported = shared->last_exported;
		SpinLockRelease(&shared->mutex);

		/*
		 * Has the worker exported the file we are waiting for?
		 */
		if (last_exported == WORKER_FILE_SNAPSHOT)
			break;

		ConditionVariableSleep(&shared->cv, WAIT_EVENT_REPACK_WORKER_EXPORT);
	}
	ConditionVariableCancelSleep();

	/* Read the snapshot from a file. */
	DecodingWorkerFileName(fname, shared->relid, WORKER_FILE_SNAPSHOT);
	file = BufFileOpenFileSet(&shared->sfs.fs, fname, O_RDONLY, false);
	BufFileReadExact(file, &snap_size, sizeof(snap_size));
	snap_space = (char *) palloc(snap_size);
	BufFileReadExact(file, snap_space, snap_size);
	BufFileClose(file);

	/* Restore it. */
	snapshot = RestoreSnapshot(snap_space);
	pfree(snap_space);

	return snapshot;
}

bool
IsRepackWorker(void)
{
	return is_repack_worker;
}

/*
 * Handle receipt of an interrupt indicating a repack worker message.
 *
 * Note: this is called within a signal handler!  All we can do is set
 * a flag that will cause the next CHECK_FOR_INTERRUPTS() to invoke
 * ProcessRepackMessages().
 */
void
HandleRepackMessageInterrupt(void)
{
	InterruptPending = true;
	RepackMessagePending = true;
	SetLatch(MyLatch);
}

/*
 * Process any queued protocol messages received from parallel workers.
 */
void
ProcessRepackMessages(void)
{
	MemoryContext oldcontext;

	static MemoryContext hpm_context = NULL;

	/*
	 * Nothing to do if we haven't launched the worker yet or have already
	 * terminated it.
	 */
	if (decoding_worker == NULL)
		return;

	/*
	 * This is invoked from ProcessInterrupts(), and since some of the
	 * functions it calls contain CHECK_FOR_INTERRUPTS(), there is a potential
	 * for recursive calls if more signals are received while this runs.  It's
	 * unclear that recursive entry would be safe, and it doesn't seem useful
	 * even if it is safe, so let's block interrupts until done.
	 */
	HOLD_INTERRUPTS();

	/*
	 * Moreover, CurrentMemoryContext might be pointing almost anywhere.  We
	 * don't want to risk leaking data into long-lived contexts, so let's do
	 * our work here in a private context that we can reset on each use.
	 */
	if (hpm_context == NULL)	/* first time through? */
		hpm_context = AllocSetContextCreate(TopMemoryContext,
											"ProcessRepackMessages",
											ALLOCSET_DEFAULT_SIZES);
	else
		MemoryContextReset(hpm_context);

	oldcontext = MemoryContextSwitchTo(hpm_context);

	/* OK to process messages.  Reset the flag saying there are more to do. */
	RepackMessagePending = false;

	/*
	 * Read as many messages as we can from each worker, but stop when no more
	 * messages can be read from the worker without blocking.
	 */
	while (true)
	{
		shm_mq_result res;
		Size		nbytes;
		void	   *data;

		res = shm_mq_receive(decoding_worker->error_mqh, &nbytes,
							 &data, true);
		if (res == SHM_MQ_WOULD_BLOCK)
			break;
		else if (res == SHM_MQ_SUCCESS)
		{
			StringInfoData msg;

			initStringInfo(&msg);
			appendBinaryStringInfo(&msg, data, nbytes);
			ProcessRepackMessage(&msg);
			pfree(msg.data);
		}
		else
		{
			/*
			 * The decoding worker is special in that it exits as soon as it
			 * has its work done. Thus the DETACHED result code is fine.
			 */
			Assert(res == SHM_MQ_DETACHED);

			break;
		}
	}

	MemoryContextSwitchTo(oldcontext);

	/* Might as well clear the context on our way out */
	MemoryContextReset(hpm_context);

	RESUME_INTERRUPTS();
}

/*
 * Process a single protocol message received from a single parallel worker.
 */
static void
ProcessRepackMessage(StringInfo msg)
{
	char		msgtype;

	msgtype = pq_getmsgbyte(msg);

	switch (msgtype)
	{
		case PqMsg_ErrorResponse:
		case PqMsg_NoticeResponse:
			{
				ErrorData	edata;

				/* Parse ErrorResponse or NoticeResponse. */
				pq_parse_errornotice(msg, &edata);

				/* Death of a worker isn't enough justification for suicide. */
				edata.elevel = Min(edata.elevel, ERROR);

				/*
				 * If desired, add a context line to show that this is a
				 * message propagated from a parallel worker.  Otherwise, it
				 * can sometimes be confusing to understand what actually
				 * happened.
				 */
				if (edata.context)
					edata.context = psprintf("%s\n%s", edata.context,
											 _("decoding worker"));
				else
					edata.context = pstrdup(_("decoding worker"));

				/* Rethrow error or print notice. */
				ThrowErrorData(&edata);

				break;
			}

		default:
			{
				elog(ERROR, "unrecognized message type received from decoding worker: %c (message length %d bytes)",
					 msgtype, msg->len);
			}
	}
}
