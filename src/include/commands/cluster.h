/*-------------------------------------------------------------------------
 *
 * cluster.h
 *	  header file for postgres cluster command stuff
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994-5, Regents of the University of California
 *
 * src/include/commands/cluster.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef CLUSTER_H
#define CLUSTER_H

#include "nodes/execnodes.h"
#include "nodes/parsenodes.h"
#include "parser/parse_node.h"
#include "replication/decode.h"
#include "postmaster/bgworker.h"
#include "replication/logical.h"
#include "storage/buffile.h"
#include "storage/lock.h"
#include "storage/shm_mq.h"
#include "utils/relcache.h"
#include "utils/resowner.h"


/* flag bits for ClusterParams->options */
#define CLUOPT_VERBOSE 0x01		/* print progress info */
#define CLUOPT_RECHECK 0x02		/* recheck relation state */
#define CLUOPT_RECHECK_ISCLUSTERED 0x04 /* recheck relation state for
										 * indisclustered */
#define CLUOPT_ANALYZE 0x08		/* do an ANALYZE */
#define CLUOPT_CONCURRENT 0x10	/* allow concurrent data changes */


/* options for CLUSTER */
typedef struct ClusterParams
{
	bits32		options;		/* bitmask of CLUOPT_* */
} ClusterParams;


/*
 * The following definitions are used by REPACK CONCURRENTLY.
 */

/*
 * Stored as a single byte in the output file.
 */
typedef enum
{
	CHANGE_INSERT,
	CHANGE_UPDATE_OLD,
	CHANGE_UPDATE_NEW,
	CHANGE_DELETE
} ConcurrentChangeKind;

/*
 * Logical decoding state.
 *
 * The output plugin uses it to store the data changes that it decodes from
 * WAL while the table contents is being copied to a new storage.
 */
typedef struct RepackDecodingState
{
	/* The relation whose changes we're decoding. */
	Oid			relid;

	/* Tuple descriptor of the relation being processed. */
	TupleDesc	tupdesc;

	/* The current output file. */
	BufFile    *file;
} RepackDecodingState;

extern PGDLLIMPORT volatile sig_atomic_t RepackMessagePending;

extern bool IsRepackWorker(void);
extern void HandleRepackMessageInterrupt(void);
extern void ProcessRepackMessages(void);

extern void ExecRepack(ParseState *pstate, RepackStmt *stmt, bool isTopLevel);

extern void cluster_rel(RepackCommand command, Relation OldHeap, Oid indexOid,
						ClusterParams *params, bool isTopLevel);
extern void check_index_is_clusterable(Relation OldHeap, Oid indexOid,
									   LOCKMODE lockmode);
extern void mark_index_clustered(Relation rel, Oid indexOid, bool is_internal);

extern Oid	make_new_heap(Oid OIDOldHeap, Oid NewTableSpace, Oid NewAccessMethod,
						  char relpersistence, LOCKMODE lockmode);
extern void finish_heap_swap(Oid OIDOldHeap, Oid OIDNewHeap,
							 bool is_system_catalog,
							 bool swap_toast_by_content,
							 bool check_constraints,
							 bool is_internal,
							 bool reindex,
							 TransactionId frozenXid,
							 MultiXactId cutoffMulti,
							 char newrelpersistence);

extern bool am_decoding_for_repack(void);
extern bool change_useless_for_repack(XLogRecordBuffer *buf);

extern void RepackWorkerMain(Datum main_arg);
#endif							/* CLUSTER_H */
