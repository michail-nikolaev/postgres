/*-------------------------------------------------------------------------
 *
 * pgoutput_repack.c
 *		Logical Replication output plugin for REPACK command
 *
 * Copyright (c) 2012-2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		  src/backend/replication/pgoutput_repack/pgoutput_repack.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/detoast.h"
#include "commands/cluster.h"
#include "replication/snapbuild.h"
#include "utils/memutils.h"

PG_MODULE_MAGIC;

static void plugin_startup(LogicalDecodingContext *ctx,
						   OutputPluginOptions *opt, bool is_init);
static void plugin_shutdown(LogicalDecodingContext *ctx);
static void plugin_begin_txn(LogicalDecodingContext *ctx,
							 ReorderBufferTXN *txn);
static void plugin_commit_txn(LogicalDecodingContext *ctx,
							  ReorderBufferTXN *txn, XLogRecPtr commit_lsn);
static void plugin_change(LogicalDecodingContext *ctx, ReorderBufferTXN *txn,
						  Relation rel, ReorderBufferChange *change);
static void store_change(LogicalDecodingContext *ctx, Relation relation,
						 ConcurrentChangeKind kind, HeapTuple tuple);

void
_PG_output_plugin_init(OutputPluginCallbacks *cb)
{
	cb->startup_cb = plugin_startup;
	cb->begin_cb = plugin_begin_txn;
	cb->change_cb = plugin_change;
	cb->commit_cb = plugin_commit_txn;
	cb->shutdown_cb = plugin_shutdown;
}


/* initialize this plugin */
static void
plugin_startup(LogicalDecodingContext *ctx, OutputPluginOptions *opt,
			   bool is_init)
{
	ctx->output_plugin_private = NULL;

	/* Probably unnecessary, as we don't use the SQL interface ... */
	opt->output_type = OUTPUT_PLUGIN_BINARY_OUTPUT;

	if (ctx->output_plugin_options != NIL)
	{
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("This plugin does not expect any options")));
	}
}

static void
plugin_shutdown(LogicalDecodingContext *ctx)
{
}

/*
 * As we don't release the slot during processing of particular table, there's
 * no room for SQL interface, even for debugging purposes. Therefore we need
 * neither OutputPluginPrepareWrite() nor OutputPluginWrite() in the plugin
 * callbacks. (Although we might want to write custom callbacks, this API
 * seems to be unnecessarily generic for our purposes.)
 */

/* BEGIN callback */
static void
plugin_begin_txn(LogicalDecodingContext *ctx, ReorderBufferTXN *txn)
{
}

/* COMMIT callback */
static void
plugin_commit_txn(LogicalDecodingContext *ctx, ReorderBufferTXN *txn,
				  XLogRecPtr commit_lsn)
{
}

/*
 * Callback for individual changed tuples
 */
static void
plugin_change(LogicalDecodingContext *ctx, ReorderBufferTXN *txn,
			  Relation relation, ReorderBufferChange *change)
{
	RepackDecodingState *dstate;

	dstate = (RepackDecodingState *) ctx->output_writer_private;

	/* Changes of other relation should not have been decoded. */
	Assert(RelationGetRelid(relation) == dstate->relid);

	/* Decode entry depending on its type */
	switch (change->action)
	{
		case REORDER_BUFFER_CHANGE_INSERT:
			{
				HeapTuple	newtuple;

				newtuple = change->data.tp.newtuple;

				/*
				 * Identity checks in the main function should have made this
				 * impossible.
				 */
				if (newtuple == NULL)
					elog(ERROR, "incomplete insert info.");

				store_change(ctx, relation, CHANGE_INSERT, newtuple);
			}
			break;
		case REORDER_BUFFER_CHANGE_UPDATE:
			{
				HeapTuple	oldtuple,
							newtuple;

				oldtuple = change->data.tp.oldtuple;
				newtuple = change->data.tp.newtuple;

				if (newtuple == NULL)
					elog(ERROR, "incomplete update info.");

				if (oldtuple != NULL)
					store_change(ctx, relation, CHANGE_UPDATE_OLD, oldtuple);

				store_change(ctx, relation, CHANGE_UPDATE_NEW, newtuple);
			}
			break;
		case REORDER_BUFFER_CHANGE_DELETE:
			{
				HeapTuple	oldtuple;

				oldtuple = change->data.tp.oldtuple;

				if (oldtuple == NULL)
					elog(ERROR, "incomplete delete info.");

				store_change(ctx, relation, CHANGE_DELETE, oldtuple);
			}
			break;
		default:

			/*
			 * Should not come here. This includes TRUNCATE of the table being
			 * processed. heap_decode() cannot check the file locator easily,
			 * but we assume that TRUNCATE uses AccessExclusiveLock on the
			 * table so it should not occur during REPACK (CONCURRENTLY).
			 */
			Assert(false);
			break;
	}
}

/* Store concurrent data change. */
static void
store_change(LogicalDecodingContext *ctx, Relation relation,
			 ConcurrentChangeKind kind, HeapTuple tuple)
{
	RepackDecodingState *dstate;
	MemoryContext	oldcxt;
	BufFile    *file;
	char		kind_byte = (char) kind;
	List	   *attrs_ext = NIL;
	uint32		natt_ext;

	dstate = (RepackDecodingState *) ctx->output_writer_private;
	file = dstate->file;

	/* Store the change kind. */
	BufFileWrite(file, &kind_byte, 1);

	/* Make sure there are no memory leaks. */
	oldcxt = MemoryContextSwitchTo(dstate->change_cxt);

	/*
	 * If the tuple contains "external indirect" attributes, we need to write
	 * the contents to the file because we have no control over that memory.
	 */
	if (HeapTupleHasExternal(tuple))
	{
		TupleDesc	desc;
		Datum	   *attrs;
		bool	   *isnull;

		desc = RelationGetDescr(relation);
		attrs = palloc0_array(Datum, desc->natts);
		isnull = palloc0_array(bool, desc->natts);

		heap_deform_tuple(tuple, desc, attrs, isnull);

		/* First, gather and count the "external indirect" attributes. */
		for (int i = 0; i < desc->natts; i++)
		{
			CompactAttribute *attr = TupleDescCompactAttr(desc, i);
			varlena    *varlena_pointer;

			if (attr->attisdropped)
				continue;

			/* not a varlena datatype */
			if (attr->attlen != -1)
				continue;

			/* no data */
			if (isnull[i])
				continue;

			/* ok, we know we have a toast datum */
			varlena_pointer = (varlena *) DatumGetPointer(attrs[i]);

			if (!VARATT_IS_EXTERNAL(varlena_pointer))
				continue;

			if (VARATT_IS_EXTERNAL_INDIRECT(varlena_pointer))
				attrs_ext = lappend(attrs_ext, varlena_pointer);
			else
			{
				/*
				 * Logical decoding should not produce "external expanded"
				 * attributes (those actually should never appear on disk), so
				 * only TOASTed attribute can be seen here.
				 */
				Assert(VARATT_IS_EXTERNAL_ONDISK(varlena_pointer));
			}
		}
		natt_ext = list_length(attrs_ext);
	}
	else
		natt_ext = 0;

	/* Write the number of external attributes. */
	BufFileWrite(file, &natt_ext, sizeof(natt_ext));
	/* ... and the attributes themselves, if there are some. */
	foreach_ptr(varlena, attr_val, attrs_ext)
	{
		varlena    *ext_val;
		Size		ext_val_size;

		ext_val = detoast_external_attr(attr_val);
		ext_val_size = VARSIZE_ANY(ext_val);
		BufFileWrite(file, ext_val, ext_val_size);
	}

	/* Finally write the tuple size ... */
	BufFileWrite(file, &tuple->t_len, sizeof(tuple->t_len));
	/* ... and the tuple itself. */
	BufFileWrite(file, tuple->t_data, tuple->t_len);

	/* Cleanup. */
	MemoryContextSwitchTo(oldcxt);
	MemoryContextReset(dstate->change_cxt);
}
