
# Copyright (c) 2026, PostgreSQL Global Development Group

# Schema shapes and DDL forms the rest of the suite does not have: hash
# partitioning, an arbiter index that treats nulls as equal, a
# constraint added unvalidated and then validated, and one REINDEX
# covering the whole schema.
#
# Gates 3231fd04552, "Stop creating constraints during DETACH
# CONCURRENTLY".  Reverted, this fails with
#
#   not ok - no partition constraint left behind by DETACH
#
# because the detached partition keeps a CHECK on
# satisfies_hash_partition() carrying the OID of the parent it is no
# longer related to.  That is invisible under range partitioning, which
# is why the partitions scenario cannot catch it however long it runs:
# the equivalent range constraint is harmless.  A run does not have to
# race anything for this one -- a single completed detach is enough --
# so it fails at the default duration.
#
# It gates four more, all of them behavioural assertions rather than
# races, and all of them previously missed because nothing here had the
# shape they need:
#
#   832e220d99a  REPACK CONCURRENTLY: Don't use deferrable primary keys
#                -- pgb_defer has one and no replica identity, and
#                repack_refuses_deferrable requires REPACK to decline it.
#                3 failures in 3 reverted.
#   c426f7c2b36  Fix assertion failure with REINDEX and event triggers
#                -- pgb_bare has no indexes and an event trigger reads a
#                catalog after the DDL.  5 in 6, as an assertion failure
#                on portal->portalSnapshot.
#   13503eb5905  Diagnose !indisvalid in more SQL functions
#                -- a unique build over duplicate values leaves an
#                invalid index and pgstatindex must refuse it.  3 in 3.
#   9511fb37ac7  Reset indisreplident for an invalid index in DROP INDEX
#                CONCURRENTLY -- a concurrent drop is made to fail while
#                the index is the replica identity, and no index may be
#                left both invalid and marked.  3 in 3.
#   b96115acb8a  Fix assertion if index is dropped during REFRESH
#                CONCURRENTLY -- a matview whose own definition drops its
#                unique index, which is the only way in.  3 in 3.
use strict;
use warnings FATAL => 'all';

use FindBin;
use lib $FindBin::RealBin;
use Stress::Run;

run_scenario(
	'hash_and_constraints',
	{
		schema => [ 'pgbench', 'partitioned_hash', 'nulls_not_distinct',
			'deferrable_pk', 'bare_table_event_trigger' ],
		pgbench_scale => 1,
		load => [ 'tpcb_like', 'hash_dml', 'nnd_upsert',
			'deferred_key_swap' ],
		ddl => [
			'repack_concurrently', 'reindex_table_concurrently',
			'reindex_pkey_concurrently', 'detach_hash_partition',
			'add_validate_constraint', 'reindex_schema_concurrently',
			'reindex_bare_table'
		],
		ddl_concurrency => 1,
		checks => [ 'balances', 'amcheck', 'no_substitute_constraints',
			'deferred_keys_intact', 'repack_refuses_deferrable',
			'pgstat_rejects_invalid_index', 'dic_clears_replident',
			'refresh_survives_dropped_index' ],
		env => 'standalone',
		clients => 20,
		tags => ['ci'],
	});
