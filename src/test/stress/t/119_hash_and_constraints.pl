
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
use strict;
use warnings FATAL => 'all';

use FindBin;
use lib $FindBin::RealBin;
use Stress::Run;

run_scenario(
	'hash_and_constraints',
	{
		schema => [ 'pgbench', 'partitioned_hash', 'nulls_not_distinct',
			'deferrable_pk' ],
		pgbench_scale => 1,
		load => [ 'tpcb_like', 'hash_dml', 'nnd_upsert',
			'deferred_key_swap' ],
		ddl => [
			'repack_concurrently', 'reindex_table_concurrently',
			'reindex_pkey_concurrently', 'detach_hash_partition',
			'add_validate_constraint', 'reindex_schema_concurrently'
		],
		ddl_concurrency => 1,
		checks => [ 'balances', 'amcheck', 'no_substitute_constraints',
			'deferred_keys_intact', 'repack_refuses_deferrable' ],
		env => 'standalone',
		clients => 20,
		tags => ['ci'],
	});
