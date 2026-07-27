
# Copyright (c) 2026, PostgreSQL Global Development Group

# Partitions attached and detached concurrently, one of them dropped
# outright after the detach, and a partitioned index built the
# documented way, while DML routes through the parent.
#
# The prepared protocol is what makes this scenario reach the partition
# descriptor and the pruning setup: those are rebuilt when a cached plan
# is revalidated, which is where the detach races have always been
# found.
use strict;
use warnings FATAL => 'all';

use FindBin;
use lib $FindBin::RealBin;
use Stress::Run;

run_scenario(
	'partitions',
	{
		schema => [ 'pgbench', 'partitioned_side' ],
		load => [ 'tpcb_like', 'partition_dml', 'partition_upsert' ],
		ddl => [
			'repack_concurrently', 'reindex_table_concurrently',
			'detach_partition_concurrently', 'detach_drop_recreate_partition',
			'partitionwise_index_build'
		],
		ddl_concurrency => 1,
		checks => [ 'partition_sum', 'balances' ],
		env => 'standalone',
		conf => ['plan_cache_mode = force_generic_plan'],
		pgbench_args => '--protocol=prepared',
		# DETACH ... CONCURRENTLY waits for every transaction holding the
		# parent, and forcing generic plans means each of them locks all
		# four partitions.  With enough clients it never gets a gap and
		# sits out the whole lock timeout, which fails the run after
		# three minutes without telling anyone anything.  Fewer clients
		# leave it room.
		clients => 10,
		tags => ['ci'],
	});
