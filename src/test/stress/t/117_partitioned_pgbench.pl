
# Copyright (c) 2026, PostgreSQL Global Development Group

# The pgbench schema itself partitioned, two levels deep, while the
# TPC-B workload runs through the parent.
#
# 106 detaches partitions of a table standing to the side of the
# workload; this one detaches a partition of the table the workload is
# on.  What makes that possible is the overflow partition: it holds only
# account numbers above the ones pgbench created, all with a zero
# balance, so taking it away moves none of the four sums.
#
# The prepared protocol is here for the same reason as in 106 -- cached
# plans are what the detach races are found through -- but generic plans
# are not forced.  A generic plan locks every partition, so the overflow
# partition is never free of lockers and DETACH CONCURRENTLY waits out
# the whole lock timeout.  Letting the planner prune leaves it mostly
# untouched, which is what the detach needs to make progress.
use strict;
use warnings FATAL => 'all';

use FindBin;
use lib $FindBin::RealBin;
use Stress::Run;

run_scenario(
	'partitioned_pgbench',
	{
		schema => [ 'pgbench', 'partitioned', 'partitioned_2_levels' ],
		load => [ 'tpcb_like', 'overflow_churn' ],
		ddl => [
			'repack_concurrently', 'reindex_table_concurrently',
			'reindex_index_concurrently', 'drop_create_index',
			'detach_overflow_partition', 'detach_subpartition'
		],
		ddl_concurrency => 1,
		checks => [ 'balances', 'amcheck' ],
		env => 'standalone',
		pgbench_args => '--protocol=prepared',
		clients => 20,
		tags => ['ci'],
	});
