
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
		# Ten, not twenty.  At this scale there is one pgbench_branches
		# row and every transaction updates it, so the clients queue on
		# that row; when a DDL command parks an AccessExclusiveLock
		# request the queue behind it grows until everyone hits the lock
		# timeout together and the run dies three minutes later having
		# tested nothing.  Halving the queue is what keeps that cascade
		# from forming -- see also the bounded ATTACH in
		# pgb_attach_bounded(), which fixes the other half.
		clients => 10,
		tags => ['ci'],
	});
