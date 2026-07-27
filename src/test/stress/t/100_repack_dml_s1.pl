
# Copyright (c) 2026, PostgreSQL Global Development Group

# The smallest scale there is: one branch row, so every transaction in
# the mix collides on it, and the DDL runs against tables that are being
# modified as fast as the lock manager allows.
use strict;
use warnings FATAL => 'all';

use FindBin;
use lib $FindBin::RealBin;
use Stress::Run;

run_scenario(
	'repack_dml_s1',
	{
		schema => ['pgbench'],
		pgbench_scale => 1,
		# expr_xid belongs at this scale rather than 101's: the expression
		# takes a transaction id every time it is evaluated, which over
		# five million rows costs more than the coverage is worth.
		indexes => [ 'btree_abalance', 'btree_history_delta',
			'toasted_predicate', 'expr_xid' ],
		load => [ 'tpcb_like', 'row_lock' ],
		ddl => [@STANDARD_DDL],
		ddl_concurrency => 1,
		checks => [ 'balances', 'index_vs_seq', 'amcheck', 'no_slot_leak' ],
		env => 'standalone',
		# expr_xid's expression assigns a transaction id every time it
		# is evaluated, and that is not allowed in parallel mode: a
		# parallel index build, or a parallel scan under amcheck's
		# heapallindexed pass, fails with "cannot assign transaction IDs
		# during a parallel operation".  That is the server behaving as
		# documented rather than a bug, so this scenario does not use
		# parallel workers.  Parallel builds are covered by 101, which
		# has the larger table and does not carry this index.
		conf => [
			'max_parallel_maintenance_workers = 0',
			'max_parallel_workers_per_gather = 0',
			# Force what parallelism there could be, so that a plan
			# choice on one platform and not another cannot decide
			# whether this scenario passes -- which is how the
			# transaction id problem above reached CI on macOS and not on
			# Linux.
			'min_parallel_table_scan_size = 0',
			'parallel_setup_cost = 0',
		],
		clients => 30,
		tags => ['ci'],
	});
