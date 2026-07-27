
# Copyright (c) 2026, PostgreSQL Global Development Group

# A hot standby replaying the rebuilds while it serves the checks, then
# taking over.
use strict;
use warnings FATAL => 'all';

use FindBin;
use lib $FindBin::RealBin;
use Stress::Run;

run_scenario(
	'standby',
	{
		schema => [ 'pgbench', 'quiet_index' ],
		indexes => ['btree_abalance'],
		load => ['tpcb_like'],
		ddl => [@STANDARD_DDL],
		ddl_concurrency => 1,
		# index_vs_seq forces the planner to build an index plan on the
		# standby, which is where a stale index list turns into an error
		# rather than a slower plan.
		checks => [ 'balances', 'index_vs_seq', 'quiet_index_scan', 'amcheck' ],
		env => 'standby',
		clients => 20,
		tags => ['ci'],
	});
