
# Copyright (c) 2026, PostgreSQL Global Development Group

# Row locks held across the DDL, savepoints and subxid-cache overflow,
# and cursors held open across it.
use strict;
use warnings FATAL => 'all';

use FindBin;
use lib $FindBin::RealBin;
use Stress::Run;

run_scenario(
	'locks_and_subxacts',
	{
		schema => [ 'pgbench', 'ledger', 'replica_role' ],
		load => [ 'tpcb_like', 'row_lock', 'subxact_churn', 'cursor_hold',
			'replica_role_apply' ],
		ddl => [@STANDARD_DDL],
		ddl_concurrency => 1,
		checks =>
		  [ 'balances', 'ledger_sum', 'row_lock_durability', 'amcheck' ],
		env => 'standalone',
		clients => 30,
		tags => ['ci'],
	});
