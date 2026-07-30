
# Copyright (c) 2026, PostgreSQL Global Development Group

# Killed and restarted while the commands are in flight, so their
# cleanup happens through crash recovery.
use strict;
use warnings FATAL => 'all';

use FindBin;
use lib $FindBin::RealBin;
use Stress::Run;

run_scenario(
	'crash_recovery',
	{
		schema => [ 'pgbench', 'ledger' ],
		indexes => ['btree_abalance'],
		load => [ 'tpcb_like', 'balanced_pair' ],
		ddl => [@STANDARD_DDL],
		ddl_concurrency => 1,
		checks => [ 'balances', 'ledger_sum', 'amcheck', 'no_slot_leak' ],
		env => 'crash_loop',
		clients => 20,
		tags => ['ci'],
	});
