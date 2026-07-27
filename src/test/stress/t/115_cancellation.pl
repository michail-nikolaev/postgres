
# Copyright (c) 2026, PostgreSQL Global Development Group

# The commands cancelled partway, so that their own cleanup paths run
# rather than recovery's.
use strict;
use warnings FATAL => 'all';

use FindBin;
use lib $FindBin::RealBin;
use Stress::Run;

run_scenario(
	'cancellation',
	{
		schema => [ 'pgbench', 'ledger' ],
		indexes => ['btree_abalance'],
		load => [ 'tpcb_like', 'balanced_pair' ],
		ddl => [@STANDARD_DDL],
		ddl_concurrency => 1,
		checks => [ 'balances', 'ledger_sum', 'amcheck', 'no_slot_leak',
			'invalid_indexes_droppable' ],
		env => 'cancellation',
		clients => 20,
		tags => ['ci'],
	});
