
# Copyright (c) 2026, PostgreSQL Global Development Group

# The same commands with a lock table small enough to run out, which
# they are heavy users of.
use strict;
use warnings FATAL => 'all';

use FindBin;
use lib $FindBin::RealBin;
use Stress::Run;

run_scenario(
	'lock_exhaustion',
	{
		schema => [ 'pgbench', 'ledger' ],
		indexes => [
			'btree_abalance', 'partial_abalance',
			'expr_abalance', 'covering_aid'
		],
		load => [ 'tpcb_like', 'balanced_pair' ],
		ddl => [@STANDARD_DDL],
		ddl_concurrency => 1,
		checks => [ 'ledger_sum', 'amcheck' ],
		env => 'lock_exhaustion',
		clients => 20,
		tags => ['ci'],
	});
