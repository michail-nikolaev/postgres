
# Copyright (c) 2026, PostgreSQL Global Development Group

# Several CONCURRENTLY commands in flight at once on different tables,
# each taking a transient slot of its own and switching logical decoding
# on and off.  wal_level = replica, so those switches are real.
use strict;
use warnings FATAL => 'all';

use FindBin;
use lib $FindBin::RealBin;
use Stress::Run;

run_scenario(
	'overlapping_ddl',
	{
		schema => [ 'pgbench', 'ledger' ],
		load => [ 'tpcb_like', 'balanced_pair' ],
		ddl => [@STANDARD_DDL],
		ddl_concurrency => 4,
		checks => [ 'balances', 'ledger_sum', 'amcheck', 'no_slot_leak',
			'decoding_disabled' ],
		env => 'wal_replica',
		clients => 30,
		tags => ['ci'],
	});
