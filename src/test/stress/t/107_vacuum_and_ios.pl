
# Copyright (c) 2026, PostgreSQL Global Development Group

# VACUUM alongside the rebuilds, with index-only scans reading through
# the visibility map it maintains.
use strict;
use warnings FATAL => 'all';

use FindBin;
use lib $FindBin::RealBin;
use Stress::Run;

run_scenario(
	'vacuum_and_ios',
	{
		indexes => [ 'covering_aid', 'btree_abalance' ],
		load => ['tpcb_like'],
		ddl => [ @STANDARD_DDL, 'vacuum' ],
		env => 'autovacuum',
		clients => 20,
		tags => ['ci'],
	});
