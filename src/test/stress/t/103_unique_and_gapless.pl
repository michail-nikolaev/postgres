
# Copyright (c) 2026, PostgreSQL Global Development Group

# Unique keys under upserts and MERGE, and inserts whose commit order is
# pinned by an advisory lock.
use strict;
use warnings FATAL => 'all';

use FindBin;
use lib $FindBin::RealBin;
use Stress::Run;

run_scenario(
	'unique_and_gapless',
	{
		schema => [ 'pgbench', 'upsert_keys', 'gapless' ],
		load => [ 'tpcb_like', 'upsert_merge', 'serial_insert' ],
		ddl => [@STANDARD_DDL],
		ddl_concurrency => 1,
		checks => [ 'distinct_keys', 'gapless_count', 'amcheck' ],
		env => 'standalone',
		clients => 30,
		tags => ['ci'],
	});
