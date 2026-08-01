
# Copyright (c) 2026, PostgreSQL Global Development Group

# The same as 100, but with indexes large enough to be multi-level, so a
# concurrent build has real work to do and page splits are routine.
use strict;
use warnings FATAL => 'all';

use FindBin;
use lib $FindBin::RealBin;
use Stress::Run;

run_scenario(
	'repack_dml_s50',
	{
		pgbench_scale => 50,
		indexes => [ 'btree_abalance', 'partial_abalance', 'expr_abalance' ],
		load => ['tpcb_like'],
		ddl => [@STANDARD_DDL],
		clients => 30,
		tags => ['ci'],
	});
