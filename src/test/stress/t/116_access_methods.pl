
# Copyright (c) 2026, PostgreSQL Global Development Group

# Every access method's index built, rebuilt and dropped concurrently
# while the columns they cover are rewritten.
#
# VACUUM belongs in the rotation here rather than only in the scenario
# named for it: some of what a rebuild leaves behind is only wrong once
# VACUUM reaches it, which is how an SP-GiST redirect written without a
# transaction id used to show up.
use strict;
use warnings FATAL => 'all';

use FindBin;
use lib $FindBin::RealBin;
use Stress::Run;

run_scenario(
	'access_methods',
	{
		schema => [ 'pgbench', 'am_columns' ],
		load => [ 'tpcb_like', 'am_churn', 'temp_table_cic' ],
		# REPACK orders a table by an index, which only btree can do, so
		# the index-ordered variant is left out here.
		ddl => [
			'repack_concurrently', 'reindex_table_concurrently',
			'reindex_index_concurrently', 'drop_create_index',
			'vacuum'
		],
		ddl_concurrency => 1,
		checks => [ 'balances', 'amcheck' ],
		env => 'standalone',
		clients => 20,
		tags => ['ci'],
	});
