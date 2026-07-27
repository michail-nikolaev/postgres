
# Copyright (c) 2026, PostgreSQL Global Development Group

# Out-of-line values, stored generated columns and an exclusion
# constraint: three things REPACK has to reproduce exactly when it
# re-applies what it decoded.
use strict;
use warnings FATAL => 'all';

use FindBin;
use lib $FindBin::RealBin;
use Stress::Run;

run_scenario(
	'rewrite_fidelity',
	{
		schema => [ 'pgbench', 'toast', 'generated', 'exclusion_slot' ],
		load => [
			'tpcb_like', 'toast_rewrite',
			'generated_update', 'exclusion_churn'
		],
		# REINDEX will not rebuild an exclusion constraint's index
		# concurrently, so that one is left out of the rotation here; the
		# blocking rebuild covers it instead.
		ddl => [
			'repack_concurrently', 'reindex_table_concurrently',
			'drop_create_index'
		],
		ddl_concurrency => 1,
		checks => [
			'toast_md5', 'generated_matches',
			'generated_defs_intact', 'distinct_slots',
			'amcheck'
		],
		env => 'standalone',
		clients => 20,
		tags => ['ci'],
	});
