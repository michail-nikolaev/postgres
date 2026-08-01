
# Copyright (c) 2026, PostgreSQL Global Development Group

# Cached plans held across the DDL that must invalidate them: the
# prepared protocol, generic plans and a PL/pgSQL function's own plan
# cache at once.  The materialized view refresh runs here too, since it
# is another thing whose plan and contents must keep up.
use strict;
use warnings FATAL => 'all';

use FindBin;
use lib $FindBin::RealBin;
use Stress::Run;

run_scenario(
	'plancache_and_matview',
	{
		load => [ 'tpcb_like', 'plancache' ],
		ddl => [ @STANDARD_DDL, 'refresh_matview_concurrently' ],
		conf => ['plan_cache_mode = force_generic_plan'],
		pgbench_args => '--protocol=prepared',
		clients => 20,
		tags => ['ci'],
	});
