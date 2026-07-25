# Copyright (c) 2026, PostgreSQL Global Development Group

# Stub for the plancache_and_matview scenario; see t/Stress/Scenarios.pm.
use strict;
use warnings FATAL => 'all';

use FindBin;
use lib $FindBin::RealBin;
use Stress::Run;

run_scenario('plancache_and_matview');
