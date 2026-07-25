# Copyright (c) 2026, PostgreSQL Global Development Group

# Stub for the rewrite_fidelity scenario; see t/Stress/Scenarios.pm.
use strict;
use warnings FATAL => 'all';

use FindBin;
use lib $FindBin::RealBin;
use Stress::Run;

run_scenario('rewrite_fidelity');
