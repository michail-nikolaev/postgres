# Copyright (c) 2026, PostgreSQL Global Development Group

# Stub for the bulk_and_rewrite scenario; see t/Stress/Scenarios.pm.
use strict;
use warnings FATAL => 'all';

use FindBin;
use lib $FindBin::RealBin;
use Stress::Run;

run_scenario('bulk_and_rewrite');
