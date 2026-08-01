
# Copyright (c) 2026, PostgreSQL Global Development Group

# The data checksum scenario (Stress::Feature::DataChecksums) under the
# crash loop.  A crash mid-transition is documented to revert the
# cluster to checksums off, and recovery replays the worker's full page
# images; what must hold afterwards is the same as always -- with
# checksums back on, every page verifies.
use strict;
use warnings FATAL => 'all';

use FindBin;
use lib $FindBin::RealBin;
use Stress::Run;

run_template('data_checksums_crash', 'data_checksums',
	env => 'crash_loop');
