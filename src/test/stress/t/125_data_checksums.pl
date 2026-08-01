
# Copyright (c) 2026, PostgreSQL Global Development Group

# The online data checksum transitions against the rotation's rewrites,
# standalone.  The scenario lives in Stress::Feature::DataChecksums;
# this file is the variant that runs it as written.
use strict;
use warnings FATAL => 'all';

use FindBin;
use lib $FindBin::RealBin;
use Stress::Run;

run_template('data_checksums', 'data_checksums');
