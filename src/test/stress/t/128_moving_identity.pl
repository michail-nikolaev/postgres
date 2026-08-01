
# Copyright (c) 2026, PostgreSQL Global Development Group

# The replica identity moved between two candidate indexes while REPACK
# (CONCURRENTLY) runs against the table, standalone.  The scenario lives
# in Stress::Feature::MovingIdentity; this file is the variant that runs
# it as written.
use strict;
use warnings FATAL => 'all';

use FindBin;
use lib $FindBin::RealBin;
use Stress::Run;

run_template('moving_identity', 'moving_identity');
