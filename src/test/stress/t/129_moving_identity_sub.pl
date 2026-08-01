
# Copyright (c) 2026, PostgreSQL Global Development Group

# The moving-identity scenario (Stress::Feature::MovingIdentity) against
# a subscription, where a stale identity index has a second consumer:
# the apply worker, which is where the same shape was caught before.
use strict;
use warnings FATAL => 'all';

use FindBin;
use lib $FindBin::RealBin;
use Stress::Run;

run_template('moving_identity_sub', 'moving_identity',
	env => 'subscription');
