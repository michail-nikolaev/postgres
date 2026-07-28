# Copyright (c) 2026, PostgreSQL Global Development Group

# The long run over the whole matrix; see t/Stress/Soak.pm.
# Off unless PG_TEST_EXTRA asks for it.
use strict;
use warnings FATAL => 'all';

use Test::More;

use FindBin;
use lib $FindBin::RealBin;
use Stress::Run;
use Stress::Soak;

plan skip_all => 'soak mode not requested (stress_mode=soak)'
  unless soak_enabled();

soak_run();
done_testing();
