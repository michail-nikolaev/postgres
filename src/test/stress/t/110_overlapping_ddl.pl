
# Copyright (c) 2026, PostgreSQL Global Development Group

# Several CONCURRENTLY commands in flight at once on different tables,
# each taking a transient slot of its own and switching logical decoding
# on and off.  wal_level = replica, so those switches are real.
#
# Gates two commits.
#
# 2af1dc89282, "Disable logical decoding after REPACK (CONCURRENTLY)".
# Without it -- with the REPACK worker dropping its slot without asking
# for decoding to be turned off again -- this fails five runs in five at
# the default duration on
#
#   not ok - logical decoding was switched back off
#
# leaving effective_wal_level stuck at 'logical' and every writer paying
# for the extra WAL.  Note that this failure is slow to arrive: the
# check polls, so a failing run waits out the timeout, and a red
# 110_overlapping_ddl that took three minutes is this and not a hang.
#
# 0fd30e2119e, "Fix race condition when enabling logical decoding
# concurrently".  Without its recheck in EnableLogicalDecoding() after
# the procsignal barrier, this fails eight runs in ten at
# stress_concurrently=4 with
#
#   ERROR:  unexpected logical decoding status change 1
#
# from a REPACK decoding worker or the backend that launched it: two
# activations overlapping, one of them writing a second activation
# record that the other's decoding then runs into.  This is the scenario
# that puts several of them in flight at once, which is what makes the
# overlap likely.
use strict;
use warnings FATAL => 'all';

use FindBin;
use lib $FindBin::RealBin;
use Stress::Run;

run_scenario(
	'overlapping_ddl',
	{
		load => [ 'tpcb_like', 'balanced_pair' ],
		ddl => [@STANDARD_DDL],
		ddl_concurrency => 4,
		env => 'wal_replica',
		clients => 30,
		tags => ['ci'],
	});
