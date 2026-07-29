
# Copyright (c) 2026, PostgreSQL Global Development Group

# A snapshot built from decoded COMMIT records, used before CLOG agrees.
#
# RecordTransactionCommit() writes the commit record, flushes it, and
# only then sets the transaction's status in CLOG.  The flush is what
# wakes a decoder, so between the two there is a window in which a
# transaction is committed as far as logical decoding is concerned and
# not committed as far as CLOG is concerned.
#
# The snapshot builder takes the decoded commit into account and leaves
# the transaction out of the running list.  A snapshot built there and
# used for an ordinary visibility check therefore reads the transaction
# as neither running nor committed, which HeapTupleSatisfiesMVCC treats
# as aborted -- and records that conclusion in the hint bits, so the
# damage outlives the window.  An UPDATE loses both versions of its row.
#
# REPACK (CONCURRENTLY) is one of the two places where such a snapshot
# is used for ordinary visibility: repack_worker.c calls
# SnapBuildInitialSnapshot() and the backend copies the old heap under
# it.  So the shape here is a tight REPACK loop over the smallest hot
# table there is -- one row at scale 1, rewritten by every transaction
# in the workload -- with far more clients than cores, so that a backend
# stalls between its flush and its CLOG update often enough to lose the
# race.
#
# The invariant is the ordinary one: a row dropped by the copy takes
# pgbench_branches out of step with the other three tables for good.
#
# https://www.postgresql.org/message-id/flat/85833.1768840165@localhost
use strict;
use warnings FATAL => 'all';

use Test::More;

use FindBin;
use lib $FindBin::RealBin;
use Stress::Run;

# The bug this reproduces is still open, so this scenario fails against
# master by design and would leave the suite permanently red.  It is
# asked for rather than run, and becomes an ordinary scenario the day the
# fix lands.
# $COLLECT means soak is reading this file for its spec rather than
# running it; skipping then would take the whole soak run with it.
plan skip_all => 'open-bug reproducers not requested (stress_open_bugs=1)'
  unless $Stress::Run::COLLECT
  || ($ENV{PG_TEST_EXTRA} // '') =~ /\bstress_open_bugs=1\b/;

run_scenario(
	'decoding_startup_race',
	{
		schema => ['pgbench'],
		# Enough branches and tellers that the workload is not serialized
		# on one row: what this needs is many backends committing at
		# once, not many backends queued behind the same tuple.
		pgbench_scale => 10,
		load => ['tpcb_like'],
		ddl => ['repack_hot_small'],
		ddl_concurrency => 2,
		checks => [ 'balances', 'amcheck' ],
		env => 'standalone',
		# The whole scenario turns on this: the window between the flush
		# that lets a decoder see a commit and the CLOG update that lets
		# an ordinary snapshot see it is under 50us in nature, and REPACK
		# needs it to outlast a 6-15ms gap.  The profile widens it for one
		# commit in eight.  Without this the scenario runs clean and
		# proves nothing; see REGRESSIONS for the measurements.
		chaos => 'decoding',
		# Fails against master by design: soak leaves it out of the
		# catalogue walk unless open-bug reproducers were asked for.
		tags => ['open-bug'],
		# Far more clients than cores, deliberately.  The window is only
		# as wide as the committing backend is slow between its flush and
		# its CLOG update, and the thing that makes a backend slow there
		# is being taken off CPU.
		clients => 300,
		conf => [
			'max_connections = 400',
			# The default for a test cluster logs every statement, which
			# at this rate costs more than the workload does.
			'log_statement = none'
		],
	});
