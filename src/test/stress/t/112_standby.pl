
# Copyright (c) 2026, PostgreSQL Global Development Group

# A hot standby replaying the rebuilds while it serves the checks, then
# taking over.
#
# Without the fix in get_relation_info() that tolerates a concurrently
# dropped index, this fails about one run in ten at
# stress_concurrently=4, with
#
#   ERROR:  could not open relation with OID <n>
#
# from a reader on the standby: on a primary index_drop() waits for
# every locker of the table before removing the index, so holding the
# table lock keeps the index list good, and replay provides no such
# interlock.
#
# What makes it reachable is the quiet_index dimension -- a few hundred
# rows nothing writes, whose index the rotation drops and recreates in
# about a millisecond, so the standby replays that come and go hundreds
# of times rather than a few.  Against pgbench_accounts alone, where
# the same cycle takes hundreds of milliseconds, twenty runs found
# nothing.  hot_standby_feedback makes no difference either way, which
# is what one would expect: it holds back the vacuum horizon, not the
# locks replay takes for DDL.

use strict;
use warnings FATAL => 'all';

use FindBin;
use lib $FindBin::RealBin;
use Stress::Run;

run_scenario(
	'standby',
	{
		schema => ['quiet_index'],
		indexes => ['btree_abalance'],
		load => ['tpcb_like'],
		ddl => [@STANDARD_DDL],
		# Of the checks that join this scenario, two matter to the standby
		# in particular: index_vs_seq forces the planner to build an index
		# plan there, which is where a stale index list turns into an
		# error rather than a slower plan, and quiet_index_scan -- which
		# is why the quiet_index schema is named -- makes it plan against
		# the index replay keeps removing.
		topology => 'standby',
		clients => 20,
		tags => ['ci'],
	});
