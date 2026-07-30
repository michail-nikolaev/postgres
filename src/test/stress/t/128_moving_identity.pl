
# Copyright (c) 2026, PostgreSQL Global Development Group

# The replica identity moved out from under a command that depends on it.
#
# REPACK (CONCURRENTLY) decides which index it will use to replay
# concurrent changes once, in check_concurrent_repack_requirements(),
# and then runs in several transactions.  ALTER TABLE ... REPLICA
# IDENTITY can commit between them, so by the time the replay happens
# the index it chose may no longer be the identity.
#
# That is the same shape as the apply worker holding a stale identity
# index across a REINDEX CONCURRENTLY, which this branch carries a fix
# for; whether REPACK has the same exposure is what this asks.  The
# invariant is the ordinary four-way balance: a replay that fails to
# find a row it should have updated loses the update, and the sums stop
# agreeing.
use strict;
use warnings FATAL => 'all';

use FindBin;
use lib $FindBin::RealBin;
use Stress::Run;

run_scenario(
	'moving_identity',
	{
		schema => [ 'pgbench', 'movable_identity' ],
		pgbench_scale => 1,
		indexes => ['btree_abalance'],
		load => ['tpcb_like'],
		ddl => [
			'repack_concurrently', 'reindex_table_concurrently',
			'move_replica_identity'
		],
		ddl_concurrency => 2,
		checks => [ 'balances', 'amcheck', 'identity_moved' ],
		env => 'standalone',
		# Few clients on purpose: ALTER TABLE ... REPLICA IDENTITY needs
		# AccessExclusiveLock and the bounded helper does not wait, so at
		# twenty clients -- and especially with the rest of the suite
		# running alongside -- it never finds a gap and the identity never
		# moves.  identity_moved reports that rather than letting the
		# scenario pass having tested nothing.
		clients => 4,
		# Exempt from a forced chaos profile.  Moving the identity needs
		# AccessExclusiveLock and the bounded helper does not wait, so on a
		# server slowed by, say, the cache discard it never wins one and
		# the scenario tests nothing -- which identity_moved then reports.
		no_forced_chaos => 1,
		# Longer than the usual six seconds.  Winning an AccessExclusiveLock
		# against continuous writers is a matter of catching a gap, and in
		# six seconds -- with the rest of the suite running alongside -- there
		# may not be one.  identity_moved fails the run when that happens
		# rather than letting it pass having tested nothing, so the scenario
		# needs long enough to be reliable.
		duration => 20,
	});
