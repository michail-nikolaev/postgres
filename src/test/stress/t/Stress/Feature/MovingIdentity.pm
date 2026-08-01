
# Copyright (c) 2026, PostgreSQL Global Development Group

# The replica identity moved between two candidate indexes while
# commands that depend on it are mid-flight.
#
# See Stress::Registry for what each declaration means.

package Stress::Feature::MovingIdentity;

use strict;
use warnings FATAL => 'all';

use Test::More;
use Stress::Registry ':declare';

# Two indexes either of which can serve as the replica identity, so
# that the identity can be moved between them while a command that
# depends on it is running.
#
# REPACK (CONCURRENTLY) picks the identity index once, in
# check_concurrent_repack_requirements(), and then uses it to replay
# the changes that happen while it copies.  It runs in several
# transactions, so ALTER TABLE ... REPLICA IDENTITY can commit in
# between -- and the index it picked may by then be neither the
# identity nor, if it was dropped, present at all.  That is the same
# shape as the apply worker holding a stale identity across a REINDEX,
# which is a fix carried earlier in this branch.
schema movable_identity => {
		# The identity moves between indexes of pgbench_accounts, which
		# the partitioning decorator replaces.
		conflicts => { schema => ['partitioned'] },
		setup => q(
			CREATE UNIQUE INDEX pgb_ident_a ON pgbench_accounts(aid);
			CREATE UNIQUE INDEX pgb_ident_b ON pgbench_accounts(aid)
				INCLUDE (bid);
			ALTER TABLE pgbench_accounts REPLICA IDENTITY USING INDEX pgb_ident_a;

			-- Which identity has been seen, so the run can prove the
			-- identity really moved.  ALTER TABLE ... REPLICA IDENTITY
			-- needs AccessExclusiveLock and goes through the bounded
			-- helper, which gives up rather than waiting; a run where it
			-- always gave up would pass while testing nothing.
			CREATE TABLE pgb_ident_seen(idx name PRIMARY KEY);

			CREATE FUNCTION pgb_note_identity() RETURNS void
			LANGUAGE sql AS $$
				INSERT INTO pgb_ident_seen
					SELECT c.relname FROM pg_index i
					JOIN pg_class c ON c.oid = i.indexrelid
					WHERE i.indrelid = 'pgbench_accounts'::regclass
					  AND i.indisreplident
				ON CONFLICT DO NOTHING;
			$$;
		),
};

# The replica identity moved from one index to another, and back,
# while the rotation rebuilds and repacks the table it belongs to.
#
# ALTER TABLE ... REPLICA IDENTITY needs AccessExclusiveLock, so it
# goes through the bounded helper; what it races is not the lock but
# the several transactions a concurrent command spans, having decided
# which index it would use in the first of them.
# Both moves in one turn, rather than one per turn.  The rotation
# picks between several entries and a six-second run may reach this
# one only once or not at all, so a variant that moved the identity a
# single step could leave a run having seen one identity and proved
# nothing -- which is what identity_moved reported when it was written
# that way.
ddl move_replica_identity => {
		requires => { schema => ['movable_identity'] },
		checks => ['identity_moved'],
		variants => sub {
			return ({
				table => 'pgbench_accounts',
				stmts => [
					"SELECT pgb_ddl_bounded('ALTER TABLE pgbench_accounts "
					  . "REPLICA IDENTITY USING INDEX pgb_ident_b');",
					'SELECT pgb_note_identity();',
					"SELECT pgb_ddl_bounded('ALTER TABLE pgbench_accounts "
					  . "REPLICA IDENTITY USING INDEX pgb_ident_a');",
					'SELECT pgb_note_identity();'
				]
			});
		},
};

# A concurrent drop that fails must not leave an invalid index still
# marked as the replica identity: reindexing such an index makes it
# valid again, and if the table\'s replica identity moved on in the
# meantime, two indexes end up marked and the relcache picks one of
# them arbitrarily.  The failure is forced here rather than waited
# for -- a session holds a lock so the drop has to wait, and
# lock_timeout ends it.
check dic_clears_replident => {
		final => sub {
			my ($node, $ctx) = @_;

			$node->safe_psql(
				'postgres', q(
				DROP TABLE IF EXISTS pgb_ri;
				CREATE TABLE pgb_ri(id int NOT NULL);
				INSERT INTO pgb_ri SELECT g FROM generate_series(1, 100) g;
				CREATE UNIQUE INDEX pgb_ri_idx ON pgb_ri(id);
				ALTER TABLE pgb_ri REPLICA IDENTITY USING INDEX pgb_ri_idx));

			my $holder = $node->background_psql('postgres');
			$holder->query_safe('BEGIN; SELECT count(*) FROM pgb_ri;');

			{
				local $ENV{PGOPTIONS} = '-c lock_timeout=2s';
				$node->psql('postgres', 'DROP INDEX CONCURRENTLY pgb_ri_idx;',
					on_error_stop => 0);
			}
			$holder->quit;

			my $state = $node->safe_psql(
				'postgres', q(
				SELECT COALESCE(string_agg(
					format('%s valid=%s replident=%s', c.relname,
						i.indisvalid, i.indisreplident), ' '), 'gone')
				FROM pg_index i JOIN pg_class c ON c.oid = i.indexrelid
				WHERE c.relname = 'pgb_ri_idx'));
			Test::More::unlike(
				$state,
				qr/valid=f replident=t/,
				'a failed concurrent drop left no invalid replica identity');
			Test::More::note("pgb_ri_idx after the failed drop: $state");

			$node->safe_psql('postgres', 'DROP TABLE IF EXISTS pgb_ri');
		},
};

# The replica identity really moved during the run.
#
# Without this the scenario passes when every ALTER gave up on the
# lock, which is a real possibility: the bounded helper does not wait.
check identity_moved => {
		requires => { schema => ['movable_identity'] },
		# Sampled by the workload too, not only by the rotation, so what
		# ends up recorded does not depend on how often the DDL client
		# happened to pick the entry that moves it.
		script => q(
			SELECT pgb_note_identity();
		),
		final => sub {
			my ($node, $ctx) = @_;
			my $seen = $node->safe_psql('postgres',
				'SELECT count(*) FROM pgb_ident_seen');
			Test::More::cmp_ok($seen, '>=', 2,
				'the replica identity moved between both indexes')
			  or Test::More::diag("identities seen: $seen");
			return;
		},
};

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
#
# One template, two test files: standalone, and against a subscription,
# where the apply worker is a second consumer of the same identity.
scenario_template moving_identity => {
	indexes => ['btree_abalance'],
	load => ['tpcb_like'],
	ddl => [
		'repack_concurrently', 'reindex_table_concurrently',
		'move_replica_identity'
	],
	ddl_concurrency => 2,
	# Few clients on purpose: ALTER TABLE ... REPLICA IDENTITY needs
	# AccessExclusiveLock and the bounded helper does not wait, so at
	# twenty clients -- and especially with the rest of the suite
	# running alongside -- it never finds a gap and the identity never
	# moves.  identity_moved reports that rather than letting the
	# scenario pass having tested nothing.
	clients => 4,
	# Exempt from a forced chaos profile and a forced modifier alike.
	# Moving the identity needs AccessExclusiveLock and the bounded
	# helper does not wait, so on a server slowed by, say, the cache
	# discard or a durability modifier it never wins one and the
	# scenario tests nothing -- which identity_moved then reports.
	no_forced_chaos => 1,
	no_forced_modifier => 1,
	# Longer than the usual six seconds.  Winning an AccessExclusiveLock
	# against continuous writers is a matter of catching a gap, and in
	# six seconds -- with the rest of the suite running alongside -- there
	# may not be one.  identity_moved fails the run when that happens
	# rather than letting it pass having tested nothing, so the scenario
	# needs long enough to be reliable.
	duration => 20,
};

1;
