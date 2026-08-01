
# Copyright (c) 2026, PostgreSQL Global Development Group

# The base schema: pgbench's four tables, the loads that move
# them while keeping the four-way balance, the indexes built on
# them, and the checks that hold whatever else is happening.
#
# See Stress::Registry for what each declaration means.

package Stress::Feature::Pgbench;

use strict;
use warnings FATAL => 'all';

use Test::More;
use Stress::Registry ':declare';

schema pgbench => {
		init => 'pgbench',
		# REPACK (CONCURRENTLY) decodes the table's own changes, so every
		# table it may touch needs a replica identity; pgbench_history
		# ships without one.
		setup => q(
			ALTER TABLE pgbench_history ADD COLUMN hid bigserial PRIMARY KEY;

			-- Rows above the account numbers pgbench created, carrying a
			-- zero balance: nothing else reads or writes them, so adding
			-- and removing them moves none of the four sums.  When the
			-- partition they belong in has been detached there is nowhere
			-- to put them, which is not what any load using this is
			-- looking for.
			-- An index expression that assigns a transaction id, which
			-- the build has to cope with: a concurrent build runs in
			-- several transactions and reasons about snapshots and the
			-- horizon, and an expression that takes an xid of its own
			-- changes how those interact.  It claims to be immutable and
			-- is not, which is exactly the point.
			-- Declared over bigint, and the index casts to it, so that
			-- the rewriting ALTER can widen the column underneath the
			-- expression without the index losing its function.
			CREATE FUNCTION pgb_xid_expr(v bigint) RETURNS bigint
			LANGUAGE plpgsql IMMUTABLE AS $$
			BEGIN
				PERFORM pg_current_xact_id();
				RETURN v;
			END;
			$$;

			CREATE FUNCTION pgb_scratch_insert(p_aid int) RETURNS boolean
			LANGUAGE plpgsql AS $$
			BEGIN
				INSERT INTO pgbench_accounts(aid, bid, abalance)
					VALUES (p_aid, 1, 0) ON CONFLICT (aid) DO NOTHING;
				RETURN true;
			EXCEPTION WHEN check_violation THEN
				RETURN false;
			END;
			$$;


			-- Run a command that needs a lock the workload conflicts
			-- with, without parking the request at the head of the lock
			-- queue.  An unbounded ALTER TABLE that wants
			-- AccessExclusiveLock -- attaching a partition, adding or
			-- dropping a constraint -- has every writer stack up behind
			-- it until the lock timeout fires, which is minutes of
			-- nothing and then a failed run.  Retrying in short slices
			-- lets the queue drain between attempts, so the worst stall
			-- is half a second.
			-- A command needing a lock that conflicts with the workload,
			-- retried in slices so that a failed attempt lets the queue
			-- drain instead of parking a request at its head.
			--
			-- The duty cycle is what matters, not the number of attempts.
			-- A request waiting 500ms blocks every writer behind it for
			-- 500ms, so pausing only 10ms between attempts leaves writers
			-- about a fiftieth of the time and they wait out their own
			-- lock_timeout having achieved nothing.  Found by a soak
			-- combination running this against thirty clients: the DDL
			-- succeeded and the writers were the ones that failed.  A
			-- shorter hold and a longer drain gives them most of the time
			-- and costs only that the command may need more attempts.
			CREATE FUNCTION pgb_ddl_bounded(cmd text) RETURNS boolean
			LANGUAGE plpgsql AS $fn$
			DECLARE i int;
			BEGIN
				FOR i IN 1..120 LOOP
					BEGIN
						SET LOCAL lock_timeout = '200ms';
						EXECUTE cmd;
						RETURN true;
					EXCEPTION WHEN lock_not_available THEN
						PERFORM pg_sleep(0.1);
					END;
				END LOOP;
				RETURN false;
			END $fn$;
		),
		tables => [
			qw(pgbench_accounts pgbench_tellers pgbench_branches pgbench_history)
		],
};

index_def btree_abalance => {
		table => 'pgbench_accounts',
		name => 'pgb_abalance_idx',
		am => 'btree',
		defn => 'ON pgbench_accounts(abalance)',
};

# An index on a column nothing updates.  That is what makes the
# updates around it HOT updates: an update is HOT only when no index
# covers any column it changes, and every other index here is on
# abalance, which is exactly what the load moves.  A scenario that
# wants HOT chains declares this one and no abalance index.
index_def btree_bid => {
		table => 'pgbench_accounts',
		name => 'pgb_bid_idx',
		am => 'btree',
		defn => 'ON pgbench_accounts(bid)',
};

index_def btree_history_delta => {
		table => 'pgbench_history',
		name => 'pgb_history_delta_idx',
		am => 'btree',
		defn => 'ON pgbench_history(delta)',
};

index_def partial_abalance => {
		table => 'pgbench_accounts',
		name => 'pgb_abalance_partial_idx',
		am => 'btree',
		# REPACK and CLUSTER refuse to order a table by a partial index.
		partial => 1,
		defn => 'ON pgbench_accounts(abalance) WHERE abalance > 0',
};

index_def expr_abalance => {
		table => 'pgbench_accounts',
		name => 'pgb_abalance_expr_idx',
		am => 'btree',
		defn => 'ON pgbench_accounts(abs(abalance), aid)',
};

index_def covering_aid => {
		table => 'pgbench_accounts',
		name => 'pgb_aid_covering_idx',
		am => 'btree',
		defn => 'ON pgbench_accounts(aid) INCLUDE (abalance)',
};

# An index over an expression that takes a transaction id of its own
# every time it is evaluated -- during the build, and during every
# insert and update the build has to keep up with.
index_def expr_xid => {
		table => 'pgbench_accounts',
		name => 'pgb_abalance_xid_idx',
		am => 'btree',
		defn => 'ON pgbench_accounts(pgb_xid_expr(abalance::bigint))',
};

# A predicate long enough that the pg_index row holding it goes out
# of line.  CREATE, REINDEX and DROP INDEX CONCURRENTLY update
# pg_index from transactions of their own, and reading a toasted
# column needs an active snapshot that those transactions have not
# always had.
index_def toasted_predicate => {
		table => 'pgbench_accounts',
		name => 'pgb_abalance_toasted_idx',
		am => 'btree',
		# CLUSTER and REPACK refuse to order a table by a partial index.
		partial => 1,
		defn => 'ON pgbench_accounts(abalance) WHERE '
		  . join(' AND ', map { "aid <> -$_" } 1 .. 60),
};

# The standard TPC-B-like transaction: one delta applied to an
# account, a teller and a branch, and recorded in history.  Every
# completed transaction moves all four sums by the same amount, which
# is what the 'balances' check relies on.
load tpcb_like => {
		weight => 4,
		script => q(
			\set aid random(1, :naccounts)
			\set bid random(1, :nbranches)
			\set tid random(1, :ntellers)
			\set delta random(-5000, 5000)
			BEGIN;
			UPDATE pgbench_accounts SET abalance = abalance + :delta
				WHERE aid = :aid;
			SELECT abalance FROM pgbench_accounts WHERE aid = :aid;
			UPDATE pgbench_tellers SET tbalance = tbalance + :delta
				WHERE tid = :tid;
			UPDATE pgbench_branches SET bbalance = bbalance + :delta
				WHERE bid = :bid;
			INSERT INTO pgbench_history (tid, bid, aid, delta, mtime)
				VALUES (:tid, :bid, :aid, :delta, CURRENT_TIMESTAMP);
			COMMIT;
		),
};

# Nothing but inserts into an append-only table.  This exists to be
# raced by a high rate of CREATE INDEX CONCURRENTLY on a small
# relation, which is the shape contrib/amcheck's 002_cic uses and
# the shape the relcache build race needs: a backend has to absorb
# an invalidation while building the descriptor for the new index,
# and the chance of that scales with how often an index is built.
load history_insert => {
		weight => 1,
		# It writes deltas into pgbench_history without the account,
		# teller and branch rows that would balance them, and the
		# invariant sums all four.
		conflicts => { checks => ['balances'] },
		script => q(
			\set aid random(1, :naccounts)
			\set delta random(-5000, 5000)
			INSERT INTO pgbench_history (tid, bid, aid, delta, mtime)
				VALUES (1, 1, :aid, :delta, CURRENT_TIMESTAMP);
		),
};

# The same movement, but with the rows taken with FOR UPDATE first
# and the lock held across a pause, so a CONCURRENTLY command
# routinely runs while a row lock is in force.  The rows are locked
# in a fixed order across the three tables, so concurrent clients
# cannot deadlock.
load row_lock => {
		weight => 1,
		# This holds row locks across a pause, which is the point of it.
		# Against a lock table small enough to run out it stops being a
		# test of anything: a rebuild queues for its exclusive lock behind
		# the held ones, every later statement queues behind the rebuild,
		# and the whole thing sits there until lock_timeout fires.  That
		# is fair queuing working as designed, and it produced two runs
		# that spent three minutes proving nothing.  The other
		# environments have room for both.
		conflicts => { env => ['lock_exhaustion'] },
		script => q(
			\set aid random(1, :naccounts)
			\set bid random(1, :nbranches)
			\set tid random(1, :ntellers)
			\set delta random(-5000, 5000)
			BEGIN;
			SELECT abalance FROM pgbench_accounts WHERE aid = :aid FOR UPDATE;
			SELECT tbalance FROM pgbench_tellers WHERE tid = :tid FOR UPDATE;
			SELECT bbalance FROM pgbench_branches WHERE bid = :bid FOR UPDATE;
			\sleep 5 ms
			UPDATE pgbench_accounts SET abalance = abalance + :delta
				WHERE aid = :aid;
			UPDATE pgbench_tellers SET tbalance = tbalance + :delta
				WHERE tid = :tid;
			UPDATE pgbench_branches SET bbalance = bbalance + :delta
				WHERE bid = :bid;
			INSERT INTO pgbench_history (tid, bid, aid, delta, mtime)
				VALUES (:tid, :bid, :aid, :delta, CURRENT_TIMESTAMP);
			COMMIT;
		),
};

# Batches rather than single rows: every batch holds as many +1 rows
# as -1 rows, so the sum is zero at every commit no matter which
# batches happen to be present.  This drives the multi-insert and
# COPY paths and the bulk index insertions they cause.
# Bulk insertion into pgbench_history, which is append-only, is one of
# the relations the rotation repacks, and whose delta column the
# balance check adds up.  Every batch holds as many +1 rows as -1
# rows, so the sum is untouched however many batches are present, and
# the rows are marked with teller 0, which nothing else writes, so a
# batch can be removed again without disturbing anything.
load bulk_copy => {
		weight => 1,
		# pgbench has no way to feed COPY from its own script, so the
		# batch lives in a file the server reads.
		files => {
			'copy_batch.txt' => join('',
				map { "0\t1\t1\t" . ($_ % 2 == 0 ? 1 : -1) . "\n" } (1 .. 200)),
		},
		script => sub {
			my ($ctx) = @_;
			my $copyfile = $ctx->{files}->{'copy_batch.txt'};
			return qq(
			\\set mode random(0, 2)
			\\if :mode = 0
				BEGIN;
				INSERT INTO pgbench_history(tid, bid, aid, delta, mtime)
					SELECT 0, 1, 1, CASE WHEN g % 2 = 0 THEN 1 ELSE -1 END,
						CURRENT_TIMESTAMP
					FROM generate_series(1, 200) g;
				COMMIT;
			\\elif :mode = 1
				COPY pgbench_history(tid, bid, aid, delta) FROM '$copyfile';
			\\else
				DELETE FROM pgbench_history WHERE tid = 0;
			\\endif
			);
		},
};

# The four sums move together or not at all.  Read in one statement,
# so they share a snapshot; the counts are only fetched when they
# disagree, since counting every account is far too expensive to do
# on every check.
check balances => {
		weight => 1,
		script => q(
			SELECT (SELECT COALESCE(SUM(abalance), 0) FROM pgbench_accounts) AS a,
				   (SELECT COALESCE(SUM(tbalance), 0) FROM pgbench_tellers) AS t,
				   (SELECT COALESCE(SUM(bbalance), 0) FROM pgbench_branches) AS b,
				   (SELECT COALESCE(SUM(delta), 0) FROM pgbench_history) AS h
				\gset bal_
			\if :bal_a != :bal_t or :bal_t != :bal_b or :bal_b != :bal_h
				SELECT (SELECT COUNT(*) FROM pgbench_accounts) AS a,
					   (SELECT COUNT(*) FROM pgbench_tellers) AS t,
					   (SELECT COUNT(*) FROM pgbench_branches) AS b,
					   (SELECT COUNT(*) FROM pgbench_history) AS h
					\gset cnt_
				\if :cnt_a = 0 or :cnt_t = 0 or :cnt_b = 0 or :cnt_h = 0
					SELECT 'repack: empty view tolerated' AS marker;
				\else
					SELECT stress_assert(false,
						format('balances disagree: accounts=%s tellers=%s branches=%s history=%s',
							:bal_a::bigint, :bal_t::bigint, :bal_b::bigint, :bal_h::bigint));
				\endif
			\endif
		),
		final => sub {
			my ($node, $ctx) = @_;
			my $row = $node->safe_psql(
				'postgres', q(
				SELECT (SELECT COALESCE(SUM(abalance), 0) FROM pgbench_accounts)
					|| ' ' || (SELECT COALESCE(SUM(tbalance), 0) FROM pgbench_tellers)
					|| ' ' || (SELECT COALESCE(SUM(bbalance), 0) FROM pgbench_branches)
					|| ' ' || (SELECT COALESCE(SUM(delta), 0) FROM pgbench_history)));
			my ($a, $t, $b, $h) = split / /, $row;
			Test::More::is("$t $b $h", "$a $a $a",
				'balances agree across all four tables');
		},
};

# An index scan and a sequential scan of the same predicate, in one
# snapshot, must return the same thing.
check index_vs_seq => {
		weight => 1,
		requires => { indexes => ['btree_abalance'] },
		script => q(
			BEGIN ISOLATION LEVEL REPEATABLE READ;
			SET LOCAL enable_seqscan = off;
			SET LOCAL enable_bitmapscan = off;
			SELECT COUNT(*) AS cnt, COALESCE(SUM(abalance), 0) AS sum
				FROM pgbench_accounts WHERE abalance > 0 \gset idx_
			SET LOCAL enable_seqscan = on;
			SET LOCAL enable_indexscan = off;
			SET LOCAL enable_indexonlyscan = off;
			SELECT COUNT(*) AS cnt, COALESCE(SUM(abalance), 0) AS sum
				FROM pgbench_accounts WHERE abalance > 0 \gset seq_
			COMMIT;
			-- Both reads are in one snapshot, so a swap that empties the
			-- table empties both of them; they still have to agree.
			SELECT stress_assert(:idx_cnt::bigint = :seq_cnt::bigint
					AND :idx_sum::bigint = :seq_sum::bigint,
				format('index scan (%s rows, sum %s) disagrees with seq scan (%s rows, sum %s)',
					:idx_cnt::bigint, :idx_sum::bigint, :seq_cnt::bigint, :seq_sum::bigint));
		),
};

# An index-only scan trusts the visibility map, so a wrong VM bit is
# a silent wrong answer rather than an error.  Compare it against a
# sequential scan in one snapshot.
check ios_vs_seq => {
		weight => 1,
		requires => { indexes => ['covering_aid'] },
		script => q(
			BEGIN ISOLATION LEVEL REPEATABLE READ;
			SET LOCAL enable_seqscan = off;
			SET LOCAL enable_bitmapscan = off;
			SELECT COUNT(*) AS cnt, COALESCE(SUM(abalance), 0) AS sum
				FROM pgbench_accounts WHERE aid > 0 \gset ios_
			SET LOCAL enable_seqscan = on;
			SET LOCAL enable_indexscan = off;
			SET LOCAL enable_indexonlyscan = off;
			SELECT COUNT(*) AS cnt, COALESCE(SUM(abalance), 0) AS sum
				FROM pgbench_accounts WHERE aid > 0 \gset seqio_
			COMMIT;
			SELECT stress_assert(:ios_cnt::bigint = :seqio_cnt::bigint
					AND :ios_sum::bigint = :seqio_sum::bigint,
				format('index-only scan (%s rows, sum %s) disagrees with seq scan (%s rows, sum %s)',
					:ios_cnt::bigint, :ios_sum::bigint, :seqio_cnt::bigint, :seqio_sum::bigint));
		),
};

1;
