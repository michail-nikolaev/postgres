
# Copyright (c) 2026, PostgreSQL Global Development Group

# Speculative insertion: upserts and MERGE against arbiter
# indexes the rotation rebuilds, including the nulls-not-distinct
# form.
#
# See Stress::Registry for what each declaration means.

package Stress::Feature::Upsert;

use strict;
use warnings FATAL => 'all';

use Test::More;
use Stress::Registry ':declare';

# A unique index that treats nulls as equal.  The executor compares
# ii_NullsNotDistinct when it decides whether one index can stand in
# for another as an arbiter, and nothing here ever set it.
schema nulls_not_distinct => {
		setup => q(
			CREATE TABLE pgb_nnd(id serial PRIMARY KEY, k int, v int);
			INSERT INTO pgb_nnd(k, v) SELECT g, 0 FROM generate_series(1, 500) g;
			INSERT INTO pgb_nnd(k, v) VALUES (NULL, 0);
			CREATE UNIQUE INDEX pgb_nnd_k ON pgb_nnd(k) NULLS NOT DISTINCT;
		),
		tables => ['pgb_nnd'],
};

# Rows that are only ever upserted, never deleted, so every upsert
# and every MERGE takes its "matched" path.  The arbiter is
# pgbench_accounts' own primary key, which the rotation rebuilds
# underneath the speculative insertions.
schema upsert_keys => {
		setup => q(
			ALTER TABLE pgbench_accounts ADD COLUMN ukey int DEFAULT 0;

			-- A second unique index over the same column as the primary
			-- key, so that ON CONFLICT (aid) infers two arbiters rather
			-- than one.  Every arbiter in this suite used to be a
			-- primary key, which meant the executor's arbiter list was
			-- always a single entry and the code that matches several of
			-- them onto a partition, dedupes them by parent and counts
			-- the ones being rebuilt was never given anything to do.
			-- reindex_table_concurrently and the repacks rebuild this
			-- one along with the rest.
			CREATE UNIQUE INDEX pgb_aid_uniq ON pgbench_accounts(aid);
		),
};

# Upserts that really do insert.  upsert_merge only ever meets rows
# that already exist, so it always takes the matched path and never
# reaches speculative insertion -- and speculative insertion is where
# an arbiter index that two transactions disagree about does its
# damage.  Here the keys live in a narrow band above the ones pgbench
# created, and a quarter of the work deletes them again, so a key is
# forever going missing and being raced for by several clients at
# once.  The rows carry no balance, so the four-way total is
# untouched no matter how many of them exist.
load upsert_contend => {
		weight => 3,
		requires => { schema => ['upsert_keys'] },
		checks => ['distinct_keys'],
		script => q(
			\set k random(:naccounts + 1, :naccounts + 16)
			\set v random(1, 100000)
			\set mode random(0, 3)
			\if :mode = 0
				DELETE FROM pgbench_accounts WHERE aid = :k;
			\elif :mode = 1
				-- Naming the constraint rather than the attribute
				-- resolves the arbiter a different way, and during a
				-- rebuild several indexes answer to the constraint's
				-- definition at once.
				INSERT INTO pgbench_accounts(aid, bid, abalance, ukey)
					VALUES (:k, 1, 0, :v)
					ON CONFLICT ON CONSTRAINT pgbench_accounts_pkey
					DO UPDATE SET ukey = EXCLUDED.ukey;
			\else
				INSERT INTO pgbench_accounts(aid, bid, abalance, ukey)
					VALUES (:k, 1, 0, :v)
					ON CONFLICT (aid) DO UPDATE SET ukey = EXCLUDED.ukey;
			\endif
		),
};

# Upserts whose arbiter is the nulls-not-distinct index, including
# the null key, which conflicts with itself under that index.
load nnd_upsert => {
		weight => 2,
		requires => { schema => ['nulls_not_distinct'] },
		script => q(
			\set k random(1, 600)
			\set usenull random(0, 9)
			\if :usenull = 0
				INSERT INTO pgb_nnd(k, v) VALUES (NULL, 1)
					ON CONFLICT (k) DO UPDATE SET v = pgb_nnd.v + 1;
			\else
				INSERT INTO pgb_nnd(k, v) VALUES (:k, 1)
					ON CONFLICT (k) DO UPDATE SET v = pgb_nnd.v + 1;
			\endif
		),
};

# Every key exists and none is ever deleted, so an upsert and a MERGE
# both take their "matched" path.  A rebuild that missed a row being
# upserted at that moment would put a second row under the same key.
#
# The two ON CONFLICT forms are both here because they reach the
# arbiter index by different routes: inferring from the column list,
# and naming the constraint.  While REINDEX CONCURRENTLY swaps a
# constraint's index, more than one index matches the constraint, and
# a speculative insertion that picks a different set from its
# neighbour reports a duplicate key that is not there.
load upsert_merge => {
		weight => 3,
		requires => { schema => ['upsert_keys'] },
		checks => ['distinct_keys'],
		script => q(
			\set k random(1, :naccounts)
			\set v random(1, 100000)
			\set mode random(0, 2)
			-- Only ukey is ever written, so none of these disturbs the
			-- four-way balance; the row always exists, so all three take
			-- their matched path and no row is ever added.
			\if :mode = 0
				INSERT INTO pgbench_accounts(aid, bid, abalance, ukey)
					VALUES (:k, 1, 0, :v)
					ON CONFLICT (aid) DO UPDATE SET ukey = EXCLUDED.ukey;
			\elif :mode = 1
				INSERT INTO pgbench_accounts(aid, bid, abalance, ukey)
					VALUES (:k, 1, 0, :v)
					ON CONFLICT ON CONSTRAINT pgbench_accounts_pkey
					DO UPDATE SET ukey = EXCLUDED.ukey;
			\else
				-- The casts give the USING columns a type: without them
				-- the parameters arrive untyped under the extended and
				-- prepared protocols and come out as text.
				MERGE INTO pgbench_accounts t
					USING (SELECT :k::int AS aid, :v::int AS ukey) s
					ON t.aid = s.aid
					WHEN MATCHED THEN UPDATE SET ukey = s.ukey;
			\endif
		),
};

# Nothing may ever hold two rows under one key.
check distinct_keys => {
		weight => 1,
		requires => { schema => ['upsert_keys'] },
		script => q(
			SELECT stress_assert(cnt = keys,
				format('%s rows under %s keys', cnt, keys))
			FROM (SELECT COUNT(*) AS cnt, COUNT(DISTINCT aid) AS keys
				FROM pgbench_accounts) x;
		),
		final => sub {
			my ($node, $ctx) = @_;
			Test::More::is(
				$node->safe_psql('postgres',
					'SELECT COUNT(*) - COUNT(DISTINCT aid) FROM pgbench_accounts'),
				'0', 'no duplicate key got past the unique index');
		},
};

1;
