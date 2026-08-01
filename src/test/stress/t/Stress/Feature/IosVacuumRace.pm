
# Copyright (c) 2026, PostgreSQL Global Development Group

# Index-only scans racing vacuum on the access methods that do
# not interlock with it.
#
# See Stress::Registry for what each declaration means.

package Stress::Feature::IosVacuumRace;

use strict;
use warnings FATAL => 'all';

use Test::More;
use Stress::Registry ':declare';

# Two tables shaped for an index-only scan to race a vacuum, one per
# access method that has the problem.
#
# GiST and SP-GiST index scans do not interlock with their own index
# vacuum in a way that guarantees an index-only scan returns only TIDs
# that were still valid when it returned them.  A scan that has queued
# TIDs and let go of the page can have them removed underneath it, and
# the pages they were on marked all-visible -- after which the scan
# returns them from the index without ever looking at the heap, which
# is a row that was deleted before the scan's snapshot began.
#
# The shape is from the report: wide rows and a fillfactor of 10, so
# that a handful of rows fill a page and the pages the deleted rows
# were on can go all-visible on their own.  Autovacuum is off for the
# same reason it is in the isolation test -- the vacuum that matters
# is the one the rotation runs, at a point in the workload rather
# than whenever the daemon happens to wake up.
schema ios_vacuum_race => {
		setup => q(
			CREATE TABLE pgb_ios_gist (
				a point NOT NULL, b int NOT NULL, pad char(1024) DEFAULT '')
				WITH (autovacuum_enabled = false, fillfactor = 10);
			CREATE TABLE pgb_ios_spgist (
				a point NOT NULL, b int NOT NULL, pad char(1024) DEFAULT '')
				WITH (autovacuum_enabled = false, fillfactor = 10);

			INSERT INTO pgb_ios_gist(a, b)
				SELECT point(aid, aid), aid FROM pgbench_accounts
				ORDER BY aid LIMIT 200;
			INSERT INTO pgb_ios_spgist(a, b)
				SELECT point(aid, aid), aid FROM pgbench_accounts
				ORDER BY aid LIMIT 200;

			CREATE INDEX pgb_ios_gist_a_idx ON pgb_ios_gist USING gist (a);
			CREATE INDEX pgb_ios_spgist_a_idx ON pgb_ios_spgist USING spgist (a);

			VACUUM (ANALYZE) pgb_ios_gist;
			VACUUM (ANALYZE) pgb_ios_spgist;

			CREATE SEQUENCE pgb_ios_seq;

			-- An index-only scan held open across a vacuum, checked
			-- against the heap under the same snapshot.
			--
			-- The comparison is between the values themselves rather than
			-- a count of them: every row version gets a y coordinate of
			-- its own from the sequence, so a value the scan returns that
			-- the heap does not have is a row that was deleted before
			-- this snapshot began -- revived because its TID was queued
			-- before a vacuum removed it and marked its page all-visible.
			-- Counting alone cannot tell that from a row still present,
			-- since a deleted row and its replacement share everything a
			-- count can see.
			--
			-- The caller must be in a repeatable read transaction, or the
			-- two sides are read under different snapshots and differ for
			-- honest reasons.
			CREATE FUNCTION pgb_ios_cursor_check(p_table text, p_sleep float8)
			RETURNS boolean LANGUAGE plpgsql AS $fn$
			DECLARE
				c refcursor;
				pt point;
				ios text[] := '{}';
				heap text[];
				extra text[];
				plan text;
			BEGIN
				-- The cursor has to be an index-only scan for any of this
				-- to mean anything.
				-- enable_indexscan has to be pinned as well: it gates
				-- index-only scan paths too, so a server-level modifier
				-- that turned it off would leave this comparing a
				-- sequential scan against a sequential scan and passing
				-- for the wrong reason.
				PERFORM set_config('enable_seqscan', 'off', true);
				PERFORM set_config('enable_bitmapscan', 'off', true);
				PERFORM set_config('enable_indexscan', 'on', true);
				PERFORM set_config('enable_indexonlyscan', 'on', true);

				-- Assert the shape rather than assume it.  If this is not
				-- an index-only scan the comparison below is vacuous.
				-- FORMAT JSON, so the whole plan arrives as one row: a
				-- plain EXPLAIN INTO takes only the first line, which for
				-- this query happens to be the scan and would have made
				-- the test pass for the wrong reason.
				EXECUTE format(
					'EXPLAIN (COSTS OFF, FORMAT JSON) SELECT a FROM %I '
					'ORDER BY a <-> point ''(0,0)''', p_table) INTO plan;
				IF plan NOT LIKE '%%Index Only Scan%%' THEN
					RAISE EXCEPTION
						'not an index-only scan on %: %', p_table, plan;
				END IF;

				OPEN c NO SCROLL FOR EXECUTE format(
					'SELECT a FROM %I ORDER BY a <-> point ''(0,0)''',
					p_table);

				-- The first fetch is what makes the scan read a page and
				-- queue what is on it; the rest come back from that queue.
				FETCH c INTO pt;
				IF NOT FOUND THEN
					CLOSE c;
					RETURN true;
				END IF;
				ios := array_append(ios, pt::text);

				-- The window a vacuum has to land in.
				PERFORM pg_sleep(p_sleep);

				LOOP
					FETCH c INTO pt;
					EXIT WHEN NOT FOUND;
					ios := array_append(ios, pt::text);
				END LOOP;
				CLOSE c;

				-- The same values, under the same snapshot, from the heap.
				PERFORM set_config('enable_seqscan', 'on', true);
				PERFORM set_config('enable_indexscan', 'off', true);
				PERFORM set_config('enable_indexonlyscan', 'off', true);
				EXECUTE format(
					'SELECT coalesce(array_agg(a::text), ''{}'') FROM %I',
					p_table) INTO heap;

				SELECT coalesce(array_agg(v), '{}') INTO extra
					FROM (SELECT unnest(ios) EXCEPT ALL
						  SELECT unnest(heap)) s(v);

				IF cardinality(extra) > 0 THEN
					RAISE EXCEPTION
						'index-only scan on % returned % rows the heap does not have, out of %: %',
						p_table, cardinality(extra), cardinality(ios),
						extra[1:5];
				END IF;
				RETURN true;
			END $fn$;
		),
		# Not in the rotation, for the same reason as pgb_bmskip: no
		# primary key and no replica identity, so REPACK refuses them and
		# the primary-key reindex has nothing to name.  vacuum_ios_tables
		# and the scenario's reindex entry name them directly.
		tables => [],
};

# The index-only scan that has to agree with the heap.  Repeatable
# read, so that the scan and the count that checks it share one
# snapshot; the sleep is the window a vacuum has to land in, and is
# the whole reason this is reachable without controlling the
# interleaving by hand.
load ios_cursor_check => {
		weight => 3,
		requires => { schema => ['ios_vacuum_race'] },
		script => q(
			\set tbl random(0, 1)
			BEGIN ISOLATION LEVEL REPEATABLE READ;
			\if :tbl = 0
				SELECT pgb_ios_cursor_check('pgb_ios_gist', 0.02);
			\else
				SELECT pgb_ios_cursor_check('pgb_ios_spgist', 0.02);
			\endif
			COMMIT;
		),
};

# Rows dying and coming back, so that there is always something for a
# vacuum to remove and an index entry pointing at where it was.  The
# row count is left where it started, which is what the scan's own
# check relies on.
load ios_churn => {
		weight => 4,
		requires => { schema => ['ios_vacuum_race'] },
		script => q(
			\set k random(1, 200)
			\set tbl random(0, 1)
			BEGIN;
			\if :tbl = 0
				DELETE FROM pgb_ios_gist WHERE b = :k;
				INSERT INTO pgb_ios_gist(a, b)
					VALUES (point(:k, nextval('pgb_ios_seq')), :k);
			\else
				DELETE FROM pgb_ios_spgist WHERE b = :k;
				INSERT INTO pgb_ios_spgist(a, b)
					VALUES (point(:k, nextval('pgb_ios_seq')), :k);
			\endif
			COMMIT;
		),
};

# The vacuum that removes what the scan queued.  TRUNCATE false
# because truncation wants AccessExclusiveLock, which against a
# workload only ends in the lock timeout -- and truncation is no part
# of what is being tested.  Aimed at these tables on their own so it
# runs as often as the rotation will allow.
ddl vacuum_ios_tables => {
		requires => { schema => ['ios_vacuum_race'] },
		variants => sub {
			return map {
				{ table => $_, stmts => ["VACUUM (TRUNCATE false) $_;"] }
			} (qw(pgb_ios_gist pgb_ios_spgist));
		},
};

1;
