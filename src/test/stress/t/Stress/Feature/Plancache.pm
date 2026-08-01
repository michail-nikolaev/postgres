
# Copyright (c) 2026, PostgreSQL Global Development Group

# Cached plans: plpgsql functions, held cursors, subtransaction
# churn and CONCURRENTLY on temporary tables.
#
# See Stress::Registry for what each declaration means.

package Stress::Feature::Plancache;

use strict;
use warnings FATAL => 'all';

use Test::More;
use Stress::Registry ':declare';

# Savepoints, and a PL/pgSQL loop whose body is an exception block --
# a subtransaction per iteration, enough of them to overflow the
# backend's subxid cache while a CONCURRENTLY command waits on it.
load subxact_churn => {
		weight => 2,
		requires => { schema => ['ledger'] },
		checks => ['ledger_sum'],
		setup => q(
			CREATE FUNCTION pgb_subxact_churn(lo int, hi int, diff int, n int)
			RETURNS void LANGUAGE plpgsql AS $$
			DECLARE
				i int;
			BEGIN
				FOR i IN 1 .. n LOOP
					BEGIN
						UPDATE pgbench_accounts SET ledger = ledger + diff WHERE aid = lo;
						UPDATE pgbench_accounts SET ledger = ledger - diff WHERE aid = hi;
						IF i < n THEN
							RAISE EXCEPTION 'discarding subtransaction %', i;
						END IF;
					EXCEPTION WHEN raise_exception THEN
						NULL;
					END;
				END LOOP;
			END;
			$$;
		),
		script => q(
			\set a random(1, :naccounts)
			\set b random(1, :naccounts)
			\set lo least(:a, :b)
			\set hi greatest(:a, :b)
			\set diff random(1, 10000)
			\set mode random(0, 1)
			\if :mode = 0
				BEGIN;
				SAVEPOINT sp1;
				UPDATE pgbench_accounts SET ledger = ledger + :diff WHERE aid = :lo;
				ROLLBACK TO SAVEPOINT sp1;
				UPDATE pgbench_accounts SET ledger = ledger + :diff WHERE aid = :lo;
				UPDATE pgbench_accounts SET ledger = ledger - :diff WHERE aid = :hi;
				COMMIT;
			\else
				SELECT pgb_subxact_churn(:lo, :hi, :diff, 80);
			\endif
		),
};

# A cursor held open across a pause, driven from PL/pgSQL so the
# whole scan-with-a-pause happens inside one call.  What the cursor
# reads must stay readable and consistent while the indexes under it
# are rebuilt.
load cursor_hold => {
		weight => 1,
		requires => { schema => ['ledger'] },
		checks => ['ledger_sum'],
		setup => q(
			CREATE FUNCTION pgb_cursor_sum(expected bigint) RETURNS void
			LANGUAGE plpgsql AS $$
			DECLARE
				c CURSOR FOR SELECT ledger FROM pgbench_accounts;
				v int;
				total bigint := 0;
				seen int := 0;
			BEGIN
				OPEN c;
				LOOP
					FETCH c INTO v;
					EXIT WHEN NOT FOUND;
					total := total + v;
					seen := seen + 1;
					IF seen = 100 THEN
						PERFORM pg_sleep(0.005);
					END IF;
				END LOOP;
				CLOSE c;
				-- REPACK (CONCURRENTLY) is not MVCC-safe yet: a snapshot
				-- spanning its swap may find the table empty.  Anything
				-- else must add up.
				IF seen <> 0 AND total <> expected THEN
					RAISE EXCEPTION 'cursor read % over % rows, not %',
						total, seen, expected;
				END IF;
			END;
			$$;
		),
		script => q(
			SELECT pgb_cursor_sum(0);
		),
};

# The same reads through a PL/pgSQL function, whose plans are cached
# in its own plan cache across calls.  Combined with the scenario's
# prepared protocol and force_generic_plan, this keeps plans alive
# across the DDL that must invalidate them.
load plancache => {
		weight => 2,
		requires => { schema => ['ledger'] },
		checks => ['ledger_sum'],
		setup => q(
			CREATE FUNCTION pgb_cached_sum() RETURNS bigint
			LANGUAGE plpgsql AS $$
			DECLARE
				total bigint;
			BEGIN
				SELECT COALESCE(SUM(ledger), 0) INTO total FROM pgbench_accounts;
				RETURN total;
			END;
			$$;
		),
		script => q(
			\set a random(1, :naccounts)
			\set b random(1, :naccounts)
			\set lo least(:a, :b)
			\set hi greatest(:a, :b)
			\set diff random(1, 10000)
			BEGIN;
			UPDATE pgbench_accounts SET ledger = ledger + :diff WHERE aid = :lo;
			UPDATE pgbench_accounts SET ledger = ledger - :diff WHERE aid = :hi;
			COMMIT;
			SELECT stress_assert(s = 0,
				-- The cast matters under the prepared protocol, where
				-- the variable below becomes a query parameter and
				-- format() gives the planner nothing to infer its type
				-- from.  Note also that pgbench substitutes variables
				-- inside SQL comments, so naming one here with its colon
				-- would create a parameter of its own.
				format('cached plan read %s, not zero', s))
				FROM (SELECT pgb_cached_sum() AS s) x;
		),
};

# CONCURRENTLY on a temporary table.  These run in several
# transactions, and ON COMMIT DELETE ROWS empties the table under
# each one, so the command has to notice it is working on a table
# nobody else can see and take the ordinary path instead.
#
# Each pgbench client has its own temporary schema, so the clients do
# not collide with each other and this load fits any scenario.
load temp_table_cic => {
		weight => 1,
		# This is a workload client that runs index DDL, and the
		# cancellation environment picks its victims by matching the
		# query text -- so it would terminate this one and the run would
		# fail on a writer that died, which is not what that environment
		# is testing.
		conflicts => { env => ['cancellation'] },
		script => q(
			-- The table survives the whole session, so every transaction
			-- after the first would report it already exists, and the run
			-- insists on an empty stderr.
			SET client_min_messages = warning;
			CREATE TEMP TABLE IF NOT EXISTS pgb_tmp(i int)
				ON COMMIT DELETE ROWS;
			INSERT INTO pgb_tmp SELECT g FROM generate_series(1, 100) g;
			DROP INDEX IF EXISTS pgb_tmp_idx;
			CREATE INDEX CONCURRENTLY pgb_tmp_idx ON pgb_tmp(i);
			REINDEX INDEX CONCURRENTLY pgb_tmp_idx;
		),
};

1;
