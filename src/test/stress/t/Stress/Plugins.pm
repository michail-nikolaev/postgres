
# Copyright (c) 2026, PostgreSQL Global Development Group

=pod

=head1 NAME

Stress::Plugins - the pieces a stress scenario is built from

=head1 DESCRIPTION

Everything a scenario is made of lives in one of the registries here.  A
scenario names entries from each; C<Stress::Run> assembles them into a
node, a set of pgbench scripts and a set of final checks.

Each registry is a hash of name => definition.  A definition may declare

  requires => { schema => [...] }   entries it cannot work without
  conflicts => { ... }              entries it must not be combined with

which C<Stress::Run::stress_run()> validates before anything is created,
so an impossible combination fails at once and says why, rather than
halfway through a run.

The registries are:

  %SCHEMA    what tables exist: one loader plus any decorators
  %INDEXES   what is built on them
  %LOAD      what changes the data, preserving some invariant
  %DDL       what runs concurrently with that
  %CHECK     what must hold regardless
  %ENVS      what the cluster looks like

A C<script> in a load or a check may be a string, or a sub called with
the scenario context when the values it needs are only known once the
schema exists.

=cut

package Stress::Plugins;

use strict;
use warnings FATAL => 'all';

use Exporter 'import';
use IPC::Run;
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

our @EXPORT_OK = qw(%SCHEMA %INDEXES %LOAD %DDL %CHECK %ENVS);

# Rows in the tables the decorators add.  Small enough to stay cheap,
# large enough that an index over one has more than a single page.
my $NROWS = 10_000;
my $NKEYS = 2_000;
my $NSLOTS = 2_000;

=pod

=head2 %SCHEMA

The first entry a scenario names is the loader; the rest are
decorators.  A decorator adds its own table -- and with it its own
invariant -- on top of the pgbench schema, so that a scenario is always
some specialized dimension running alongside an ordinary workload.

  init      loader only: how the base schema is created
  setup     SQL applied after the schema exists
  tables    tables the DDL rotation and checks may target
  indexes   indexes created with the decorator, in %INDEXES form
  context   sub($node) returning values the scripts need to be told

=cut

our %SCHEMA = (
	pgbench => {
		init => 'pgbench',
		# REPACK (CONCURRENTLY) decodes the table's own changes, so every
		# table it may touch needs a replica identity; pgbench_history
		# ships without one.
		setup => q(
			ALTER TABLE pgbench_history ADD COLUMN hid bigserial PRIMARY KEY;
		),
		tables => [
			qw(pgbench_accounts pgbench_tellers pgbench_branches pgbench_history)
		],
	},

	# A table whose column sum never moves, because every writer applies
	# a balanced pair of updates.  Several dimensions need an invariant
	# that is a constant rather than a relation between sums, and this is
	# it.
	ledger => {
		setup => qq(
			CREATE TABLE pgb_ledger(id int PRIMARY KEY, val int);
			INSERT INTO pgb_ledger SELECT g, g FROM generate_series(1, $NROWS) g;
		),
		tables => ['pgb_ledger'],
		indexes => [ {
			table => 'pgb_ledger',
			name => 'pgb_ledger_val_idx',
			am => 'btree',
			defn => 'ON pgb_ledger(val)',
		} ],
		context => sub {
			my ($node) = @_;
			return {
				ledger_sum =>
				  $node->safe_psql('postgres', 'SELECT SUM(val) FROM pgb_ledger'),
				ledger_rows => $NROWS,
			};
		},
	},

	# Inserts serialized behind an advisory lock, so the values a
	# sequence hands out are committed in increasing order.  At any later
	# snapshot the number of rows with val <= j must then be exactly j.
	gapless => {
		setup => q(
			CREATE TABLE pgb_gapless(id bigserial PRIMARY KEY, val bigint);
			CREATE SEQUENCE pgb_gapless_val;
		),
		tables => ['pgb_gapless'],
	},

	# Keys that are only ever upserted, never deleted, so every upsert
	# and every MERGE takes its "matched" path and the key set is fixed.
	upsert_keys => {
		setup => qq(
			CREATE TABLE pgb_keys(k int PRIMARY KEY, v int);
			INSERT INTO pgb_keys SELECT g, g FROM generate_series(1, $NKEYS) g;
		),
		tables => ['pgb_keys'],
		context => sub { return { nkeys => $NKEYS } },
	},

	# Wide values that go out of line, stored with an md5 of themselves
	# so that a torn or stale TOAST fetch is visible as a mismatch.
	toast => {
		setup => q(
			CREATE TABLE pgb_toast(id int PRIMARY KEY, payload text, h text);
			INSERT INTO pgb_toast
				SELECT g, repeat('x', 4000), md5(repeat('x', 4000))
				FROM generate_series(1, 200) g;
		),
		tables => ['pgb_toast'],
		context => sub { return { ntoast => 200 } },
	},

	# A stored generated column, which REPACK has to reproduce exactly
	# when it re-applies the changes it decoded.
	generated => {
		setup => qq(
			CREATE TABLE pgb_gen(id int PRIMARY KEY, base int,
				gen int GENERATED ALWAYS AS (base * 2 + 1) STORED,
				seq int GENERATED ALWAYS AS IDENTITY);
			INSERT INTO pgb_gen(id, base)
				SELECT g, g FROM generate_series(1, $NROWS) g;
		),
		tables => ['pgb_gen'],
		indexes => [ {
			table => 'pgb_gen',
			name => 'pgb_gen_gen_idx',
			am => 'btree',
			defn => 'ON pgb_gen(gen)',
		} ],
		context => sub { return { ngen => $NROWS } },
	},

	# A child table whose every insert and repoint fires a referential
	# integrity check against pgbench_accounts.
	fk_child => {
		setup => q(
			CREATE TABLE pgb_child(cid bigserial PRIMARY KEY,
				aid int NOT NULL REFERENCES pgbench_accounts(aid), val int);
			INSERT INTO pgb_child(aid, val)
				SELECT g, g FROM generate_series(1, 1000) g;
		),
		tables => ['pgb_child'],
		indexes => [ {
			table => 'pgb_child',
			name => 'pgb_child_aid_idx',
			am => 'btree',
			defn => 'ON pgb_child(aid)',
		} ],
	},

	# One row per slot, kept that way by an exclusion constraint.  The
	# constraint is written over a range so that it needs nothing but the
	# built-in GiST opclasses, and so that its index is built from an
	# expression.
	exclusion_slot => {
		setup => qq(
			CREATE TABLE pgb_slot(id bigserial PRIMARY KEY, slot int NOT NULL,
				CONSTRAINT pgb_slot_excl
					EXCLUDE USING gist (int4range(slot, slot + 1) WITH &&));
			INSERT INTO pgb_slot(slot) SELECT g FROM generate_series(1, $NSLOTS) g;
			CREATE FUNCTION pgb_try_slot(p_slot int) RETURNS boolean
			LANGUAGE plpgsql AS \$\$
			BEGIN
				INSERT INTO pgb_slot(slot) VALUES (p_slot);
				RETURN true;
			EXCEPTION WHEN exclusion_violation THEN
				RETURN false;
			END;
			\$\$;
		),
		tables => ['pgb_slot'],
		context => sub { return { nslots => $NSLOTS, has_exclusion => 1 } },
	},

	# Columns the non-btree access methods have opclasses for, so that
	# CREATE INDEX CONCURRENTLY and its rebuilds can be driven against
	# every AM rather than btree alone.
	am_columns => {
		setup => qq(
			CREATE TABLE pgb_am(id int PRIMARY KEY, tags text[], p point,
				n int, ip inet);
			INSERT INTO pgb_am
				SELECT g, ARRAY[md5(g::text)], point(g, g), g,
					('10.0.0.' || (g % 255))::inet
				FROM generate_series(1, $NROWS) g;
		),
		tables => ['pgb_am'],
		indexes => [
			{
				table => 'pgb_am',
				name => 'pgb_am_gin_idx',
				am => 'gin',
				defn => 'ON pgb_am USING gin (tags)',
			},
			{
				table => 'pgb_am',
				name => 'pgb_am_gist_idx',
				am => 'gist',
				defn => 'ON pgb_am USING gist (p)',
			},
			{
				table => 'pgb_am',
				name => 'pgb_am_brin_idx',
				am => 'brin',
				defn => 'ON pgb_am USING brin (n)',
			},
			{
				table => 'pgb_am',
				name => 'pgb_am_hash_idx',
				am => 'hash',
				defn => 'ON pgb_am USING hash (n)',
			},
			{
				table => 'pgb_am',
				name => 'pgb_am_spgist_idx',
				am => 'spgist',
				defn => 'ON pgb_am USING spgist (ip)',
			},
		],
		context => sub { return { nam => $NROWS } },
	},

	# A materialized view over the ledger, so REFRESH ... CONCURRENTLY
	# has something whose contents can be predicted: whatever snapshot
	# the refresh used, the ledger's sum was the same at that instant.
	matview => {
		requires => { schema => ['ledger'] },
		setup => q(
			CREATE MATERIALIZED VIEW pgb_mv AS
				SELECT id, val FROM pgb_ledger;
			CREATE UNIQUE INDEX pgb_mv_id_idx ON pgb_mv(id);
		),
	},

	# A partitioned table, with the partitions the partition DDL then
	# attaches, detaches and rebuilds indexes on.
	partitioned => {
		setup => qq(
			CREATE TABLE pgb_part(id int NOT NULL, val int) PARTITION BY RANGE (id);
			CREATE TABLE pgb_part_1 PARTITION OF pgb_part
				FOR VALUES FROM (1) TO (2501);
			CREATE TABLE pgb_part_2 PARTITION OF pgb_part
				FOR VALUES FROM (2501) TO (5001);
			CREATE TABLE pgb_part_3 PARTITION OF pgb_part
				FOR VALUES FROM (5001) TO (7501);
			CREATE TABLE pgb_part_4 PARTITION OF pgb_part
				FOR VALUES FROM (7501) TO ($NROWS + 1);
			ALTER TABLE pgb_part ADD PRIMARY KEY (id);
			INSERT INTO pgb_part SELECT g, g FROM generate_series(1, $NROWS) g;
		),
		# The parent is where the DML goes; the partitions are what the
		# CONCURRENTLY commands can be pointed at, since most of them
		# refuse a partitioned table.
		tables => [qw(pgb_part_1 pgb_part_2 pgb_part_3 pgb_part_4)],
		context => sub {
			my ($node) = @_;
			return {
				part_rows => $NROWS,
				part_sum =>
				  $node->safe_psql('postgres', 'SELECT SUM(val) FROM pgb_part'),
			};
		},
	},
);

=pod

=head2 %INDEXES

C<defn> is everything after the index name, so that both the blocking
and the concurrent form of the build can be generated from it;
C<table> is what the index is on, and C<am> decides which amcheck
function, if any, can verify it.

=cut

our %INDEXES = (
	btree_abalance => {
		table => 'pgbench_accounts',
		name => 'pgb_abalance_idx',
		am => 'btree',
		defn => 'ON pgbench_accounts(abalance)',
	},
	btree_history_delta => {
		table => 'pgbench_history',
		name => 'pgb_history_delta_idx',
		am => 'btree',
		defn => 'ON pgbench_history(delta)',
	},
	partial_abalance => {
		table => 'pgbench_accounts',
		name => 'pgb_abalance_partial_idx',
		am => 'btree',
		# REPACK and CLUSTER refuse to order a table by a partial index.
		partial => 1,
		defn => 'ON pgbench_accounts(abalance) WHERE abalance > 0',
	},
	expr_abalance => {
		table => 'pgbench_accounts',
		name => 'pgb_abalance_expr_idx',
		am => 'btree',
		defn => 'ON pgbench_accounts(abs(abalance), aid)',
	},
	covering_aid => {
		table => 'pgbench_accounts',
		name => 'pgb_aid_covering_idx',
		am => 'btree',
		defn => 'ON pgbench_accounts(aid) INCLUDE (abalance)',
	},
);

=pod

=head2 %LOAD

A load is a pgbench script that changes data while preserving the
invariant its scenario checks.  C<weight> is its share of the
transaction mix, and C<setup> is any SQL it needs beforehand.

=cut

our %LOAD = (
	# The standard TPC-B-like transaction: one delta applied to an
	# account, a teller and a branch, and recorded in history.  Every
	# completed transaction moves all four sums by the same amount, which
	# is what the 'balances' check relies on.
	tpcb_like => {
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
	},

	# The same movement, but with the rows taken with FOR UPDATE first
	# and the lock held across a pause, so a CONCURRENTLY command
	# routinely runs while a row lock is in force.  The rows are locked
	# in a fixed order across the three tables, so concurrent clients
	# cannot deadlock.
	row_lock => {
		weight => 1,
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
	},

	# A balanced pair of updates on the ledger: one +diff, one -diff in
	# the same transaction, in id order so that concurrent writers cannot
	# deadlock.  The sum is therefore the same at every commit.
	balanced_pair => {
		weight => 3,
		requires => { schema => ['ledger'] },
		script => q(
			\set a random(1, :ledger_rows)
			\set b random(1, :ledger_rows)
			\set lo least(:a, :b)
			\set hi greatest(:a, :b)
			\set diff random(1, 10000)
			BEGIN;
			UPDATE pgb_ledger SET val = val + :diff WHERE id = :lo;
			\sleep 1 ms
			UPDATE pgb_ledger SET val = val - :diff WHERE id = :hi;
			COMMIT;
		),
	},

	# The same, through PREPARE TRANSACTION and COMMIT PREPARED (or,
	# sometimes, ROLLBACK PREPARED): either way the transaction is
	# internally balanced, and the CONCURRENTLY commands have to cope
	# with transactions that are prepared but not yet resolved.
	twophase => {
		weight => 1,
		requires => { schema => ['ledger'] },
		conf => ['max_prepared_transactions = 100'],
		script => q(
			\set a random(1, :ledger_rows)
			\set b random(1, :ledger_rows)
			\set lo least(:a, :b)
			\set hi greatest(:a, :b)
			\set diff random(1, 10000)
			\set abort random(0, 4)
			BEGIN;
			UPDATE pgb_ledger SET val = val + :diff WHERE id = :lo;
			UPDATE pgb_ledger SET val = val - :diff WHERE id = :hi;
			PREPARE TRANSACTION 'stress_:client_id';
			\sleep 2 ms
			\if :abort = 0
				ROLLBACK PREPARED 'stress_:client_id';
			\else
				COMMIT PREPARED 'stress_:client_id';
			\endif
		),
	},

	# Batches rather than single rows: every batch holds as many +1 rows
	# as -1 rows, so the sum is zero at every commit no matter which
	# batches happen to be present.  This drives the multi-insert and
	# COPY paths and the bulk index insertions they cause.
	bulk_copy => {
		weight => 1,
		setup => q(
			CREATE TABLE pgb_bulk(id bigserial PRIMARY KEY, batch int, val int);
			CREATE INDEX pgb_bulk_batch_idx ON pgb_bulk(batch);
		),
		# pgbench has no way to feed COPY from its own script, so the
		# batch lives in a file the server reads.  Batch 0 is reserved
		# for it, and it sums to zero like every other batch.
		files => {
			'copy_batch.txt' =>
			  join('', map { "0\t" . ($_ % 2 == 0 ? 1 : -1) . "\n" } (1 .. 200)),
		},
		script => sub {
			my ($ctx) = @_;
			my $copyfile = $ctx->{files}->{'copy_batch.txt'};
			return qq(
			\\set batch random(1, 50)
			\\set mode random(0, 2)
			\\if :mode = 0
				BEGIN;
				INSERT INTO pgb_bulk(batch, val)
					SELECT :batch, CASE WHEN g % 2 = 0 THEN 1 ELSE -1 END
					FROM generate_series(1, 200) g;
				COMMIT;
			\\elif :mode = 1
				COPY pgb_bulk(batch, val) FROM '$copyfile';
			\\else
				DELETE FROM pgb_bulk WHERE batch = :batch;
			\\endif
			);
		},
	},

	# Every key exists and none is ever deleted, so an upsert and a MERGE
	# both take their "matched" path.  A rebuild that missed a row being
	# upserted at that moment would put a second row under the same key.
	upsert_merge => {
		weight => 3,
		requires => { schema => ['upsert_keys'] },
		script => q(
			\set k random(1, :nkeys)
			\set v random(1, 100000)
			\set mode random(0, 1)
			\if :mode = 0
				INSERT INTO pgb_keys VALUES (:k, :v)
					ON CONFLICT (k) DO UPDATE SET v = EXCLUDED.v;
			\else
				MERGE INTO pgb_keys t USING (SELECT :k AS k, :v AS v) s
					ON t.k = s.k
					WHEN MATCHED THEN UPDATE SET v = s.v
					WHEN NOT MATCHED THEN INSERT VALUES (s.k, s.v);
			\endif
		),
	},

	# Inserts serialized behind an advisory lock, so that commit order
	# matches the order the sequence handed the values out.
	serial_insert => {
		weight => 3,
		requires => { schema => ['gapless'] },
		script => q(
			BEGIN;
			SELECT pg_advisory_xact_lock(7);
			INSERT INTO pgb_gapless(val) VALUES (nextval('pgb_gapless_val'));
			COMMIT;
		),
	},

	# Wide values rewritten together with their md5, in one statement, so
	# that every row satisfies md5(payload) = h at every commit.
	toast_rewrite => {
		weight => 2,
		requires => { schema => ['toast'] },
		script => q(
			\set id random(1, :ntoast)
			\set len random(2000, 8000)
			-- The payload is built once in the subquery and used for both
			-- columns; computing it twice would give two different values
			-- and a mismatch that is the test's fault, not the server's.
			UPDATE pgb_toast SET payload = s.p, h = md5(s.p)
				FROM (SELECT repeat(md5(random()::text), :len / 32) AS p) s
				WHERE id = :id;
		),
	},

	# Updates of the column a stored generated column is computed from.
	generated_update => {
		weight => 2,
		requires => { schema => ['generated'] },
		script => q(
			\set id random(1, :ngen)
			\set base random(1, 1000000)
			UPDATE pgb_gen SET base = :base WHERE id = :id;
		),
	},

	# Inserts, deletes and repointings of child rows, each of which fires
	# a foreign key check against the parent.
	fk_churn => {
		weight => 2,
		requires => { schema => ['fk_child'] },
		script => q(
			\set aid random(1, :naccounts)
			\set mode random(0, 2)
			\if :mode = 0
				INSERT INTO pgb_child(aid, val) VALUES (:aid, :aid);
			\elif :mode = 1
				UPDATE pgb_child SET aid = :aid
					WHERE cid = (SELECT cid FROM pgb_child ORDER BY cid DESC LIMIT 1);
			\else
				DELETE FROM pgb_child WHERE cid =
					(SELECT cid FROM pgb_child ORDER BY cid DESC LIMIT 1)
					AND (SELECT COUNT(*) FROM pgb_child) > 1000;
			\endif
		),
	},

	# Duplicate slots, which must always be rejected, and a slot freed
	# and taken again in one transaction, which must end up occupied
	# exactly once.
	exclusion_churn => {
		weight => 2,
		requires => { schema => ['exclusion_slot'] },
		script => q(
			\set slot random(1, :nslots)
			\set mode random(0, 1)
			\if :mode = 0
				SELECT stress_assert(NOT pgb_try_slot(:slot),
					format('duplicate slot %s was accepted', :slot));
			\else
				BEGIN;
				DELETE FROM pgb_slot WHERE slot = :slot;
				\sleep 1 ms
				SELECT pgb_try_slot(:slot);
				COMMIT;
			\endif
		),
	},

	# Savepoints, and a PL/pgSQL loop whose body is an exception block --
	# a subtransaction per iteration, enough of them to overflow the
	# backend's subxid cache while a CONCURRENTLY command waits on it.
	subxact_churn => {
		weight => 2,
		requires => { schema => ['ledger'] },
		setup => q(
			CREATE FUNCTION pgb_subxact_churn(lo int, hi int, diff int, n int)
			RETURNS void LANGUAGE plpgsql AS $$
			DECLARE
				i int;
			BEGIN
				FOR i IN 1 .. n LOOP
					BEGIN
						UPDATE pgb_ledger SET val = val + diff WHERE id = lo;
						UPDATE pgb_ledger SET val = val - diff WHERE id = hi;
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
			\set a random(1, :ledger_rows)
			\set b random(1, :ledger_rows)
			\set lo least(:a, :b)
			\set hi greatest(:a, :b)
			\set diff random(1, 10000)
			\set mode random(0, 1)
			\if :mode = 0
				BEGIN;
				SAVEPOINT sp1;
				UPDATE pgb_ledger SET val = val + :diff WHERE id = :lo;
				ROLLBACK TO SAVEPOINT sp1;
				UPDATE pgb_ledger SET val = val + :diff WHERE id = :lo;
				UPDATE pgb_ledger SET val = val - :diff WHERE id = :hi;
				COMMIT;
			\else
				SELECT pgb_subxact_churn(:lo, :hi, :diff, 80);
			\endif
		),
	},

	# A cursor held open across a pause, driven from PL/pgSQL so the
	# whole scan-with-a-pause happens inside one call.  What the cursor
	# reads must stay readable and consistent while the indexes under it
	# are rebuilt.
	cursor_hold => {
		weight => 1,
		requires => { schema => ['ledger'] },
		setup => q(
			CREATE FUNCTION pgb_cursor_sum(expected bigint) RETURNS void
			LANGUAGE plpgsql AS $$
			DECLARE
				c CURSOR FOR SELECT val FROM pgb_ledger;
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
			SELECT pgb_cursor_sum(:ledger_sum);
		),
	},

	# The same reads through a PL/pgSQL function, whose plans are cached
	# in its own plan cache across calls.  Combined with the scenario's
	# prepared protocol and force_generic_plan, this keeps plans alive
	# across the DDL that must invalidate them.
	plancache => {
		weight => 2,
		requires => { schema => ['ledger'] },
		setup => q(
			CREATE FUNCTION pgb_cached_sum() RETURNS bigint
			LANGUAGE plpgsql AS $$
			DECLARE
				total bigint;
			BEGIN
				SELECT COALESCE(SUM(val), 0) INTO total FROM pgb_ledger;
				RETURN total;
			END;
			$$;
		),
		script => q(
			\set a random(1, :ledger_rows)
			\set b random(1, :ledger_rows)
			\set lo least(:a, :b)
			\set hi greatest(:a, :b)
			\set diff random(1, 10000)
			BEGIN;
			UPDATE pgb_ledger SET val = val + :diff WHERE id = :lo;
			UPDATE pgb_ledger SET val = val - :diff WHERE id = :hi;
			COMMIT;
			SELECT stress_assert(s = 0 OR s = :ledger_sum,
				-- The cast matters under the prepared protocol, where
				-- the variable below becomes a query parameter and
				-- format() gives the planner nothing to infer its type
				-- from.  Note also that pgbench substitutes variables
				-- inside SQL comments, so naming one here with its colon
				-- would create a parameter of its own.
				format('cached plan read %s, not %s', s, :ledger_sum::bigint))
				FROM (SELECT pgb_cached_sum() AS s) x;
		),
	},

	# Upserts over every column the access methods index, so each of them
	# has insertions to absorb while it is being rebuilt.
	am_churn => {
		weight => 2,
		requires => { schema => ['am_columns'] },
		script => q(
			\set id random(1, :nam)
			\set n random(1, 1000000)
			UPDATE pgb_am SET tags = ARRAY[md5(random()::text)],
				p = point(:n, :n), n = :n,
				ip = ('10.0.0.' || (:n % 255))::inet
				WHERE id = :id;
		),
	},

	# DML through the partitioned parent, routed across all partitions,
	# with both rows of the pair in the same partition so that each
	# partition's sum is invariant on its own.
	#
	# Both rows are moved by a single statement on purpose.  A partition
	# can be detached between two statements of one transaction, and then
	# the second would match nothing and leave the pair unbalanced --
	# which is a property of the test, not a bug in the server.  One
	# statement sees one partition descriptor throughout.
	partition_dml => {
		weight => 3,
		requires => { schema => ['partitioned'] },
		script => q(
			\set part random(0, 3)
			\set a random(1, 2500)
			\set b random(1, 2500)
			\set lo least(:a, :b) + :part * 2500
			\set hi greatest(:a, :b) + :part * 2500
			\set diff random(1, 10000)
			UPDATE pgb_part SET val = val
					+ CASE WHEN id = :lo THEN (:diff) ELSE 0 END
					+ CASE WHEN id = :hi THEN -(:diff) ELSE 0 END
				WHERE id IN (:lo, :hi);
		),
	},
);

=pod

=head2 %DDL

C<variants> returns the alternatives the DDL client picks between, one
per invocation; each is an arrayref of statements that run together.  It
is called with the scenario context, so an entry expands itself over
whatever tables and indexes the scenario actually has.

=cut

our %DDL = (
	repack_concurrently => {
		variants => sub {
			my ($ctx) = @_;
			return map { { table => $_, stmts => ["REPACK (CONCURRENTLY) $_;"] } }
			  @{ $ctx->{tables} };
		},
	},

	repack_using_index => {
		variants => sub {
			my ($ctx) = @_;
			return map {
				{
					table => $_->{table},
					stmts =>
					  ["REPACK (CONCURRENTLY) $_->{table} USING INDEX $_->{name};"]
				}
			} grep { $_->{am} eq 'btree' && !$_->{partial} } @{ $ctx->{indexes} };
		},
	},

	reindex_table_concurrently => {
		variants => sub {
			my ($ctx) = @_;
			# REINDEX will not rebuild an exclusion constraint's index
			# concurrently and says so with a WARNING.  That is expected
			# here -- it still rebuilds every other index of the table --
			# but it would land on pgbench's stderr, where the run insists
			# on silence, so keep it quiet where such a constraint exists.
			my @quiet =
			  $ctx->{has_exclusion}
			  ? ('SET client_min_messages = error;')
			  : ();
			my @restore =
			  $ctx->{has_exclusion} ? ('RESET client_min_messages;') : ();
			return map {
				{
					table => $_,
					stmts => [ @quiet, "REINDEX TABLE CONCURRENTLY $_;", @restore ]
				}
			} @{ $ctx->{tables} };
		},
	},

	reindex_index_concurrently => {
		variants => sub {
			my ($ctx) = @_;
			return map {
				{
					table => $_->{table},
					stmts => ["REINDEX INDEX CONCURRENTLY $_->{name};"]
				}
			} @{ $ctx->{indexes} };
		},
	},

	drop_create_index => {
		variants => sub {
			my ($ctx) = @_;
			return map {
				{
					table => $_->{table},
					stmts => [
						"DROP INDEX CONCURRENTLY $_->{name};",
						"CREATE INDEX CONCURRENTLY $_->{name} $_->{defn};"
					]
				}
			} @{ $ctx->{indexes} };
		},
	},

	# VACUUM's index cleanup and freezing have to coexist with the
	# concurrent rebuilds and the tuple movement.
	vacuum => {
		variants => sub {
			my ($ctx) = @_;
			return map {
				{ table => $_, stmts => ["VACUUM $_;"] },
				  { table => $_, stmts => ["VACUUM (FREEZE) $_;"] },
				  { table => $_, stmts => ["VACUUM (ANALYZE) $_;"] }
			} @{ $ctx->{tables} };
		},
	},

	# A rewriting ALTER TABLE takes AccessExclusiveLock and gives the
	# table a new relfilenode with every index rebuilt, so a REINDEX or
	# REPACK that was waiting resumes against a table of a new shape.
	alter_table_rewrite => {
		variants => sub {
			my ($ctx) = @_;
			return (
				{
					table => 'pgbench_accounts',
					stmts => [
						'ALTER TABLE pgbench_accounts ALTER COLUMN abalance TYPE bigint;',
						'ALTER TABLE pgbench_accounts ALTER COLUMN abalance TYPE int;'
					]
				},
				{
					table => 'pgbench_tellers',
					stmts => [
						'ALTER TABLE pgbench_tellers ADD COLUMN pad text DEFAULT random()::text;',
						'ALTER TABLE pgbench_tellers DROP COLUMN pad;'
					]
				});
		},
	},

	refresh_matview_concurrently => {
		requires => { schema => ['matview'] },
		variants => sub {
			return ({
				table => 'pgb_mv',
				stmts => ['REFRESH MATERIALIZED VIEW CONCURRENTLY pgb_mv;']
			});
		},
	},

	# Detaching a partition concurrently leaves behind a CHECK constraint
	# matching the bound, so the re-attach needs no validation scan.
	detach_partition_concurrently => {
		requires => { schema => ['partitioned'] },
		variants => sub {
			my ($ctx) = @_;
			my @v;
			my @bounds = (
				[ 1, 2501 ], [ 2501, 5001 ], [ 5001, 7501 ],
				[ 7501, $NROWS + 1 ]);
			for my $i (0 .. 3)
			{
				my $p = 'pgb_part_' . ($i + 1);
				push @v,
				  {
					table => 'pgb_part',
					stmts => [
						"ALTER TABLE pgb_part DETACH PARTITION $p CONCURRENTLY;",
						'\sleep 10 ms',
						"ALTER TABLE pgb_part ATTACH PARTITION $p "
						  . "FOR VALUES FROM ($bounds[$i][0]) TO ($bounds[$i][1]);"
					]
				  };
			}
			return @v;
		},
	},

	# CREATE INDEX CONCURRENTLY refuses a partitioned table; the
	# documented way to build one without blocking writes is to create it
	# on ONLY the parent, build a matching index on every partition, and
	# attach them one by one, at which point the parent index becomes
	# valid.
	partitionwise_index_build => {
		requires => { schema => ['partitioned'] },
		variants => sub {
			my ($ctx) = @_;
			my @stmts = ('CREATE INDEX pgb_part_val_idx ON ONLY pgb_part(val);');
			for my $i (1 .. 4)
			{
				push @stmts,
				  "CREATE INDEX CONCURRENTLY pgb_part_${i}_val_idx "
				  . "ON pgb_part_$i(val);",
				  "ALTER INDEX pgb_part_val_idx ATTACH PARTITION pgb_part_${i}_val_idx;";
			}
			push @stmts, '\sleep 10 ms', 'DROP INDEX pgb_part_val_idx;';
			return ({ table => 'pgb_part', stmts => [@stmts] });
		},
	},
);

=pod

=head2 %CHECK

C<script> is a pgbench fragment run as its own weighted script;
C<final> is a sub run against the node once the workload is over.
Either may be omitted.

=cut

our %CHECK = (
	# The four sums move together or not at all.  Read in one statement,
	# so they share a snapshot; the counts are only fetched when they
	# disagree, since counting every account is far too expensive to do
	# on every check.
	balances => {
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
							:bal_a, :bal_t, :bal_b, :bal_h));
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
	},

	# The ledger's sum never moves.
	ledger_sum => {
		weight => 1,
		requires => { schema => ['ledger'] },
		script => q(
			SELECT stress_assert(cnt = 0
					OR (cnt = :ledger_rows AND sum = :ledger_sum),
				format('ledger has %s rows summing to %s', cnt, sum))
			FROM (SELECT COUNT(*) AS cnt, COALESCE(SUM(val), 0) AS sum
				FROM pgb_ledger) x;
		),
		final => sub {
			my ($node, $ctx) = @_;
			Test::More::is(
				$node->safe_psql('postgres', 'SELECT SUM(val) FROM pgb_ledger'),
				$ctx->{ledger_sum}, 'ledger sum is unchanged');
		},
	},

	# Once the row with val = j is committed, exactly j rows have
	# val <= j -- the sequence was handed out under a lock, so commit
	# order matches value order.
	gapless_count => {
		weight => 1,
		requires => { schema => ['gapless'] },
		script => q(
			SELECT COALESCE(MAX(val), 0) AS j FROM pgb_gapless \gset g_
			\if :g_j > 0
				SELECT stress_assert(cnt = :g_j,
					format('%s rows with val <= %s, not %s', cnt, :g_j, :g_j))
				FROM (SELECT COUNT(*) AS cnt FROM pgb_gapless
					WHERE val <= :g_j) x;
			\endif
		),
	},

	# Nothing may ever hold two rows under one key.
	distinct_keys => {
		weight => 1,
		requires => { schema => ['upsert_keys'] },
		script => q(
			SELECT stress_assert(cnt = 0 OR (cnt = :nkeys AND keys = :nkeys),
				format('pgb_keys has %s rows under %s keys', cnt, keys))
			FROM (SELECT COUNT(*) AS cnt, COUNT(DISTINCT k) AS keys
				FROM pgb_keys) x;
		),
		final => sub {
			my ($node, $ctx) = @_;
			Test::More::is(
				$node->safe_psql('postgres',
					'SELECT COUNT(*) - COUNT(DISTINCT k) FROM pgb_keys'),
				'0', 'no duplicate key got past the unique index');
		},
	},

	# Every row's out-of-line value still matches the md5 stored with it.
	toast_md5 => {
		weight => 1,
		requires => { schema => ['toast'] },
		script => q(
			SELECT stress_assert(bad = 0,
				format('%s rows whose payload does not match its md5', bad))
			FROM (SELECT COUNT(*) AS bad FROM pgb_toast
				WHERE md5(payload) <> h) x;
		),
		final => sub {
			my ($node, $ctx) = @_;
			Test::More::is(
				$node->safe_psql('postgres',
					'SELECT COUNT(*) FROM pgb_toast WHERE md5(payload) <> h'),
				'0', 'every TOASTed payload matches its md5');
		},
	},

	# The stored generated column is a fixed function of its base column,
	# however the table has been rewritten.
	generated_matches => {
		weight => 1,
		requires => { schema => ['generated'] },
		script => q(
			SELECT stress_assert(bad = 0,
				format('%s rows whose generated column does not match', bad))
			FROM (SELECT COUNT(*) AS bad FROM pgb_gen
				WHERE gen <> base * 2 + 1) x;
		),
		final => sub {
			my ($node, $ctx) = @_;
			Test::More::is(
				$node->safe_psql('postgres',
					'SELECT COUNT(*) FROM pgb_gen WHERE gen <> base * 2 + 1'),
				'0', 'every generated value matches its base column');
		},
	},

	# No child row may reference a parent that is not there.
	no_orphans => {
		weight => 1,
		requires => { schema => ['fk_child'] },
		script => q(
			SELECT stress_assert(orphans = 0,
				format('%s child rows reference a missing parent', orphans))
			FROM (SELECT COUNT(*) AS orphans FROM pgb_child c
				WHERE NOT EXISTS (SELECT 1 FROM pgbench_accounts a
					WHERE a.aid = c.aid)) x;
		),
		final => sub {
			my ($node, $ctx) = @_;
			Test::More::is(
				$node->safe_psql(
					'postgres', q(
					SELECT COUNT(*) FROM pgb_child c WHERE NOT EXISTS
						(SELECT 1 FROM pgbench_accounts a WHERE a.aid = c.aid))),
				'0', 'no orphan child rows');
		},
	},

	# One row per slot, and never two.
	distinct_slots => {
		weight => 1,
		requires => { schema => ['exclusion_slot'] },
		script => q(
			SELECT stress_assert(cnt = 0 OR (cnt = :nslots AND slots = :nslots),
				format('pgb_slot has %s rows over %s slots', cnt, slots))
			FROM (SELECT COUNT(*) AS cnt, COUNT(DISTINCT slot) AS slots
				FROM pgb_slot) x;
		),
		final => sub {
			my ($node, $ctx) = @_;
			Test::More::is(
				$node->safe_psql('postgres',
					'SELECT COUNT(*) - COUNT(DISTINCT slot) FROM pgb_slot'),
				'0', 'no duplicate slot got past the exclusion constraint');
		},
	},

	# The materialized view holds the ledger as of some snapshot, and the
	# ledger's sum was the same at every snapshot.
	matview_matches => {
		weight => 1,
		requires => { schema => ['matview'] },
		script => q(
			SELECT stress_assert(cnt = 0
					OR (cnt = :ledger_rows AND sum = :ledger_sum),
				format('matview has %s rows summing to %s', cnt, sum))
			FROM (SELECT COUNT(*) AS cnt, COALESCE(SUM(val), 0) AS sum
				FROM pgb_mv) x;
		),
	},

	# Each partition's sum is invariant on its own, whether it is
	# currently attached or not, and the parent must stay queryable while
	# the partition descriptor changes underneath it.
	partition_sum => {
		weight => 1,
		requires => { schema => ['partitioned'] },
		script => q(
			SELECT COALESCE(SUM(val), 0) AS s, COUNT(*) AS c FROM pgb_part \gset p_
			\if :p_c = :part_rows
				SELECT stress_assert(:p_s = :part_sum,
					format('partitioned sum is %s, not %s', :p_s, :part_sum));
			\endif
		),
	},

	# An index scan and a sequential scan of the same predicate, in one
	# snapshot, must return the same thing.
	index_vs_seq => {
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
			SELECT stress_assert(:idx_cnt = :seq_cnt AND :idx_sum = :seq_sum,
				format('index scan (%s rows, sum %s) disagrees with seq scan (%s rows, sum %s)',
					:idx_cnt, :idx_sum, :seq_cnt, :seq_sum));
		),
	},

	# An index-only scan trusts the visibility map, so a wrong VM bit is
	# a silent wrong answer rather than an error.  Compare it against a
	# sequential scan in one snapshot.
	ios_vs_seq => {
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
			SELECT stress_assert(:ios_cnt = :seqio_cnt AND :ios_sum = :seqio_sum,
				format('index-only scan (%s rows, sum %s) disagrees with seq scan (%s rows, sum %s)',
					:ios_cnt, :ios_sum, :seqio_cnt, :seqio_sum));
		),
	},

	# Nothing may change under a held row lock: the second read runs in a
	# fresh snapshot, so it would see any concurrent commit.
	row_lock_durability => {
		weight => 1,
		requires => { schema => ['ledger'] },
		script => q(
			\set lo random(1, :ledger_rows - 4)
			BEGIN;
			SELECT COUNT(*) AS cnt, COALESCE(SUM(val), 0) AS sum FROM
				(SELECT val FROM pgb_ledger WHERE id BETWEEN :lo AND :lo + 4
					ORDER BY id FOR UPDATE) s \gset locked_
			\sleep 20 ms
			SELECT COUNT(*) AS cnt, COALESCE(SUM(val), 0) AS sum
				FROM pgb_ledger WHERE id BETWEEN :lo AND :lo + 4 \gset reread_
			COMMIT;
			SELECT stress_assert(:reread_cnt = 0
					OR (:locked_cnt = :reread_cnt AND :locked_sum = :reread_sum),
				format('rows changed under a held lock: locked (%s rows, sum %s), re-read (%s rows, sum %s)',
					:locked_cnt, :locked_sum, :reread_cnt, :reread_sum));
		),
	},

	# Every index the scenario built must still be a valid index.
	amcheck => {
		final => sub {
			my ($node, $ctx) = @_;
			foreach my $idx (@{ $ctx->{indexes} })
			{
				# amcheck covers btree and GIN; the other access methods
				# are exercised by being built and rebuilt rather than
				# verified afterwards.
				if ($idx->{am} eq 'btree')
				{
					$node->safe_psql('postgres',
						"SELECT bt_index_parent_check('$idx->{name}', heapallindexed => true)"
					);
				}
				elsif ($idx->{am} eq 'gin')
				{
					$node->safe_psql('postgres',
						"SELECT gin_index_check('$idx->{name}')");
				}
			}
			Test::More::pass('indexes pass amcheck');
		},
	},

	# The visibility map must describe the table it ended up with.
	visibility_map => {
		final => sub {
			my ($node, $ctx) = @_;
			$node->safe_psql('postgres', 'CREATE EXTENSION IF NOT EXISTS pg_visibility');
			my $bad = $node->safe_psql(
				'postgres', q(
				SELECT (SELECT COUNT(*) FROM pg_check_visible('pgbench_accounts'))
					+ (SELECT COUNT(*) FROM pg_check_frozen('pgbench_accounts'))));
			Test::More::is($bad, '0', 'the visibility map matches the heap');
		},
	},

	# A cancelled or completed REPACK must not leave its transient slot
	# behind, and logical decoding must have been switched off again.
	no_slot_leak => {
		final => sub {
			my ($node, $ctx) = @_;
			Test::More::is(
				$node->safe_psql(
					'postgres', 'SELECT COUNT(*) FROM pg_replication_slots'),
				'0', 'no replication slot leaked');
		},
	},
);

=pod

=head2 %ENVS

An environment decides what cluster the scenario runs against, and
carries the settings that make that cluster behave.  Getting those wrong
is itself a source of false failures, so they belong here rather than in
each scenario.

=cut

our %ENVS = (
	standalone => {
		conf => ['wal_level = logical'],
	},

	# wal_level = replica, so that the transient slot REPACK takes really
	# does toggle logical decoding on and off.
	wal_replica => {
		conf => [ 'wal_level = replica', 'max_connections = 50' ],
	},

	# Aggressive autovacuum, so the visibility map is being set
	# continuously rather than once at the start.
	autovacuum => {
		conf => [
			'wal_level = logical',
			'autovacuum_naptime = 1s',
			'autovacuum_vacuum_scale_factor = 0.0',
			'autovacuum_vacuum_threshold = 100',
			'autovacuum_vacuum_insert_scale_factor = 0.0',
			'autovacuum_vacuum_insert_threshold = 100',
		],
	},

	# A deliberately small lock table, which the CONCURRENTLY commands
	# are heavy users of.
	lock_exhaustion => {
		conf => [
			'wal_level = logical',
			'max_locks_per_transaction = 16',
			'max_connections = 50',
		],
	},

	# The cluster is killed and restarted while the commands are in
	# flight, so their cleanup happens through crash recovery rather than
	# through their own code.
	crash_loop => {
		conf => ['wal_level = logical'],
		run => sub {
			my ($node, $ctx) = @_;

			foreach my $cycle (1 .. 3)
			{
				my ($out, $err) = ('', '');
				# Long enough that the kill lands mid-workload; the run is
				# ended by the kill, not by the clock.
				my $h = IPC::Run::start(
					$ctx->{pgbench_cmd}->(duration => 60),
					'>', \$out, '2>', \$err);
				sleep(2);
				$node->kill9;

				# pgbench cannot help but fail when the server disappears
				# under it, so its exit status says nothing here.
				eval { IPC::Run::finish($h) };

				# The children of a SIGKILLed postmaster take a moment to
				# notice and let go of shared memory; until they have, a
				# new postmaster refuses to start.
				my $started = 0;
				foreach my $try (1 .. 20)
				{
					last if $started = $node->start(fail_ok => 1);
					sleep 1;
				}
				die 'the server did not come back after the crash'
				  unless $started;
				Test::More::pass("cycle $cycle: recovered after a crash");

				# An interrupted concurrent build may leave an invalid
				# index behind, which is documented; it must at least be
				# droppable.
				$node->safe_psql(
					'postgres', q(
					DO $$
					DECLARE
						idx oid;
					BEGIN
						FOR idx IN SELECT indexrelid FROM pg_index
							WHERE NOT indisvalid
						LOOP
							CONTINUE WHEN NOT EXISTS
								(SELECT 1 FROM pg_class WHERE oid = idx);
							EXECUTE format('DROP INDEX %s', idx::regclass);
						END LOOP;
					END;
					$$;
				));
			}
			return;
		},
	},

	# The commands are interrupted partway rather than allowed to
	# finish, which exercises their own cleanup paths.
	cancellation => {
		conf => [ 'wal_level = replica', 'max_connections = 50' ],
		run => sub {
			my ($node, $ctx) = @_;

			# The workload runs as usual, minus the DDL script: the
			# commands are issued from here instead, so that their errors
			# can be tolerated.
			my ($out, $err) = ('', '');
			my $h = IPC::Run::start(
				$ctx->{pgbench_cmd}->(files => $ctx->{noddl_opts}),
				'>', \$out, '2>', \$err);

			my @variants = @{ $ctx->{ddl_variants} };
			my ($attempts, $interrupted) = (0, 0);
			my $deadline = time() + $ctx->{duration};
			while (time() < $deadline)
			{
				my $v = $variants[ int(rand(scalar @variants)) ];
				# pgbench meta-commands are not SQL; skip a variant that
				# is only a pause.
				my @stmts = grep { !/^\\/ } @{ $v->{stmts} };
				next unless @stmts;

				# Mostly cancel at some arbitrary point, sometimes let the
				# command run to completion.
				my $timeout = (int(rand(4)) == 0) ? 0 : 1 + int(rand(200));
				my (undef, undef, $stderr) = $node->psql(
					'postgres',
					"SET statement_timeout = $timeout; " . join(' ', @stmts),
					on_error_stop => 0);
				$attempts++;

				next if $stderr eq '';
				$interrupted++;
				# The only errors expected are the cancellation itself and
				# the complaints that follow from a previous one having
				# left the indexes in an unexpected state.
				# Written on one line on purpose: under /x the spaces
				# inside these messages would be ignored and none of them
				# would ever match.
				Test::More::like(
					$stderr,
					qr/canceling statement due to statement timeout|(?:relation|index) "[^"]+" (?:already exists|does not exist)|skipping reindex of invalid index|cannot cluster on (?:invalid|partial) index|deadlock detected/,
					'interrupted command failed only in expected ways')
				  or Test::More::diag("unexpected error: $stderr");
			}

			IPC::Run::finish($h);
			Test::More::like($out, qr{actually processed}, 'writers completed');
			Test::More::like($err, $ctx->{stderr_re}, 'writers reported nothing');
			Test::More::note(
				"$attempts commands issued, $interrupted of them interrupted");
			Test::More::cmp_ok($interrupted, '>', 0, 'some were interrupted');

			# Whatever was cut off, nothing may be left half-built.
			$node->safe_psql(
				'postgres', q(
				DO $$
				DECLARE
					idx oid;
				BEGIN
					FOR idx IN SELECT indexrelid FROM pg_index WHERE NOT indisvalid
					LOOP
						CONTINUE WHEN NOT EXISTS
							(SELECT 1 FROM pg_class WHERE oid = idx);
						EXECUTE format('DROP INDEX %s', idx::regclass);
					END LOOP;
				END;
				$$;
			));
			return;
		},
		final => sub {
			my ($node, $ctx) = @_;
			# A cancelled REPACK must not leave logical decoding switched
			# on behind it.
			$node->poll_query_until('postgres',
				q(SELECT current_setting('effective_wal_level') = 'replica'))
			  or die 'timed out waiting for logical decoding to be disabled';
			Test::More::pass('effective_wal_level fell back to replica');
			return;
		},
	},

	# A hot standby replaying the DDL while serving the checks.
	standby => {
		init => { allows_streaming => 1 },
		conf => ['max_connections = 50'],
		setup => sub {
			my ($primary, $ctx) = @_;

			$primary->backup('stress_bkp');
			my $standby = PostgreSQL::Test::Cluster->new('stress_standby');
			$standby->init_from_backup($primary, 'stress_bkp',
				has_streaming => 1);

			# A finite delay, never -1: replay takes the
			# AccessExclusiveLocks the primary logged before it applies
			# the records that conflict with a reader's snapshot, so with
			# -1 it can wait forever on a reader that is itself blocked on
			# a lock replay holds.  Nothing detects that cycle.  A finite
			# delay lets replay cancel the reader instead, which is the
			# documented way out.
			$standby->append_conf('postgresql.conf',
				'max_standby_streaming_delay = 5s');
			$standby->append_conf('postgresql.conf',
				'log_recovery_conflict_waits = on');
			$standby->append_conf('postgresql.conf', 'log_lock_waits = on');
			$standby->start;

			$ctx->{standby} = $standby;
			push @{ $ctx->{extra_nodes} }, $standby;
			return;
		},
		run => sub {
			my ($primary, $ctx) = @_;

			# The primary runs the whole mix; the standby runs the checks
			# alone, since it cannot write.  A query cancelled by a
			# recovery conflict fails with a serialization error, which is
			# what pgbench retries for; without that the first
			# cancellation would end the run.
			my $pri_cmd = $ctx->{pgbench_cmd}->();
			my $sby_cmd = $ctx->{pgbench_cmd}->(
				node => $ctx->{standby},
				files => $ctx->{check_opts},
				clients => 10,
				args => '--max-tries=100');

			my ($po, $pe, $so, $se) = ('', '', '', '');
			my $ph = IPC::Run::start($pri_cmd, '>', \$po, '2>', \$pe);
			my $sh = IPC::Run::start($sby_cmd, '>', \$so, '2>', \$se);
			IPC::Run::finish($ph);
			IPC::Run::finish($sh);

			Test::More::like($po, qr{actually processed}, 'primary workload ran');
			Test::More::like($pe, $ctx->{stderr_re}, 'primary reported nothing');
			Test::More::like($so, qr{actually processed}, 'standby workload ran');
			Test::More::like($se, $ctx->{stderr_re}, 'standby reported nothing');
			return;
		},
		final => sub {
			my ($primary, $ctx) = @_;
			my $standby = $ctx->{standby};

			$primary->wait_for_catchup($standby);
			my $q = 'SELECT COALESCE(SUM(abalance), 0) FROM pgbench_accounts';
			Test::More::is(
				$standby->safe_psql('postgres', $q),
				$primary->safe_psql('postgres', $q),
				'the standby replayed the DDL churn to the same data');

			# It must also survive promotion with everything intact.
			$standby->promote;
			Test::More::is(
				$standby->safe_psql('postgres', $q),
				$primary->safe_psql('postgres', $q),
				'and still has it after promotion');
			return;
		},
	},

	# A subscriber applying what the workload produces while the
	# publisher's tables are rebuilt underneath the decoding.
	subscription => {
		init => { allows_streaming => 'logical' },
		conf => ['max_connections = 50'],
		setup => sub {
			my ($publisher, $ctx) = @_;

			my $subscriber = PostgreSQL::Test::Cluster->new('stress_subscriber');
			$subscriber->init;
			$subscriber->append_conf('postgresql.conf',
				'max_logical_replication_workers = 8');
			$subscriber->start;

			# The subscriber needs the same tables.  Take them from the
			# publisher with pg_dump rather than describing the schema a
			# second time, so a decorator's tables come across too.
			my $dumpfile = $publisher->basedir . '/schema.sql';
			PostgreSQL::Test::Utils::system_or_bail('pg_dump', '--schema-only',
				'--file', $dumpfile, $publisher->connstr('postgres'));
			PostgreSQL::Test::Utils::system_or_bail('psql', '--no-psqlrc',
				'--quiet', '--file', $dumpfile, '--dbname',
				$subscriber->connstr('postgres'));

			$publisher->safe_psql('postgres',
				'CREATE PUBLICATION stress_pub FOR ALL TABLES');
			my $connstr = $publisher->connstr . ' dbname=postgres';
			$subscriber->safe_psql('postgres',
				"CREATE SUBSCRIPTION stress_sub CONNECTION '$connstr' "
				  . 'PUBLICATION stress_pub');
			$publisher->wait_for_catchup('stress_sub');

			$ctx->{subscriber} = $subscriber;
			push @{ $ctx->{extra_nodes} }, $subscriber;
			return;
		},
		final => sub {
			my ($publisher, $ctx) = @_;
			my $subscriber = $ctx->{subscriber};

			$publisher->wait_for_catchup('stress_sub');
			my $q = 'SELECT COUNT(*), COALESCE(SUM(abalance), 0) '
			  . 'FROM pgbench_accounts';
			Test::More::is(
				$subscriber->safe_psql('postgres', $q),
				$publisher->safe_psql('postgres', $q),
				'the subscriber applied everything the publisher produced');

			# An apply worker that could not find its target row reports
			# it as a conflict rather than failing, so the log is where
			# such a loss would show up.
			my $log = PostgreSQL::Test::Utils::slurp_file($subscriber->logfile);
			Test::More::unlike($log, qr/conflict=update_missing/,
				'no update_missing conflict was logged');
			Test::More::unlike($log, qr/conflict=delete_missing/,
				'no delete_missing conflict was logged');
			return;
		},
	},
);

1;
