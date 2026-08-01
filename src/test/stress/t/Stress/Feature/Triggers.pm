
# Copyright (c) 2026, PostgreSQL Global Development Group

# Triggers on the table the rotation rebuilds: rows written from
# inside the executor, plans cached by plpgsql, and the DDL that
# invalidates them.
#
# See Stress::Registry for what each declaration means.

package Stress::Feature::Triggers;

use strict;
use warnings FATAL => 'all';

use Test::More;
use Stress::Registry ':declare';

# Triggers on the table the rotation rebuilds, and the tables they
# write into.  Two paths arrive with them that nothing else here
# has.  The rows of pgb_audit are written from inside a trigger,
# after the statement rather than as part of it, so its indexes are
# fed by a path no other scenario drives.  And the queries the
# trigger bodies run come from plans plpgsql caches and reuses, so a
# rebuild that swaps or drops an index has to reach a plan made
# before it and force a replan -- every other statement in this
# suite is parsed afresh and can only ever see the current catalog.
schema trigger_audit => {
		# The trigger is created on pgbench_accounts by name, and the
		# partitioning decorator renames that table out from under it.
		# Which of the two runs first decides whether the trigger ends up
		# on the parent or on the partition holding the old rows, and the
		# DDL that names it later would only find it in one of those.
		#
		# And not against a subscription.  The deferred audit trigger
		# stretches every publisher commit, and against the topology's
		# own churn -- tablesync re-copies, publication refreshes, the
		# apply worker's stream -- an unbounded CONCURRENTLY command in
		# the rotation can then outwait a survey's lock timeout twice
		# over with nothing being wrong.  Driving triggers against a
		# subscription deserves a scenario tuned for it, not an invented
		# combination that mostly measures queueing.  Found by the first
		# soak in which the trigger loads pulled this schema themselves.
		conflicts => {
			schema => ['partitioned'],
			topology => ['subscription'],
		},
		setup => q(
			CREATE TABLE pgb_audit(
				id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
				aid int NOT NULL,
				delta int NOT NULL);

			-- The counter rows the log trigger upserts into, keyed by the
			-- pgbench client rather than by anything the workload picks,
			-- so two sessions never want the same row.  What is under
			-- test is the arbiter index; a shared row would only add
			-- waiting, and an update order for a deadlock to form in.
			CREATE TABLE pgb_calls(
				client int PRIMARY KEY,
				hits bigint NOT NULL);

			CREATE TABLE pgb_call_log(client int NOT NULL);

			CREATE FUNCTION pgb_audit_trigger() RETURNS trigger
			LANGUAGE plpgsql AS $$
			BEGIN
				INSERT INTO pgb_audit(aid, delta)
					VALUES (NEW.aid, NEW.abalance - OLD.abalance);
				RETURN NULL;
			END $$;

			CREATE TRIGGER pgb_audit_trg AFTER UPDATE ON pgbench_accounts
				FOR EACH ROW
				WHEN (OLD.abalance IS DISTINCT FROM NEW.abalance)
				EXECUTE FUNCTION pgb_audit_trigger();

			-- Deferred to commit, so its lookup runs after the statement
			-- that queued it and after everything else the transaction
			-- did: a window a rebuild can land in the middle of.  The row
			-- it looks for is the transaction's own, so it is visible
			-- however the planner reaches it, and not finding it means
			-- the index it was reached through has lost it.
			CREATE FUNCTION pgb_audit_defer_trigger() RETURNS trigger
			LANGUAGE plpgsql AS $$
			DECLARE n bigint;
			BEGIN
				SELECT count(*) INTO n FROM pgb_audit WHERE id = NEW.id;
				IF n <> 1 THEN
					RAISE EXCEPTION
						'audit row % not found at commit, count %', NEW.id, n;
				END IF;
				RETURN NULL;
			END $$;

			CREATE CONSTRAINT TRIGGER pgb_audit_defer_trg
				AFTER INSERT ON pgb_audit
				DEFERRABLE INITIALLY DEFERRED
				FOR EACH ROW EXECUTE FUNCTION pgb_audit_defer_trigger();

			-- An upsert that has to infer its arbiter every time, from a
			-- plan that was cached the first time this session fired the
			-- trigger and is reused for the rest of the run.
			CREATE FUNCTION pgb_calls_trigger() RETURNS trigger
			LANGUAGE plpgsql AS $$
			BEGIN
				INSERT INTO pgb_calls(client, hits) VALUES (NEW.client, 1)
					ON CONFLICT (client) DO UPDATE
					SET hits = pgb_calls.hits + 1;
				RETURN NULL;
			END $$;

			CREATE TRIGGER pgb_calls_trg AFTER INSERT ON pgb_call_log
				FOR EACH ROW EXECUTE FUNCTION pgb_calls_trigger();

			-- What the trigger DDL creates and drops.  It returns NEW
			-- rather than NULL: a BEFORE ROW trigger returning NULL
			-- skips the row, which would quietly discard the workload's
			-- updates and every invariant with them.
			CREATE FUNCTION pgb_noop_trigger() RETURNS trigger
			LANGUAGE plpgsql AS $$ BEGIN RETURN NEW; END $$;
		),
		# pgb_call_log stays out of the rotation: it has no identity, so
		# REPACK and the primary key rebuilds would have nothing to work
		# with.  It is append-only anyway, like pgbench_history.
		tables => [ 'pgb_audit', 'pgb_calls' ],
};

# An index fed entirely from inside a trigger: every row of pgb_audit
# arrives from the after-update trigger on pgbench_accounts rather
# than from a statement pgbench sent, so the rows and the index
# entries are made in a part of the executor the rest of this suite
# never reaches while a build is running.  amcheck's heapallindexed
# is what has to agree at the end.
index_def btree_audit_aid => {
		# The only index here whose table is not one of pgbench's, so the
		# only one that is not there for every scenario.
		requires => { schema => ['trigger_audit'] },
		table => 'pgb_audit',
		name => 'pgb_audit_aid_idx',
		am => 'btree',
		defn => 'ON pgb_audit(aid)',
};

# Rows that exist only to fire a trigger.  The insert itself is
# trivial and touches a table nothing rebuilds; the work is in the
# after-insert trigger, which upserts a counter row and so has to
# resolve an arbiter index every time -- from a plan cached the first
# time this session fired it, while the rotation rebuilds the very
# index that arbiter resolves to.  Every other upsert here is sent by
# pgbench and inferred afresh against the catalog as it stands.
load trigger_upsert_log => {
		weight => 3,
		requires => { schema => ['trigger_audit'] },
		checks => ['trigger_counts_agree'],
		script => q(
			INSERT INTO pgb_call_log(client) VALUES (:client_id);
		),
};

# A read that runs from a cached plan rather than a fresh one.  The
# plan chose an index on pgb_audit and holds it; drop_create_index
# takes that index away and builds it again underneath.  A plan that
# does not notice is a session holding an index list it read before
# the drop, which is the shape the planner had to be taught to
# tolerate on a standby.
load audit_probe => {
		weight => 2,
		requires => { schema => ['trigger_audit'] },
		setup => q(
			CREATE FUNCTION pgb_audit_probe(p_aid int) RETURNS bigint
			LANGUAGE plpgsql AS $$
			DECLARE n bigint;
			BEGIN
				SELECT count(*) INTO n FROM pgb_audit WHERE aid = p_aid;
				RETURN n;
			END $$;
		),
		script => q(
			\set aid random(1, :naccounts)
			SELECT pgb_audit_probe(:aid);
		),
};

# A trigger created on the table the rotation is rebuilding, and
# dropped again.  Both ends invalidate the relation, so every backend
# that has it open rebuilds its descriptor -- which is what a
# concurrent build has to survive, and what the historical
# RelationBuildDesc race got wrong.  Nothing else here invalidates a
# table this cheaply and this often: the index commands do it too,
# but each of them costs a whole build.
#
# CREATE TRIGGER takes ShareRowExclusiveLock and DROP TRIGGER an
# AccessExclusiveLock, so both wait for the workload and both have to
# be bounded.  OR REPLACE and IF EXISTS keep the pair idempotent, so
# a create that timed out does not make the drop that follows it an
# error -- and two DDL clients aimed at the same table serialize on
# the locks rather than collide.
ddl create_drop_trigger => {
		requires => { schema => ['trigger_audit'] },
		variants => sub {
			my ($ctx) = @_;
			return map {
				my $t = $_;
				{
					table => $t,
					stmts => [
						# "trigger does not exist, skipping" is a NOTICE,
						# and the run insists on a silent stderr.
						'SET client_min_messages = warning;',
						"SELECT pgb_ddl_bounded('CREATE OR REPLACE TRIGGER "
						  . "pgb_noop_trg BEFORE UPDATE ON $t FOR EACH ROW "
						  . "EXECUTE FUNCTION pgb_noop_trigger()');",
						"SELECT pgb_ddl_bounded('DROP TRIGGER IF EXISTS "
						  . "pgb_noop_trg ON $t');",
						'RESET client_min_messages;'
					]
				}
			} @{ $ctx->{tables} };
		},
};

# The same thing as a constraint trigger, which is a different
# catalog shape for the same idea: it carries a pg_constraint row of
# its own, so creating and dropping it goes through the dependency
# machinery rather than pg_trigger alone.  While it is there every
# update queues an event for commit time instead of firing in place,
# which is the after-trigger queue the deferred audit trigger also
# uses.
#
# There is no OR REPLACE for a constraint trigger -- the server
# rejects the combination outright -- so this drops whatever the last
# invocation left and creates it again, leaving it in place until the
# next one.  That is not idempotent between two DDL clients running
# it at once, hence solo.
ddl create_drop_constraint_trigger => {
		requires => { schema => ['trigger_audit'] },
		solo => 1,
		# Not against a subscription.  This leaves its deferrable trigger
		# installed between invocations, so every row the apply worker
		# writes queues an event for commit; with tablesync re-copies and
		# publication churn on the same tables, a concurrent reindex on
		# the publisher can then outwait a survey's lock timeout without
		# anything being wrong.  Found by the first soak in which the
		# trigger loads pulled their schema in themselves.
		conflicts => { topology => ['subscription'] },
		variants => sub {
			my ($ctx) = @_;
			return map {
				my $t = $_;
				{
					table => $t,
					stmts => [
						'SET client_min_messages = warning;',
						"SELECT pgb_ddl_bounded('DROP TRIGGER IF EXISTS "
						  . "pgb_noop_ctrg ON $t');",
						"SELECT pgb_ddl_bounded('CREATE CONSTRAINT "
						  . "TRIGGER pgb_noop_ctrg AFTER UPDATE ON $t "
						  . 'DEFERRABLE INITIALLY DEFERRED FOR EACH ROW '
						  . "EXECUTE FUNCTION pgb_noop_trigger()');",
						'RESET client_min_messages;'
					]
				}
			} @{ $ctx->{tables} };
		},
};

# The audit trigger switched off and on again.  ENABLE and DISABLE
# take ShareRowExclusiveLock -- they conflict with writers and
# nothing else -- and they change a field of the descriptor rather
# than the set of triggers, so they land on a different part of the
# relcache entry from the create and drop above.  ALWAYS and REPLICA
# are here because that field is what decides whether a trigger fires
# for an apply worker, and this suite has an environment with one.
#
# The audit rows stop arriving while it is off, which nothing checks
# a count of: the invariant that does hold is between the log and its
# counters, and that trigger is on another table.
ddl toggle_trigger => {
		requires => { schema => ['trigger_audit'] },
		variants => sub {
			return map {
				{
					table => 'pgbench_accounts',
					stmts => [
						"SELECT pgb_ddl_bounded('ALTER TABLE pgbench_accounts "
						  . "DISABLE TRIGGER pgb_audit_trg');",
						"SELECT pgb_ddl_bounded('ALTER TABLE pgbench_accounts "
						  . "$_ TRIGGER pgb_audit_trg');",
						# Always back to firing, whichever state the middle
						# statement left it in.  ALWAYS and REPLICA are worth
						# visiting, but a run that ends up sitting in one of
						# them stops writing audit rows, and the audit table
						# is what amcheck and the deferred trigger work on.
						"SELECT pgb_ddl_bounded('ALTER TABLE pgbench_accounts "
						  . "ENABLE TRIGGER pgb_audit_trg');"
					]
				}
			} ('ENABLE', 'ENABLE ALWAYS', 'ENABLE REPLICA');
		},
};

# The trigger functions altered rather than the tables.  This takes
# no lock on any relation: it updates pg_proc, which invalidates the
# function and with it every plan that plpgsql cached for its body,
# in every session at once.  So it is the cheapest invalidation the
# rotation has, and the one that can be aimed at a running build at
# the highest rate.  COST is chosen because it changes nothing a
# caller can observe -- the bodies stay exactly as the schema wrote
# them, and the invariants with them.
ddl alter_trigger_function => {
		requires => { schema => ['trigger_audit'] },
		variants => sub {
			my @fns = qw(pgb_audit_trigger pgb_calls_trigger
			  pgb_audit_defer_trigger);
			return map {
				my $fn = $_;
				{
					# Named for the relation this really writes to.  Naming
					# a table would gate it against the very rebuilds it is
					# supposed to interrupt, and it takes no lock on one.
					table => 'pg_proc',
					stmts => [
						"ALTER FUNCTION $fn() COST 100;",
						"ALTER FUNCTION $fn() COST 1;"
					]
				}
			} @fns;
		},
};

# Every row of the log fired the trigger that counts it, in the same
# transaction that inserted it, so the counters and the log agree
# exactly however many rebuilds ran underneath them.  What this
# catches is a count that went missing: an upsert whose arbiter
# resolved to an index that is no longer the one enforcing the key
# can take the insert path where it should have taken the update
# path, or find nothing to conflict with at all, and the total comes
# up short.  A duplicated count would not show here -- the primary
# key on pgb_calls makes that impossible unless the index itself is
# wrong, which is amcheck's question rather than this one.
check trigger_counts_agree => {
		requires => { schema => ['trigger_audit'] },
		final => sub {
			my ($node, $ctx) = @_;
			my $row = $node->safe_psql(
				'postgres', q(
				SELECT (SELECT COALESCE(SUM(hits), 0) FROM pgb_calls)
					|| ' ' || (SELECT COUNT(*) FROM pgb_call_log)
					|| ' ' || (SELECT COUNT(*) FROM pgb_audit)));
			my ($counted, $logged, $audited) = split / /, $row;
			# Reported so that a run where the triggers never fired --
			# every count zero, and the comparison below true for the
			# wrong reason -- is visible in the log rather than green.
			Test::More::note(
				"trigger writes: $counted counted, $logged logged, "
				  . "$audited audit rows");
			Test::More::is($counted, $logged,
				'every logged call was counted once by its trigger');
		},
};

1;
