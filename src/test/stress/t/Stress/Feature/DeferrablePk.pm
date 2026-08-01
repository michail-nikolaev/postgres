
# Copyright (c) 2026, PostgreSQL Global Development Group

# A deferrable primary key, which REPACK must refuse and the key
# swaps drive through its transient-duplicate window.
#
# See Stress::Registry for what each declaration means.

package Stress::Feature::DeferrablePk;

use strict;
use warnings FATAL => 'all';

use Test::More;
use Stress::Registry ':declare';

# A deferrable primary key.  REPACK has to locate a tuple by an
# identity, and a deferrable key is not usable for that: under
# deferral the index can hold duplicates until commit, so a tuple
# being modified may not be findable by key.  Nothing else here
# declares one.
schema deferrable_pk => {
		setup => q(
			CREATE TABLE pgb_defer(id int NOT NULL, rid int NOT NULL, val int);
			ALTER TABLE pgb_defer ADD CONSTRAINT pgb_defer_pkey
				PRIMARY KEY (id) DEFERRABLE;
			-- Deliberately no replica identity: the deferrable key is
			-- the only identity this table has, which is the case REPACK
			-- has to decline.  Designating another index would defeat
			-- the test, because the broken code prefers a replica
			-- identity too and only falls back to the primary key.
			INSERT INTO pgb_defer
				SELECT g, g, 0 FROM generate_series(1, 2000) g;
		),
		# Deliberately not in the rotation: with the fix in place REPACK
		# refuses this table by design, and a refusal arriving in the DDL
		# client would be indistinguishable from a failure.  The check
		# asserts the refusal instead.
		tables => [],
};

# Swapping two primary keys, which is what a deferrable key exists
# for: neither UPDATE is unique on its own and only the pair is, so
# the index carries a duplicate until commit.  Each client swaps
# only within its own residue class mod 64, so clients never contend
# for a key and the transient duplicate is always this client's own;
# any client count up to 64 works.  The keys are taken in ascending
# order, so there is no deadlock either.
load deferred_key_swap => {
		weight => 2,
		requires => { schema => ['deferrable_pk'] },
		checks => ['deferred_keys_intact'],
		script => q(
			\set i1 random(0, 30)
			\set i2 random(0, 30)
			\set x :i1 * 64 + (:client_id % 64) + 1
			\set y :i2 * 64 + (:client_id % 64) + 1
			\set lo least(:x, :y)
			\set hi greatest(:x, :y)
			BEGIN;
			SET CONSTRAINTS pgb_defer_pkey DEFERRED;
			UPDATE pgb_defer SET id = -1 - :client_id WHERE id = :lo;
			UPDATE pgb_defer SET id = :lo WHERE id = :hi;
			UPDATE pgb_defer SET id = :hi WHERE id = -1 - :client_id;
			COMMIT;
		),
};

# REPACK cannot locate tuples by a deferrable key, so it has to
# refuse a table whose only identity is one.  Asserting the refusal
# is the only way to gate this: a server that accepts the table is
# the bug, and it does not announce itself.
check repack_refuses_deferrable => {
		auto => 1,
		requires => { schema => ['deferrable_pk'] },
		final => sub {
			my ($node, $ctx) = @_;
			my ($rc, $out, $err) =
			  $node->psql('postgres', 'REPACK (CONCURRENTLY) pgb_defer;',
				on_error_stop => 0);
			Test::More::like(
				$err,
				qr/does not support deferrable primary keys/,
				'REPACK refuses a table whose only identity is deferrable');
		},
};

# The key swaps permute the keys and put every one of them back, so
# once the load stops the set must be exactly what it started as.
check deferred_keys_intact => {
		requires => { schema => ['deferrable_pk'] },
		final => sub {
			my ($node, $ctx) = @_;
			Test::More::is(
				$node->safe_psql(
					'postgres', 'SELECT COUNT(*) || \'/\' || COALESCE(SUM(id), 0)'
					  . ' FROM pgb_defer'),
				'2000/2001000',
				'the deferrable key set survived the swaps');
		},
};

1;
