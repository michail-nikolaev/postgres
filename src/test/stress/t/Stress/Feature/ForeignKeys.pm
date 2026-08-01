
# Copyright (c) 2026, PostgreSQL Global Development Group

# A small, quiet foreign key parent whose primary key is rebuilt
# underneath the referential integrity checks.
#
# See Stress::Registry for what each declaration means.

package Stress::Feature::ForeignKeys;

use strict;
use warnings FATAL => 'all';

use Test::More;
use Stress::Registry ':declare';

use Stress::MVCC qw(stress_repack_tolerated);

# A self-referencing foreign key, so that every repoint fires a
# referential integrity check that resolves the parent through
# pgbench_accounts' own primary key -- while the rotation rebuilds
# that index underneath it.
# A parent and a child of their own, rather than a column of
# pgbench_accounts referencing itself.
#
# This is the one dimension where reusing the pgbench schema was
# tried and measured worse, and the reason is the size of the parent.
# What the referential integrity race needs is the parent's primary
# key being swapped underneath a check, and REINDEX INDEX
# CONCURRENTLY on a thousand rows finishes in about a millisecond
# where the same command on pgbench_accounts takes hundreds.  The
# rate of the thing being raced is what decides whether the race is
# ever seen, so the parent is kept small and its primary key is
# rebuilt on its own rather than as part of the whole table.
# A parent and a child of their own, rather than a column of the
# pgbench schema referencing another part of it.
#
# This is the one dimension where reusing pgbench's tables was tried
# and measured worse, and it is worth saying why, because the obvious
# reading is wrong.  What the referential integrity race needs is the
# parent's primary key swapped underneath a check, so the parent has
# to be small enough that REINDEX INDEX CONCURRENTLY on it finishes
# quickly.  pgbench_tellers is small -- ten rows -- but it is also the
# table every TPC-B transaction updates, so a concurrent rebuild of
# its primary key spends its time waiting for lockers and the swap
# rate collapses.  Referencing it reproduced nothing in eight runs at
# stressval 4; referencing pgbench_accounts, which is quiet enough but
# has a hundred thousand rows, reproduced nothing in fifty-odd.  A
# thousand-row parent that nothing writes reproduces it in two runs of
# eight.  Small and quiet, not just small.
schema fk_child => {
		setup => q(
			CREATE TABLE pgb_parent(id int PRIMARY KEY, val int);
			CREATE TABLE pgb_child(cid bigserial PRIMARY KEY,
				pid int NOT NULL REFERENCES pgb_parent(id), val int);
			INSERT INTO pgb_parent SELECT g, g FROM generate_series(1, 1000) g;
			INSERT INTO pgb_child(pid, val)
				SELECT g, g FROM generate_series(1, 1000) g;
		),
		tables => [ 'pgb_parent', 'pgb_child' ],
		indexes => [ {
			table => 'pgb_child',
			name => 'pgb_child_pid_idx',
			am => 'btree',
			defn => 'ON pgb_child(pid)',
		} ],
		context => sub { return { nparents => 1000 } },
};

# Inserts, deletes and repointings of child rows, each of which fires
# a foreign key check against the parent.
load fk_churn => {
		weight => 4,
		requires => { schema => ['fk_child'] },
		checks => ['no_orphans'],
		script => q(
			\set pid_a random(1, :nparents)
			\set pid_b random(1, :nparents)
			\set mode random(0, 2)
			-- Each of these fires a referential integrity check against
			-- pgb_parent, which resolves it through the primary key the
			-- rotation is rebuilding.
			\if :mode = 0
				INSERT INTO pgb_child(pid, val) VALUES (:pid_a, :pid_b);
			\elif :mode = 1
				UPDATE pgb_child SET pid = :pid_b
					WHERE cid = (SELECT cid FROM pgb_child
						WHERE pid = :pid_a LIMIT 1);
			\else
				DELETE FROM pgb_child
					WHERE cid = (SELECT cid FROM pgb_child
						WHERE pid = :pid_a ORDER BY cid DESC LIMIT 1)
					AND (SELECT COUNT(*) FROM pgb_child) > :nparents;
			\endif
		),
};

# No child row may reference a parent that is not there.
check no_orphans => {
		weight => 1,
		requires => { schema => ['fk_child'] },
		script => sub {
			# The relation is in the rotation's reach, so it reading empty
			# has to be allowed for: an empty table would make every
			# reference look like an orphan.
			my $tol = stress_repack_tolerated('rows');
			return qq(
			SELECT stress_assert(${tol}orphans = 0,
				format('%s rows reference a missing parent', orphans))
			FROM (SELECT
				(SELECT COUNT(*) FROM pgb_child c WHERE NOT EXISTS
					(SELECT 1 FROM pgb_parent p WHERE p.id = c.pid)) AS orphans,
				(SELECT COUNT(*) FROM pgb_child) AS rows) x;
			);
		},
		final => sub {
			my ($node, $ctx) = @_;
			Test::More::is(
				$node->safe_psql(
					'postgres', q(
					SELECT COUNT(*) FROM pgb_child c WHERE NOT EXISTS
						(SELECT 1 FROM pgb_parent p WHERE p.id = c.pid))),
				'0', 'no orphan child rows');
		},
};

1;
