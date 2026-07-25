# Copyright (c) 2026, PostgreSQL Global Development Group

# Stress test for CONCURRENTLY commands on tables tied together by a
# foreign key.
#
# Referential integrity checks look the parent up through the index
# backing its primary key, so rebuilding that index concurrently
# (REINDEX INDEX/TABLE CONCURRENTLY changes the index OID, which the
# constraint has to follow) or repacking the parent underneath the
# checks must not let a violation slip through.
#
# Writer clients insert child rows referencing random parents, delete
# child rows, and repoint existing child rows at other parents -- each
# of which fires an RI check -- while one client rotates the DDL over
# both the parent and the child.  Reader clients verify that no child
# row ever references a missing parent, and that the parent set is
# intact.  Any SQL error, RI violation or broken invariant aborts
# pgbench, failing the test.
#
# XXX This test is occasionally seen to fail (order of once in ~15
# runs) with:
#
#   ERROR:  could not open relation with OID <n>
#
# raised from a foreign-key check while a REPACK (CONCURRENTLY) or
# REINDEX CONCURRENTLY of the referenced parent's primary-key index is
# in progress -- apparently the RI lookup resolves the parent's index
# to an OID that the concurrent rebuild is dropping.  It is not clear
# yet whether this is a genuine RI-vs-CONCURRENTLY race worth fixing in
# the backend, or a transient the test should tolerate; it is left here
# as-is deliberately, so that it keeps surfacing until that is decided.
# See also the REPACK (CONCURRENTLY) MVCC-safety caveat referenced in
# the other tests.
use strict;
use warnings FATAL => 'all';

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;

use Test::More;

use FindBin;
use lib $FindBin::RealBin;
use StressConcurrently;

my $stressval = stress_plan(skip =>
	'skipping disabled foreign key stress test');

my $duration = 6 * $stressval;
my $nparents = 1000;

my $node;

#
# Test set-up
#
$node = stress_init_node('foreign_keys');
$node->safe_psql(
	'postgres', qq(
	CREATE EXTENSION amcheck;
	CREATE TABLE parent(id int PRIMARY KEY, val int);
	CREATE TABLE child(cid bigserial PRIMARY KEY, pid int NOT NULL
		REFERENCES parent(id), val int);
	CREATE INDEX child_pid_idx ON child(pid);
	INSERT INTO parent SELECT g, g FROM generate_series(1, $nparents) g;
	INSERT INTO child(pid, val)
		SELECT g, g FROM generate_series(1, $nparents) g;
));

$node->pgbench(
	"--no-vacuum --client=30 --jobs=4 --exit-on-abort -T $duration",
	0,
	[qr{actually processed}],
	[qr{^$}],
	'CONCURRENTLY commands with concurrent foreign key checks',
	{
		'concurrent_ops' => stress_ddl_gate(
			indent => "\t\t",
			ddl => [
				'REPACK (CONCURRENTLY) parent;',
				'REPACK (CONCURRENTLY) child;',
				'REINDEX INDEX CONCURRENTLY parent_pkey;',
				'REINDEX TABLE CONCURRENTLY child;',
				[
					'DROP INDEX CONCURRENTLY child_pid_idx;',
					'CREATE INDEX CONCURRENTLY child_pid_idx ON child(pid);',
				],
			],
			post => [
				"SELECT bt_index_check('parent_pkey', heapallindexed => true, checkunique => true);",
				"SELECT bt_index_check('child_pkey', heapallindexed => true, checkunique => true);",
			],
			else => stress_workload(
				mutations => [
					qq(-- Insert fires an RI check against the parent.
					\\set pid_a random(1, $nparents)
					\\set pid_b random(1, $nparents)
					INSERT INTO child(pid, val) VALUES (:pid_a, :pid_b);
					\\sleep 1 ms),
					qq(-- Repointing a child fires an RI check too.
					\\set pid_a random(1, $nparents)
					\\set pid_b random(1, $nparents)
					UPDATE child SET pid = :pid_b
						WHERE cid = (SELECT cid FROM child WHERE pid = :pid_a LIMIT 1);
					\\sleep 1 ms),
					qq(-- Keep the child table from growing without bound.
					\\set pid_a random(1, $nparents)
					DELETE FROM child
						WHERE cid = (SELECT cid FROM child
									 WHERE pid = :pid_a ORDER BY cid DESC LIMIT 1)
						AND (SELECT COUNT(*) FROM child) > $nparents;
					\\sleep 1 ms),
				],
				checks => [
					qq(-- No child row may reference a missing parent, and the
					-- parent set must stay intact.
					SELECT stress_assert(orphans = 0 AND parents = $nparents,
						format('orphans=%s parents=%s (want 0 orphans, $nparents parents)',
							orphans, parents))
					FROM (SELECT
						(SELECT COUNT(*) FROM child c
							WHERE NOT EXISTS (SELECT 1 FROM parent p
								WHERE p.id = c.pid)) AS orphans,
						(SELECT COUNT(*) FROM parent) AS parents) t;),
				],
			),
		),
	});

my $orphans = $node->safe_psql(
	'postgres', q(
	SELECT COUNT(*) FROM child c
	WHERE NOT EXISTS (SELECT 1 FROM parent p WHERE p.id = c.pid)));
is($orphans, '0', 'no orphan child rows after DDL churn');

# The foreign key must still be enforced, and still point at a valid
# index, after all the concurrent rebuilds.
my ($ret, $out, $err) = $node->psql('postgres',
	qq(INSERT INTO child(pid, val) VALUES (@{[ $nparents + 1 ]}, 0)));
isnt($ret, 0, 'foreign key still rejects a missing parent');
like(
	$err,
	qr/violates foreign key constraint/,
	'foreign key violation reported');

is( $node->safe_psql('postgres',
		q(SELECT COUNT(*) FROM pg_constraint c JOIN pg_index i
		  ON i.indexrelid = c.conindid
		  WHERE c.conname = 'child_pid_fkey' AND i.indisvalid)),
	'1', 'foreign key points at a valid index');

$node->safe_psql(
	'postgres', q(
	SELECT bt_index_parent_check('parent_pkey', heapallindexed => true);
	SELECT bt_index_parent_check('child_pkey', heapallindexed => true);
	SELECT bt_index_parent_check('child_pid_idx', heapallindexed => true);
));

$node->stop;

done_testing();
