# Copyright (c) 2026, PostgreSQL Global Development Group

# Stress test for CONCURRENTLY commands on a table carrying row-level
# security and replica-mode triggers, driven under
# session_replication_role = replica.
#
# session_replication_role = replica is the mode logical replication
# apply workers run in: it bypasses row-level security and fires only
# triggers marked ENABLE REPLICA / ALWAYS.  This test exercises that
# mode directly, with a client that mimics an apply worker -- it sets
# the role to replica and rewrites the rows through a table that has an
# RLS policy and both an ordinary and a replica-mode trigger -- while
# the DDL rotation rebuilds and repacks the table underneath it.
#
# A replica-mode BEFORE trigger maintains a shadow column so that
# gen_shadow always equals a fixed function of val; the ordinary
# trigger, which must NOT fire in replica mode, would instead corrupt
# it.  Readers therefore verify both the sum invariant and that the
# shadow column still matches, which together check that RLS was
# bypassed and only the right trigger fired even as the table was
# rewritten concurrently.
use strict;
use warnings FATAL => 'all';

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;

use Test::More;

use FindBin;
use lib $FindBin::RealBin;
use StressConcurrently;

my $stressval = stress_plan(skip =>
	'skipping disabled replica-role stress test');

my $duration = 6 * $stressval;
my $nrows = 5000;

my $node;

#
# Test set-up
#
$node = stress_init_node('replica_role',
	extra_conf => [ 'max_connections = 50' ]);

$node->safe_psql(
	'postgres', qq(
	CREATE EXTENSION amcheck;
	CREATE TABLE tbl(id int PRIMARY KEY, val int, shadow int);
	CREATE INDEX tbl_shadow_idx ON tbl(shadow);

	-- The replica-mode trigger keeps shadow = val + 1000; the ordinary
	-- trigger would set it to something wrong, and must not fire when
	-- session_replication_role = replica.
	CREATE FUNCTION set_shadow_ok() RETURNS trigger LANGUAGE plpgsql AS \$\$
	BEGIN NEW.shadow := NEW.val + 1000; RETURN NEW; END; \$\$;
	CREATE FUNCTION set_shadow_bad() RETURNS trigger LANGUAGE plpgsql AS \$\$
	BEGIN NEW.shadow := NEW.val - 1; RETURN NEW; END; \$\$;

	CREATE TRIGGER shadow_ok BEFORE INSERT OR UPDATE ON tbl
		FOR EACH ROW EXECUTE FUNCTION set_shadow_ok();
	ALTER TABLE tbl ENABLE REPLICA TRIGGER shadow_ok;
	CREATE TRIGGER shadow_bad BEFORE INSERT OR UPDATE ON tbl
		FOR EACH ROW EXECUTE FUNCTION set_shadow_bad();

	-- An RLS policy that would hide every row from an ordinary session,
	-- but is bypassed in replica mode.
	ALTER TABLE tbl ENABLE ROW LEVEL SECURITY;
	ALTER TABLE tbl FORCE ROW LEVEL SECURITY;
	CREATE POLICY hide_all ON tbl USING (false);
));

# Populate in replica mode, just like an apply worker would: RLS is
# bypassed, and the replica trigger sets shadow = val + 1000.  (In
# normal mode the RLS WITH CHECK would reject the insert, and the
# ordinary trigger would set the wrong shadow value.)
$node->safe_psql(
	'postgres', qq(
	SET session_replication_role = replica;
	INSERT INTO tbl(id, val) SELECT g, g FROM generate_series(1, $nrows) g;
));

my $sum = $node->safe_psql('postgres', q(SELECT SUM(val) FROM tbl));

$node->pgbench(
	"--no-vacuum --client=30 --jobs=30 --exit-on-abort -T $duration",
	0,
	[qr{actually processed}],
	[qr{^$}],
	'CONCURRENTLY commands under session_replication_role = replica',
	{
		'concurrent_ops' => stress_ddl_gate(
			indent => "\t\t",
			ddl => [
				'REPACK (CONCURRENTLY) tbl;',
				'REINDEX TABLE CONCURRENTLY tbl;',
				[
					'DROP INDEX CONCURRENTLY tbl_shadow_idx;',
					'CREATE INDEX CONCURRENTLY tbl_shadow_idx ON tbl(shadow);',
				],
			],
			post =>
			  "SELECT bt_index_check('tbl_pkey', heapallindexed => true, checkunique => true);",
			else => stress_workload(
				mutations => [
					qq(-- Write like an apply worker: in replica mode, so RLS
					-- is bypassed and only the replica trigger fires.
					\\set num_a random(1, $nrows)
					\\set num_b random(1, $nrows)
					\\set diff random(1, 10000)
					SET session_replication_role = replica;
					BEGIN;
					UPDATE tbl SET val = val + :diff WHERE id = :num_a;
					\\sleep 1 ms
					UPDATE tbl SET val = val - :diff WHERE id = :num_b;
					\\sleep 1 ms
					COMMIT;
					RESET session_replication_role;),
				],
				checks => [
					# Verify as the apply worker would see it: replica mode
					# bypasses RLS, so the rows are visible, and the shadow
					# column must have been maintained only by the replica
					# trigger.  REPACK is not MVCC-safe yet, so tolerate an
					# empty view.
					qq(SET session_replication_role = replica;
					SELECT stress_assert(
						cnt = 0 OR (bad = 0 AND cnt = $nrows AND sum = $sum),
						format('rows=%s bad=%s sum=%s (want $nrows rows, 0 bad, sum $sum)',
							cnt, bad, sum))
					FROM (SELECT COUNT(*) AS cnt,
						COUNT(*) FILTER (WHERE shadow <> val + 1000) AS bad,
						COALESCE(SUM(val), 0) AS sum FROM tbl) t;
					RESET session_replication_role;),
				],
			),
		),
	});

# Final verification, again in replica mode so RLS lets us see the rows.
$node->safe_psql('postgres', q(SET session_replication_role = replica));

is( $node->safe_psql(
		'postgres',
		q(SET session_replication_role = replica;
		  SELECT SUM(val) FROM tbl)),
	$sum, 'sum invariant holds after replica-role churn');
is( $node->safe_psql(
		'postgres',
		q(SET session_replication_role = replica;
		  SELECT COUNT(*) FROM tbl)),
	"$nrows", 'no rows lost');
is( $node->safe_psql(
		'postgres',
		q(SET session_replication_role = replica;
		  SELECT COUNT(*) FROM tbl WHERE shadow <> val + 1000)),
	'0', 'only the replica trigger fired: shadow column is intact');

$node->safe_psql(
	'postgres', q(
	SELECT bt_index_parent_check('tbl_pkey', heapallindexed => true);
	SELECT bt_index_parent_check('tbl_shadow_idx', heapallindexed => true);
));

$node->stop;

done_testing();
