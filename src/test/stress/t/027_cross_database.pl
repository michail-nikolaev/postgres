# Copyright (c) 2026, PostgreSQL Global Development Group

# Stress test for REPACK (CONCURRENTLY) and logical decoding happening
# in different databases at the same time.
#
# REPACK (CONCURRENTLY) drives logical decoding through a transient
# slot in the database it runs in.  Logical decoding infrastructure --
# the xl_running_xacts records the snapshot builder relies on, and the
# global switch between replica and logical WAL levels -- is shared
# across the whole cluster, even though a given slot only ever decodes
# its own database.  This test runs REPACK churn in one database while
# a real logical replication slot decodes a second database, so the two
# exercise that shared machinery against each other.
#
# Writers in each database keep that database's sum invariant.  The
# decoding database is also consumed through pg_logical_slot_get_changes
# so that its slot keeps advancing rather than just pinning WAL.
use strict;
use warnings FATAL => 'all';

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use IPC::Run qw(start finish);

use Test::More;

use FindBin;
use lib $FindBin::RealBin;
use StressConcurrently;

my $stressval = stress_plan(skip =>
	'skipping disabled cross-database stress test');

my $duration = 6 * $stressval;
my $nrows = 5000;

my $node;

#
# Test set-up
#
$node = stress_init_node('cross_database',
	extra_conf => [ 'max_replication_slots = 4', 'max_connections = 50' ]);

# db_repack gets the REPACK churn; db_decode gets a logical slot.
$node->safe_psql('postgres', q(CREATE DATABASE db_repack));
$node->safe_psql('postgres', q(CREATE DATABASE db_decode));

# stress_init_node created stress_assert() in postgres, but the
# workloads run in these two databases; create it there too.
$node->safe_psql('db_repack', stress_assert_defn());
$node->safe_psql('db_decode', stress_assert_defn());

$node->safe_psql(
	'db_repack', qq(
	CREATE EXTENSION amcheck;
	CREATE TABLE tbl(id int PRIMARY KEY, val int);
	CREATE INDEX tbl_val_idx ON tbl(val);
	INSERT INTO tbl SELECT g, g FROM generate_series(1, $nrows) g;
));

$node->safe_psql(
	'db_decode', qq(
	CREATE TABLE tbl(id int PRIMARY KEY, val int);
	INSERT INTO tbl SELECT g, g FROM generate_series(1, $nrows) g;
	SELECT 'init' FROM pg_create_logical_replication_slot('decode_slot',
		'test_decoding');
));

my $repack_sum = $node->safe_psql('db_repack', q(SELECT SUM(val) FROM tbl));
my $decode_sum = $node->safe_psql('db_decode', q(SELECT SUM(val) FROM tbl));

# Churn + REPACK in db_repack.
my $repack_sql = $node->basedir . '/repack_ops.sql';
PostgreSQL::Test::Utils::append_to_file(
	$repack_sql,
	stress_ddl_gate(
		indent => '',
		ddl => [
			'REPACK (CONCURRENTLY) tbl;',
			'REINDEX TABLE CONCURRENTLY tbl;',
			[
				'DROP INDEX CONCURRENTLY tbl_val_idx;',
				'CREATE INDEX CONCURRENTLY tbl_val_idx ON tbl(val);'
			],
		],
		post =>
		  "SELECT bt_index_check('tbl_pkey', heapallindexed => true, checkunique => true);",
		else => qq(\t\\set num_a random(1, $nrows)
\t\\set num_b random(1, $nrows)
\t\\set diff random(1, 10000)
\tBEGIN;
\tUPDATE tbl SET val = val + :diff WHERE id = :num_a;
\t\\sleep 1 ms
\tUPDATE tbl SET val = val - :diff WHERE id = :num_b;
\t\\sleep 1 ms
\tCOMMIT;
\tSELECT stress_assert(cnt = 0 OR (cnt = $nrows AND sum = $repack_sum),
\t\tformat('rows=%s sum=%s (want 0, or $nrows rows sum $repack_sum)',
\t\t\tcnt, sum))
\tFROM (SELECT COUNT(*) AS cnt, COALESCE(SUM(val), 0) AS sum FROM tbl) t;)
	) . "\n");

# Churn + slot consumption in db_decode.
my $decode_sql = $node->basedir . '/decode_ops.sql';
PostgreSQL::Test::Utils::append_to_file($decode_sql,
	qq(\\set num_a random(1, $nrows)
\\set num_b random(1, $nrows)
\\set diff random(1, 10000)
BEGIN;
UPDATE tbl SET val = val + :diff WHERE id = :num_a;
UPDATE tbl SET val = val - :diff WHERE id = :num_b;
COMMIT;

SELECT stress_assert(COALESCE(SUM(val), 0) = $decode_sum,
	format('sum is %s, not $decode_sum', COALESCE(SUM(val), 0))) FROM tbl;

)
	  . stress_ddl_gate(
		indent => '',
		lock => 99,
		var => 'gotslot',
		sleep_ms => 0,
		# Keep the slot moving so it does not just pin WAL.  A slot can
		# only be consumed by one session at a time, hence the gate; any
		# error from decoding aborts pgbench by itself.
		ddl => [
			"SELECT count(*) FROM pg_logical_slot_get_changes('decode_slot', NULL, NULL);"
		],
		else => '',
	  ) . "\n");

my @repack_cmd = (
	'pgbench', '--no-vacuum', '--client=15', '--jobs=15', '--exit-on-abort',
	'-T', $duration, '-p', $node->port, '-h', $node->host,
	'-f', $repack_sql, 'db_repack');
my @decode_cmd = (
	'pgbench', '--no-vacuum', '--client=10', '--jobs=10', '--exit-on-abort',
	'-T', $duration, '-p', $node->port, '-h', $node->host,
	'-f', $decode_sql, 'db_decode');

my ($ro, $re, $do, $de) = ('', '', '', '');
my $rh = start \@repack_cmd, '>', \$ro, '2>', \$re;
my $dh = start \@decode_cmd, '>', \$do, '2>', \$de;
finish $rh;
finish $dh;

like($ro, qr/actually processed/, 'db_repack pgbench');
is($re, '', 'db_repack pgbench no stderr');
like($do, qr/actually processed/, 'db_decode pgbench');
is($de, '', 'db_decode pgbench no stderr');

is( $node->safe_psql('db_repack', q(SELECT SUM(val) FROM tbl)),
	$repack_sum, 'db_repack sum invariant holds');
is( $node->safe_psql('db_decode', q(SELECT SUM(val) FROM tbl)),
	$decode_sum, 'db_decode sum invariant holds');

$node->safe_psql(
	'db_repack', q(
	SELECT bt_index_parent_check('tbl_pkey', heapallindexed => true);
	SELECT bt_index_parent_check('tbl_val_idx', heapallindexed => true);
));

# The decoding slot must still be usable, and drain cleanly.
$node->safe_psql('db_decode',
	q(SELECT count(*) >= 0 FROM pg_logical_slot_get_changes('decode_slot',
		NULL, NULL)));
$node->safe_psql('db_decode',
	q(SELECT pg_drop_replication_slot('decode_slot')));
pass('decoding slot in the other database remained usable');

# Only REPACK's transient slots existed besides decode_slot, and those
# must be gone.
is( $node->safe_psql('postgres', q(SELECT COUNT(*) FROM pg_replication_slots)),
	'0', 'no replication slot leaked');

$node->stop;

done_testing();
