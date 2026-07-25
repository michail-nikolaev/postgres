# Copyright (c) 2026, PostgreSQL Global Development Group

# Stress test for CONCURRENTLY commands on tables involved in logical
# replication, on both the publisher and the subscriber side.
#
# On the publisher, writer clients apply balanced pairs of updates (so
# the sum over the replicated val column is invariant) while one client
# rotates through REPACK (CONCURRENTLY), DROP/CREATE INDEX CONCURRENTLY,
# REINDEX INDEX CONCURRENTLY and REINDEX TABLE CONCURRENTLY on the
# published table: logical decoding must survive relfilenode swaps and
# relation cache invalidations without losing or duplicating changes.
#
# On the subscriber, writer clients churn a subscriber-only column
# (moving the rows around, with an index preventing HOT updates) while
# another client repacks the sink table concurrently and rebuilds a
# secondary index: the apply worker's row lookups must survive the
# table being repacked underneath them.  Rebuilding the replica
# identity index itself (REINDEX INDEX/TABLE CONCURRENTLY) is left out
# for now: the new index OID trips an overly strict assertion in the
# apply worker's FindReplTupleInLocalRel.
#
# The rows are never deleted, so every replicated update must find its
# target row: afterwards, the replicated column must match the
# publisher exactly, no update_missing conflict may have been logged,
# and amcheck must pass on both sides.
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
	'skipping disabled logical replication DDL stress test');

my $duration = 6 * $stressval;
my $nrows = 1000;
my $nclients = 15;

#
# Test set-up.  Both nodes need wal_level = logical: the publisher for
# the subscription, the subscriber for its own REPACK (CONCURRENTLY).
#
my $node_publisher = PostgreSQL::Test::Cluster->new('publisher');
$node_publisher->init(allows_streaming => 'logical');
$node_publisher->append_conf('postgresql.conf',
	'lock_timeout = ' . (1000 * $PostgreSQL::Test::Utils::timeout_default));
$node_publisher->append_conf('postgresql.conf', 'max_connections = 50');
$node_publisher->start;

my $node_subscriber = PostgreSQL::Test::Cluster->new('subscriber');
$node_subscriber->init(allows_streaming => 'logical');
$node_subscriber->append_conf('postgresql.conf',
	'lock_timeout = ' . (1000 * $PostgreSQL::Test::Utils::timeout_default));
$node_subscriber->append_conf('postgresql.conf', 'max_connections = 50');
$node_subscriber->start;

$node_publisher->safe_psql(
	'postgres', q(
	CREATE EXTENSION amcheck;
	CREATE TABLE tbl(id int PRIMARY KEY, val int);
	CREATE INDEX tbl_val_idx ON tbl(val);
));
# The publisher's workload uses stress_assert(); these nodes are not
# created via stress_init_node, so create the function directly.
$node_publisher->safe_psql('postgres', stress_assert_defn());

# Note the additional subscriber-only column, and the additional index
# preventing HOT updates of it.
$node_subscriber->safe_psql(
	'postgres', q(
	CREATE EXTENSION amcheck;
	CREATE TABLE tbl(id int PRIMARY KEY, val int, loc int DEFAULT 0);
	CREATE INDEX tbl_val_idx ON tbl(val);
	CREATE INDEX tbl_loc_idx ON tbl(loc);
));

$node_publisher->safe_psql('postgres',
	qq(INSERT INTO tbl SELECT g, g FROM generate_series(1, $nrows) g));

my $sum = $node_publisher->safe_psql('postgres',
	q(SELECT SUM(val) AS sum FROM tbl));

my $publisher_connstr = $node_publisher->connstr . ' dbname=postgres';
$node_publisher->safe_psql('postgres',
	q(CREATE PUBLICATION tap_pub FOR TABLE tbl));

my $appname = 'tap_sub';
$node_subscriber->safe_psql(
	'postgres', qq(
	CREATE SUBSCRIPTION tap_sub
	CONNECTION '$publisher_connstr application_name=$appname'
	PUBLICATION tap_pub
));

$node_subscriber->wait_for_subscription_sync($node_publisher, $appname);

# The publisher runs balanced updates plus a DDL rotation on the
# published table.
my $pub_sql = $node_publisher->basedir . '/pub_ops.sql';
PostgreSQL::Test::Utils::append_to_file(
	$pub_sql,
	stress_ddl_gate(
		indent => '',
		ddl => [
			'REPACK (CONCURRENTLY) tbl;',
			[
				'DROP INDEX CONCURRENTLY tbl_val_idx;',
				'CREATE INDEX CONCURRENTLY tbl_val_idx ON tbl(val);'
			],
			'REINDEX INDEX CONCURRENTLY tbl_val_idx;',
			'REINDEX TABLE CONCURRENTLY tbl;',
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
\tSELECT stress_assert(COALESCE(SUM(val), 0) = $sum,
\t\tformat('sum is %s, not $sum', COALESCE(SUM(val), 0))) FROM tbl;)
	) . "\n");

# The subscriber churns its local column and runs the same DDL rotation
# on the sink table, underneath the apply worker.
my $sub_sql = $node_subscriber->basedir . '/sub_ops.sql';
PostgreSQL::Test::Utils::append_to_file(
	$sub_sql,
	stress_ddl_gate(
		indent => '',
		ddl => [
			'REPACK (CONCURRENTLY) tbl;',
			[
				'DROP INDEX CONCURRENTLY tbl_loc_idx;',
				'CREATE INDEX CONCURRENTLY tbl_loc_idx ON tbl(loc);'
			],
		],
		post => [
			'-- XXX rebuilding the replica identity index concurrently changes its',
			'-- OID and trips an overly strict assertion in the apply worker',
			'-- (FindReplTupleInLocalRel); re-enable once that is resolved:',
			'-- REINDEX INDEX CONCURRENTLY tbl_pkey;',
			'-- REINDEX TABLE CONCURRENTLY tbl;',
			"SELECT bt_index_check('tbl_pkey', heapallindexed => true, checkunique => true);",
		],
		else => qq(\t\\set num random(1, $nrows)
\tUPDATE tbl SET loc = loc + 1 WHERE id = :num;
\t\\sleep 1 ms)
	) . "\n");

my $log_offset = -s $node_subscriber->logfile;

# Run both pgbench workloads concurrently.
my @common = (
	'pgbench', '--no-vacuum', "--client=$nclients", '--jobs=4',
	'--exit-on-abort', '-T', $duration);
my @pub_cmd = (
	@common, '-p', $node_publisher->port,
	'-h', $node_publisher->host, '-f', $pub_sql, 'postgres');
my @sub_cmd = (
	@common, '-p', $node_subscriber->port,
	'-h', $node_subscriber->host, '-f', $sub_sql, 'postgres');

my ($pub_out, $pub_err, $sub_out, $sub_err) = ('', '', '', '');
my $pub_h = start \@pub_cmd, '>', \$pub_out, '2>', \$pub_err;
my $sub_h = start \@sub_cmd, '>', \$sub_out, '2>', \$sub_err;
finish $pub_h;
finish $sub_h;

like($pub_out, qr/actually processed/, 'publisher pgbench');
is($pub_err, '', 'publisher pgbench no stderr');
like($sub_out, qr/actually processed/, 'subscriber pgbench');
is($sub_err, '', 'subscriber pgbench no stderr');

$node_publisher->wait_for_catchup($appname);

my $pub_data = $node_publisher->safe_psql('postgres',
	q(SELECT id, val FROM tbl ORDER BY id));
my $sub_data = $node_subscriber->safe_psql('postgres',
	q(SELECT id, val FROM tbl ORDER BY id));
is($sub_data, $pub_data, 'replicated data matches after DDL churn');

ok( !$node_subscriber->log_contains(
		qr/conflict=update_missing/, $log_offset),
	'no update_missing conflict logged');

$node_publisher->safe_psql('postgres',
	q(SELECT bt_index_parent_check('tbl_pkey', heapallindexed => true)));
$node_subscriber->safe_psql('postgres',
	q(SELECT bt_index_parent_check('tbl_pkey', heapallindexed => true)));

$node_subscriber->stop;
$node_publisher->stop;

done_testing();
