# Copyright (c) 2026, PostgreSQL Global Development Group

# Stress test for logical replication apply against heavy local write
# contention on the very rows being replicated.
#
# 007_logical_replication.pl has the subscriber churn a column the
# publisher does not replicate.  Here the subscriber instead rewrites
# the rows themselves, including transactions that delete a row and
# insert it again under the same key.  Such a transaction is atomic, so
# at every commit boundary the row is present exactly once -- but while
# it runs, the row's only live version belongs to an uncommitted
# transaction, which is precisely the state the apply worker's tuple
# lookup has to cope with.
#
# The publisher only ever updates the replicated column of rows that
# always exist, so every replicated change must find its target:
# afterwards the replicated column must match the publisher exactly,
# the subscriber must still hold one row per key, and no
# update_missing conflict may have been logged.
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
	'skipping disabled subscriber contention stress test');

my $duration = 6 * $stressval;

# A small key space, so that the publisher and the subscriber keep
# landing on the same rows.
my $nkeys = 100;
my $nclients = 10;

#
# Test set-up
#
my $node_publisher = PostgreSQL::Test::Cluster->new('publisher');
$node_publisher->init(allows_streaming => 'logical');
$node_publisher->append_conf('postgresql.conf', 'max_connections = 50');
$node_publisher->start;

my $node_subscriber = PostgreSQL::Test::Cluster->new('subscriber');
$node_subscriber->init;
$node_subscriber->append_conf('postgresql.conf', 'max_connections = 50');
$node_subscriber->start;

$node_publisher->safe_psql(
	'postgres', qq(
	CREATE TABLE tbl(id int PRIMARY KEY, pub_val int, local_val int DEFAULT 0);
	INSERT INTO tbl SELECT g, 0, 0 FROM generate_series(1, $nkeys) g;
));

# The extra index keeps the local updates from being HOT, so the rows
# really do move around underneath the apply worker.
$node_subscriber->safe_psql(
	'postgres', qq(
	CREATE TABLE tbl(id int PRIMARY KEY, pub_val int, local_val int DEFAULT 0);
	CREATE INDEX tbl_local_val_idx ON tbl(local_val);
	INSERT INTO tbl SELECT g, 0, 0 FROM generate_series(1, $nkeys) g;
));

my $publisher_connstr = $node_publisher->connstr . ' dbname=postgres';
$node_publisher->safe_psql('postgres',
	q(CREATE PUBLICATION tap_pub FOR TABLE tbl));

my $appname = 'tap_sub';
# copy_data => false: both sides already hold the same rows.
$node_subscriber->safe_psql(
	'postgres', qq(
	CREATE SUBSCRIPTION tap_sub
	CONNECTION '$publisher_connstr application_name=$appname'
	PUBLICATION tap_pub WITH (copy_data = false)
));

$node_subscriber->wait_for_subscription_sync($node_publisher, $appname);

# The publisher only updates the replicated column; rows are never
# removed there, so every change has a target row on the subscriber.
my $pub_sql = $node_publisher->basedir . '/pub_ops.sql';
PostgreSQL::Test::Utils::append_to_file($pub_sql,
	qq(\\set k random(1, $nkeys)
UPDATE tbl SET pub_val = pub_val + 1 WHERE id = :k;
));

# The subscriber rewrites the same rows locally.  The delete-and-insert
# variant leaves the row's only live version uncommitted for a moment,
# but the transaction is atomic, so the row is never actually missing.
my $sub_sql = $node_subscriber->basedir . '/sub_ops.sql';
PostgreSQL::Test::Utils::append_to_file(
	$sub_sql,
	qq(\\set k random(1, $nkeys)\n)
	  . stress_workload(
		indent => '',
		# Both rewrite the same rows the publisher is replicating into;
		# there is nothing to check locally, since convergence can only
		# be judged once both sides have settled (see the end of the
		# test).
		mutations => [
			qq(BEGIN;
			-- Serialize delete-and-insert against itself: two of them on
			-- the same key would collide on the primary key, which says
			-- nothing about replication.
			SELECT pg_advisory_xact_lock(:k);
			DELETE FROM tbl WHERE id = :k;
			INSERT INTO tbl(id, pub_val, local_val)
				SELECT :k, COALESCE(MAX(pub_val), 0), COALESCE(MAX(local_val), 0) + 1
				FROM tbl WHERE id = :k;
			COMMIT;),
			q(UPDATE tbl SET local_val = local_val + 1 WHERE id = :k;),
			q(UPDATE tbl SET local_val = local_val + 1 WHERE id = :k;),
		],
	  ) . "\n");

my $log_offset = -s $node_subscriber->logfile;

my @common = (
	'pgbench', '--no-vacuum', "--client=$nclients", "--jobs=$nclients",
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

# Every key must still be present exactly once on the subscriber: the
# delete-and-insert transactions are atomic.
is( $node_subscriber->safe_psql(
		'postgres', q(SELECT COUNT(*), COUNT(DISTINCT id) FROM tbl)),
	"$nkeys|$nkeys",
	'subscriber holds exactly one row per key');

# The replicated column must have converged.  Note that a row deleted
# and re-inserted locally carries its pub_val over, so no replicated
# update may be lost.
my $pub_data = $node_publisher->safe_psql('postgres',
	q(SELECT id, pub_val FROM tbl ORDER BY id));
my $sub_data = $node_subscriber->safe_psql('postgres',
	q(SELECT id, pub_val FROM tbl ORDER BY id));
is($sub_data, $pub_data, 'replicated column converged under local contention');

ok( !$node_subscriber->log_contains(qr/conflict=update_missing/, $log_offset),
	'no update_missing conflict logged');

$node_subscriber->stop;
$node_publisher->stop;

done_testing();
