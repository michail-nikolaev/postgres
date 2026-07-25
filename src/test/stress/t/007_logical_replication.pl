# Copyright (c) 2026, PostgreSQL Global Development Group

# Stress test for logical replication apply with concurrent updates on
# the subscriber.
#
# The subscriber's table has a subscriber-only column (plus an index on
# a replicated column, to prevent HOT updates) that concurrent clients
# keep updating while the publisher churns the replicated columns.
# Row lookup during apply must reliably find the target row even though
# it is being moved around by the concurrent local updates.
#
# Two phases:
# - update phase: the publisher updates the replicated column;
#   afterwards, the replicated column on the subscriber must match the
#   publisher exactly, and no update_missing conflict may have been
#   logged (the row always exists on the subscriber).
# - delete phase: the publisher deletes all the rows; afterwards, the
#   subscriber's table must be empty, and no delete_missing conflict
#   may have been logged.
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
	'skipping disabled logical replication stress test');

# This file runs two pgbench phases, so give each one half of the
# calibrated total duration.
my $duration = 3 * $stressval;

# Few rows and many clients, to maximize contention.
my $nrows = 10;
my $nclients = 10;

#
# Test set-up
#
my $node_publisher = PostgreSQL::Test::Cluster->new('publisher');
$node_publisher->init(allows_streaming => 'logical');
$node_publisher->append_conf('postgresql.conf',
	'track_commit_timestamp = on');
$node_publisher->start;

my $node_subscriber = PostgreSQL::Test::Cluster->new('subscriber');
$node_subscriber->init;
$node_subscriber->append_conf('postgresql.conf',
	'track_commit_timestamp = on');
$node_subscriber->start;

$node_publisher->safe_psql('postgres',
	q(CREATE TABLE tbl(a int PRIMARY KEY, data_pub int)));

# Note the additional subscriber-only column, and the additional index
# preventing HOT updates.
$node_subscriber->safe_psql(
	'postgres', q(
	CREATE TABLE tbl(a int PRIMARY KEY, data_pub int, data_sub int DEFAULT 0);
	CREATE INDEX data_index ON tbl(data_pub);
));

$node_publisher->safe_psql('postgres',
	qq(INSERT INTO tbl SELECT i, i FROM generate_series(1, $nrows) i));

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

# Prepare pgbench scripts: the subscriber updates only the
# subscriber-only column, the publisher updates the replicated column
# resp. deletes rows.
my $sub_sql = $node_subscriber->basedir . '/sub_update.sql';
my $pub_update_sql = $node_publisher->basedir . '/pub_update.sql';
my $pub_delete_sql = $node_publisher->basedir . '/pub_delete.sql';

PostgreSQL::Test::Utils::append_to_file($sub_sql,
	qq(\\set num random(1, $nrows)
UPDATE tbl SET data_sub = data_sub + 1 WHERE a = :num;
));
PostgreSQL::Test::Utils::append_to_file($pub_update_sql,
	qq(\\set num random(1, $nrows)
UPDATE tbl SET data_pub = data_pub + 1 WHERE a = :num;
));
PostgreSQL::Test::Utils::append_to_file($pub_delete_sql,
	qq(\\set num random(1, $nrows)
DELETE FROM tbl WHERE a = :num;
));

# Runs pgbench on both nodes concurrently, with the given script files,
# and checks that both complete successfully.
sub concurrent_pgbench
{
	my ($pub_file, $sub_file, $test_name) = @_;
	local $Test::Builder::Level = $Test::Builder::Level + 1;

	my @common = (
		'pgbench', '--no-vacuum', "--client=$nclients", '--jobs=4',
		'--exit-on-abort', '-T', $duration);
	my @pub_cmd = (
		@common, '-p', $node_publisher->port,
		'-h', $node_publisher->host, '-f', $pub_file, 'postgres');
	my @sub_cmd = (
		@common, '-p', $node_subscriber->port,
		'-h', $node_subscriber->host, '-f', $sub_file, 'postgres');

	my ($pub_out, $pub_err, $sub_out, $sub_err) = ('', '', '', '');
	my $pub_h = start \@pub_cmd, '>', \$pub_out, '2>', \$pub_err;
	my $sub_h = start \@sub_cmd, '>', \$sub_out, '2>', \$sub_err;
	finish $pub_h;
	finish $sub_h;

	like($pub_out, qr/actually processed/, "$test_name: publisher pgbench");
	is($pub_err, '', "$test_name: publisher pgbench no stderr");
	like($sub_out, qr/actually processed/, "$test_name: subscriber pgbench");
	is($sub_err, '', "$test_name: subscriber pgbench no stderr");
	return;
}

my $log_offset = -s $node_subscriber->logfile;

#
# Update phase.  The rows are never deleted, so every replicated update
# must find its target row: an update_missing conflict would mean that
# the apply worker failed to look it up.
#
concurrent_pgbench($pub_update_sql, $sub_sql, 'update phase');

$node_publisher->wait_for_catchup($appname);

my $pub_data = $node_publisher->safe_psql('postgres',
	q(SELECT a, data_pub FROM tbl ORDER BY a));
my $sub_data = $node_subscriber->safe_psql('postgres',
	q(SELECT a, data_pub FROM tbl ORDER BY a));
is($sub_data, $pub_data, 'replicated column matches after update churn');

ok( !$node_subscriber->log_contains(
		qr/conflict=update_missing/, $log_offset),
	'no update_missing conflict logged');

#
# Delete phase.  The subscriber never deletes rows, so every replicated
# delete must find its target row: a delete_missing conflict would mean
# that the apply worker failed to look it up, and the row would survive
# on the subscriber.
#
concurrent_pgbench($pub_delete_sql, $sub_sql, 'delete phase');

# Remove any remaining rows; once caught up, the subscriber must have
# none either.
$node_publisher->safe_psql('postgres', q(DELETE FROM tbl));
$node_publisher->wait_for_catchup($appname);

my $sub_count = $node_subscriber->safe_psql('postgres',
	q(SELECT COUNT(*) FROM tbl));
is($sub_count, '0', 'no rows survive on subscriber after delete churn');

ok( !$node_subscriber->log_contains(
		qr/conflict=delete_missing/, $log_offset),
	'no delete_missing conflict logged');

$node_subscriber->stop;
$node_publisher->stop;

done_testing();
