# Copyright (c) 2025, PostgreSQL Global Development Group

# Show that the SNAPSHOT_DIRTY race is reachable with an ordinary workload -
# no injection points, no artificially widened window - and that its damage is
# permanent.
#
# Two pgbench runs work on the same rows at the same time: one on the
# publisher, one on the subscriber updating a column that exists only locally.
# The two never touch the same column, so there is no logical conflict at all.
#
# Phase 1 measures how often the apply worker loses a tuple.  It uses plain
# UPDATEs, which is the benign case: logical replication ships the whole new
# row, so the next update of the same row silently repairs the damage and the
# final state still converges.  The count is the interesting part.
#
# Phase 2 removes that accidental self-healing by using an operation that
# cannot be replayed: DELETE followed by re-INSERT of the same key, the shape
# of any queue or refresh workload.  A lost DELETE leaves a row on the
# subscriber that the publisher no longer has, and the very next INSERT of that
# key can then never be applied.
#
# Not intended to be committed: it is heavy and, being a real race, the number
# of hits varies from run to run.
use strict;
use warnings FATAL => 'all';
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use IPC::Run qw(start finish);
use Test::More;
use Time::HiRes qw(usleep);

###############################
# Setup
###############################

my $node_publisher = PostgreSQL::Test::Cluster->new('publisher');
$node_publisher->init(allows_streaming => 'logical');
$node_publisher->append_conf('postgresql.conf',
	qq(track_commit_timestamp = on));
$node_publisher->start;

my $node_subscriber = PostgreSQL::Test::Cluster->new('subscriber');
$node_subscriber->init;
$node_subscriber->append_conf('postgresql.conf',
	qq(track_commit_timestamp = on));
$node_subscriber->start;

$node_publisher->safe_psql('postgres',
	"CREATE TABLE tbl(a int PRIMARY key, data_pub int);");

# data_sub exists only on the subscriber and is the column the local pgbench
# updates.  Indexing it makes those updates non-HOT, so each one inserts a new
# entry into the primary key index - the index the apply worker is scanning.
$node_subscriber->safe_psql(
	'postgres',
	"CREATE TABLE tbl(a int PRIMARY key, data_pub int, data_sub int default 0);
	 CREATE INDEX data_sub_index ON tbl(data_sub);");

my $publisher_connstr = $node_publisher->connstr . ' dbname=postgres';
$node_publisher->safe_psql('postgres',
	"CREATE PUBLICATION tap_pub FOR TABLE tbl");

my $appname = 'tap_sub';
$node_subscriber->safe_psql(
	'postgres',
	"CREATE SUBSCRIPTION tap_sub
	 CONNECTION '$publisher_connstr application_name=$appname'
	 PUBLICATION tap_pub");

# Few rows on purpose: the race needs two sessions working on the same row, so
# a small hot set is what makes it show up in a short run.
my $num_rows = 10;
my $num_updates = 10000;
my $num_clients = 10;

$node_publisher->safe_psql('postgres',
	"INSERT INTO tbl SELECT i, 0 FROM generate_series(1,$num_rows) i");

$node_subscriber->wait_for_subscription_sync($node_publisher, $appname);

# Run one pgbench against each node at the same time and wait for both.
#
# The publisher side takes a client count of its own: the apply worker is
# single threaded, so the race does not need a concurrent publisher, and a
# workload that recycles keys needs one client to avoid conflicting with
# itself.
sub run_both_pgbench
{
	my ($pub_script, $pub_clients, $sub_script) = @_;

	my $pub_file = $node_publisher->basedir . '/pub.sql';
	my $sub_file = $node_subscriber->basedir . '/sub.sql';

	open my $fh1, '>', $pub_file or die $!;
	print $fh1 $pub_script;
	close $fh1;

	open my $fh2, '>', $sub_file or die $!;
	print $fh2 $sub_script;
	close $fh2;

	my $pub_jobs = $pub_clients < 4 ? $pub_clients : 4;

	my @pub_cmd = (
		'pgbench', '--no-vacuum',
		"--client=$pub_clients", "--jobs=$pub_jobs",
		'--exit-on-abort', "--transactions=$num_updates",
		'-p', $node_publisher->port,
		'-h', $node_publisher->host,
		'-f', $pub_file,
		'postgres');
	my @sub_cmd = (
		'pgbench', '--no-vacuum',
		"--client=$num_clients", '--jobs=4',
		'--exit-on-abort', "--transactions=$num_updates",
		'-p', $node_subscriber->port,
		'-h', $node_subscriber->host,
		'-f', $sub_file,
		'postgres');

	my ($pub_out, $pub_err, $sub_out, $sub_err) = ('', '', '', '');
	my $pub_h = start \@pub_cmd, '>', \$pub_out, '2>', \$pub_err;
	my $sub_h = start \@sub_cmd, '>', \$sub_out, '2>', \$sub_err;

	finish $pub_h;
	finish $sub_h;

	return ($pub_out, $sub_out);
}

# The local workload is the same in both phases.
my $local_update =
  "\\set num random(1,$num_rows)\nUPDATE tbl SET data_sub = data_sub + 1 WHERE a = :num;\n";

###########################################################
# Phase 1: how often does the apply worker lose a tuple?
###########################################################

my $phase1_offset = -s $node_subscriber->logfile;

my ($pub_out, $sub_out) = run_both_pgbench(
	"\\set num random(1,$num_rows)\nUPDATE tbl SET data_pub = data_pub + 1 WHERE a = :num;\n",
	$num_clients, $local_update);

like($pub_out, qr/actually processed/,
	'phase 1: publisher pgbench completed');
like(
	$sub_out,
	qr/actually processed/,
	'phase 1: subscriber pgbench completed');

$node_publisher->wait_for_catchup($appname);

my $phase1_log =
  PostgreSQL::Test::Utils::slurp_file($node_subscriber->logfile,
	$phase1_offset);
my $update_missing = () = $phase1_log =~ /conflict=update_missing/g;

diag(   "phase 1: $update_missing UPDATE(s) out of "
	  . ($num_updates * $num_clients)
	  . " were reported as update_missing and skipped");

is($update_missing, 0, 'phase 1: no update was reported as update_missing');

# Each row is repeatedly overwritten with a full row image, so a skipped UPDATE
# is repaired by the next one and the final state converges anyway.  That is
# luck, not a guarantee - phase 2 removes the luck.
is( $node_subscriber->safe_psql(
		'postgres', 'SELECT count(*), sum(data_pub) FROM tbl'),
	$node_publisher->safe_psql(
		'postgres', 'SELECT count(*), sum(data_pub) FROM tbl'),
	'phase 1: repeated full-row updates hid the damage');

###########################################################
# Phase 2: the same race against an operation that cannot
# be replayed
###########################################################

my $phase2_offset = -s $node_subscriber->logfile;

($pub_out, $sub_out) = run_both_pgbench(
	"\\set num random(1,$num_rows)\n"
	  . "BEGIN;\n"
	  . "DELETE FROM tbl WHERE a = :num;\n"
	  . "INSERT INTO tbl(a, data_pub) VALUES (:num, :num);\n"
	  . "END;\n",
	1, $local_update);

like($pub_out, qr/actually processed/,
	'phase 2: publisher pgbench completed');
like(
	$sub_out,
	qr/actually processed/,
	'phase 2: subscriber pgbench completed');

# A lost DELETE leaves a row behind that the publisher does not have, and the
# re-INSERT of that key in the same transaction then fails for good.  Poll for
# either outcome instead of waiting for a catch-up that may never come.
my $break_re =
  qr/ERROR:  conflict detected on relation \"public.tbl\": conflict=(insert_exists|multiple_unique_conflicts)/;
my $deadline = time() + $PostgreSQL::Test::Utils::timeout_default;
my $pub_state = $node_publisher->safe_psql('postgres',
	'SELECT count(*), coalesce(sum(a),0), coalesce(sum(data_pub),0) FROM tbl'
);
my $sub_state = '';
my $broken = 0;

while (time() < $deadline)
{
	$broken = $node_subscriber->log_contains($break_re, $phase2_offset);
	last if $broken;

	$sub_state = $node_subscriber->safe_psql('postgres',
		'SELECT count(*), coalesce(sum(a),0), coalesce(sum(data_pub),0) FROM tbl'
	);
	last if $sub_state eq $pub_state;

	usleep(100_000);
}

my $phase2_log =
  PostgreSQL::Test::Utils::slurp_file($node_subscriber->logfile,
	$phase2_offset);
my $delete_missing = () = $phase2_log =~ /conflict=delete_missing/g;

diag(   "phase 2: $delete_missing DELETE(s) were reported as delete_missing "
	  . "and skipped");

if ($broken)
{
	my $failures = () = $phase2_log =~ /$break_re/g;
	diag(   "phase 2: the apply worker is stuck, it has already rejected the "
		  . "same transaction $failures time(s); the subscription cannot "
		  . "advance without manual intervention");
}

is($delete_missing, 0, 'phase 2: no delete was reported as delete_missing');

ok(!$broken, 'phase 2: the subscription is still able to apply changes');

is($sub_state, $pub_state, 'phase 2: subscriber still matches the publisher');

done_testing();
