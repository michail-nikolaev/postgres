
# Copyright (c) 2026, PostgreSQL Global Development Group

# Apply searching for a row that moves behind its scan.
#
# With REPLICA IDENTITY FULL the apply worker searches the local table
# through whatever index it can use, walking the entries for the key and
# comparing whole tuples.  Scanned under a single dirty snapshot taken up
# front, that search can miss the row altogether: if the row is updated
# while the scan is under way and its new version is placed on a page the
# scan has already left, the old version is dead by the time the scan
# would reach it and the new one is behind, so nothing is found and the
# change is reported as missing.
#
# The page matters, not the offset.  A row that moves within a page the
# scan still has open is found anyway, because that page is re-read; a
# row that moves to an earlier page is gone for good.  Hence the wide,
# incompressible rows here: they spread the table over many pages, so the
# space freed at the start belongs to a page the scan has finished with
# by the time the row is moved into it.

use strict;
use warnings FATAL => 'all';

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;
use Time::HiRes qw(usleep);

if ($ENV{enable_injection_points} ne 'yes')
{
	plan skip_all => 'Injection points not supported by this build';
}

my $publisher = PostgreSQL::Test::Cluster->new('publisher');
$publisher->init(allows_streaming => 'logical');
$publisher->start;

my $subscriber = PostgreSQL::Test::Cluster->new('subscriber');
$subscriber->init;
$subscriber->start;

if (!$subscriber->check_extension('injection_points'))
{
	plan skip_all => 'Extension injection_points not installed';
}

$subscriber->safe_psql('postgres', 'CREATE EXTENSION injection_points');

# Every row shares the indexed value, so the search walks all of them.
my $ddl = q[
	CREATE TABLE t (k int, tag int, pad text);
	CREATE INDEX t_k ON t (k);
	ALTER TABLE t REPLICA IDENTITY FULL;
];
$publisher->safe_psql('postgres', $ddl);
$subscriber->safe_psql('postgres', $ddl);

# Incompressible padding, so the rows are wide on disk and the table
# covers many pages rather than one.
$publisher->safe_psql(
	'postgres', q[
	INSERT INTO t
		SELECT 1, g, repeat(md5(g::text || random()::text), 60)
		FROM generate_series(1, 60) g]);

$publisher->safe_psql('postgres', 'CREATE PUBLICATION pub FOR TABLE t');
my $connstr = $publisher->connstr . ' dbname=postgres';
$subscriber->safe_psql('postgres',
	"CREATE SUBSCRIPTION sub CONNECTION '$connstr' PUBLICATION pub");
$subscriber->wait_for_subscription_sync($publisher, 'sub');

is($subscriber->safe_psql('postgres', 'SELECT count(*) FROM t'),
	'60', 'initial data copied');

# Empty the first pages and reclaim them, so there is somewhere early in
# the heap for a row to be moved to.
$publisher->safe_psql('postgres', 'DELETE FROM t WHERE tag <= 12');
$publisher->wait_for_catchup('sub');
$subscriber->safe_psql('postgres', 'VACUUM t');

# Hold apply on the first entry it walks.  Everything it has passed by
# then is on pages above the freed ones.
$subscriber->safe_psql('postgres',
	q[SELECT injection_points_attach('repl-tuple-scan-in-progress', 'wait')]);

$publisher->safe_psql('postgres', 'UPDATE t SET tag = 550 WHERE tag = 55');

ok( $subscriber->poll_query_until(
		'postgres', q[
		SELECT count(*) > 0 FROM pg_stat_activity
		WHERE wait_event = 'repl-tuple-scan-in-progress']),
	'apply is walking the scan');

# Move the row apply is looking for into the reclaimed space.  Its values
# do not change, so it still matches what apply is searching for; only
# the page it lives on does.
$subscriber->safe_psql('postgres', 'UPDATE t SET pad = pad WHERE tag = 55');

is( $subscriber->safe_psql('postgres', q[SELECT ctid FROM t WHERE tag = 55]),
	'(0,1)',
	'the row moved to the first page, behind the scan');

# Stop the point catching later entries of the same scan, then release
# the backend it is already holding.
$subscriber->psql('postgres',
	q[SELECT injection_points_detach('repl-tuple-scan-in-progress')],
	on_error_stop => 0);
for (1 .. 100)
{
	my $waiting = $subscriber->safe_psql(
		'postgres', q[
		SELECT count(*) FROM pg_stat_activity
		WHERE wait_event = 'repl-tuple-scan-in-progress']);
	last if $waiting eq '0';
	$subscriber->psql('postgres',
		q[SELECT injection_points_wakeup('repl-tuple-scan-in-progress')],
		on_error_stop => 0);
	usleep(100_000);
}

$publisher->wait_for_catchup('sub');

is( $subscriber->safe_psql(
		'postgres', 'SELECT count(*) FROM t WHERE tag = 550'),
	'1',
	'the update reached the row that moved behind the scan');

is($subscriber->safe_psql('postgres', 'SELECT count(*) FROM t WHERE tag = 55'),
	'0', 'and the row it replaced is gone');

ok( !$subscriber->log_contains(qr/conflict=update_missing/, undef),
	'apply did not report the row as missing');

$subscriber->stop;
$publisher->stop;
done_testing();
