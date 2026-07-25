# Copyright (c) 2026, PostgreSQL Global Development Group

# Stress test for CONCURRENTLY commands on the primary while a hot
# standby serves queries.
#
# On the primary, writer clients apply balanced pairs of updates (so
# the sum over the val column is invariant at every commit) while one
# client rotates through REPACK (CONCURRENTLY), DROP/CREATE INDEX
# CONCURRENTLY, REINDEX INDEX CONCURRENTLY and REINDEX TABLE
# CONCURRENTLY.  The primary runs with wal_level = replica, so the
# REPACK also exercises dynamic activation of logical decoding with a
# standby attached.
#
# The standby concurrently runs reader clients that verify the sum
# invariant and occasionally check an index with amcheck, while
# replaying the DDL churn.  Replay is allowed to cancel a reader it
# conflicts with (see max_standby_streaming_delay below); pgbench
# retries the transaction when that happens, so any other SQL error or
# broken invariant on the standby still fails the test.
#
# Afterwards, the replayed data must match the primary exactly, and the
# standby must survive promotion with the invariant intact and its
# indexes passing amcheck.
#
# This is also the regression test for the planner's handling of an
# index that replay drops underneath a standby reader: before that was
# fixed, a reader here would fail with "could not open relation with OID
# <n>" about one run in five at stress_concurrently=4, while the primary
# was running DROP INDEX CONCURRENTLY or REINDEX CONCURRENTLY.
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
	'skipping disabled hot standby stress test');

my $duration = 6 * $stressval;
my $nrows = 10_000;

#
# Test set-up
#
my $primary = PostgreSQL::Test::Cluster->new('primary');
$primary->init(allows_streaming => 1);
$primary->append_conf('postgresql.conf',
	'lock_timeout = ' . (1000 * $PostgreSQL::Test::Utils::timeout_default));
$primary->append_conf('postgresql.conf', 'max_connections = 50');
$primary->start;

$primary->safe_psql(
	'postgres', qq(
	CREATE EXTENSION amcheck;
	CREATE TABLE tbl(id int PRIMARY KEY, val int);
	CREATE INDEX tbl_val_idx ON tbl(val);
	INSERT INTO tbl SELECT g, g FROM generate_series(1, $nrows) g;
));
# Both the primary and the standby workloads use stress_assert(); create
# it on the primary before the backup so the standby inherits it too.
$primary->safe_psql('postgres', stress_assert_defn());

# The standby amchecks an index while replaying the primary's DDL.  A
# REINDEX CONCURRENTLY being replayed can leave the index named here
# momentarily in a state amcheck refuses ("cannot check index ..."); on
# a standby that is an expected transient, not corruption, so tolerate
# exactly that error and re-raise anything else.  Created on the primary
# so the standby inherits it through the backup.
$primary->safe_psql(
	'postgres', q(
	CREATE FUNCTION stress_amcheck_or_skip(idx text) RETURNS void
	LANGUAGE plpgsql AS $$
	BEGIN
		PERFORM bt_index_check(idx);
	EXCEPTION WHEN OTHERS THEN
		IF SQLERRM NOT LIKE 'cannot check index%' THEN
			RAISE;
		END IF;
	END; $$;
));

my $sum = $primary->safe_psql('postgres',
	q(SELECT SUM(val) AS sum FROM tbl));

$primary->backup('bkp');
my $standby = PostgreSQL::Test::Cluster->new('standby');
$standby->init_from_backup($primary, 'bkp', has_streaming => 1);
# Give replay a generous but finite grace period before it cancels a
# conflicting query.  It must not be -1: replay acquires the
# AccessExclusiveLocks the primary logged before it applies the records
# that conflict with a reader's snapshot, so with -1 it can end up
# waiting forever on a reader that is itself blocked on a lock replay is
# holding.  Nothing detects that cycle -- hot standby's deadlock check
# covers the startup process waiting *for* a lock, not waiting on a
# snapshot while holding one -- so it would only come apart when the
# reader hits lock_timeout, a long way further on.  A finite delay lets
# replay cancel the reader instead, which is the documented way out.
$standby->append_conf('postgresql.conf', 'max_standby_streaming_delay = 5s');
# These two only produce output when something actually waits, and they
# are what makes such a standoff readable after the fact.
$standby->append_conf('postgresql.conf', 'log_recovery_conflict_waits = on');
$standby->append_conf('postgresql.conf', 'log_lock_waits = on');
$standby->start;

# The primary runs balanced updates plus a DDL rotation.
my $primary_sql = $primary->basedir . '/primary_ops.sql';
PostgreSQL::Test::Utils::append_to_file(
	$primary_sql,
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

# The standby verifies the invariant while replaying the churn.  Note
# that bt_index_parent_check() is not allowed during recovery, so use
# bt_index_check() without heapallindexed, and only in one client at a
# time: its ShareLock could otherwise collide with replayed
# AccessExclusiveLocks of concurrent DDL for longer than needed.
my $standby_sql = $standby->basedir . '/standby_ops.sql';
PostgreSQL::Test::Utils::append_to_file(
	$standby_sql,
	stress_ddl_gate(
		indent => '',
		# Only one client at a time runs amcheck (see above); the gate is
		# used here just for that serialization, not for real DDL.
		ddl => ["SELECT stress_amcheck_or_skip('tbl_pkey');"],
		else => qq(\tBEGIN;
\tSELECT 1;
\t\\sleep 1 ms
\tSELECT stress_assert(COALESCE(SUM(val), 0) = $sum,
\t\tformat('sum is %s, not $sum', COALESCE(SUM(val), 0))) FROM tbl;
\tCOMMIT;)
	) . "\n");

# Run both pgbench workloads concurrently.
my @common = (
	'pgbench', '--no-vacuum', '--jobs=4', '--exit-on-abort', '-T',
	$duration);
my @primary_cmd = (
	@common, '--client=20', '-p', $primary->port,
	'-h', $primary->host, '-f', $primary_sql, 'postgres');
# A query cancelled by a recovery conflict fails with a serialization
# error, which is exactly what pgbench retries for; without this the
# first such cancellation would abort the run.  Retried transactions are
# only counted, not printed, so the stderr check below still holds.
my @standby_cmd = (
	@common, '--client=10', '--max-tries=100', '-p', $standby->port,
	'-h', $standby->host, '-f', $standby_sql, 'postgres');

my ($pri_out, $pri_err, $sby_out, $sby_err) = ('', '', '', '');
my $pri_h = start \@primary_cmd, '>', \$pri_out, '2>', \$pri_err;
my $sby_h = start \@standby_cmd, '>', \$sby_out, '2>', \$sby_err;
finish $pri_h;
finish $sby_h;

like($pri_out, qr/actually processed/, 'primary pgbench');
is($pri_err, '', 'primary pgbench no stderr');
like($sby_out, qr/actually processed/, 'standby pgbench');
is($sby_err, '', 'standby pgbench no stderr');

$primary->wait_for_catchup($standby);

my $primary_data = $primary->safe_psql('postgres',
	q(SELECT id, val FROM tbl ORDER BY id));
my $standby_data = $standby->safe_psql('postgres',
	q(SELECT id, val FROM tbl ORDER BY id));
is($standby_data, $primary_data, 'replayed data matches after DDL churn');

# The standby must survive promotion with everything intact.
$standby->promote;

my $promoted_sum = $standby->safe_psql('postgres',
	q(SELECT SUM(val) FROM tbl));
is($promoted_sum, $sum, 'sum invariant holds after promotion');

$standby->safe_psql(
	'postgres', q(
	SELECT bt_index_parent_check('tbl_pkey', heapallindexed => true);
	SELECT bt_index_parent_check('tbl_val_idx', heapallindexed => true);
));

$standby->stop;
$primary->stop;

done_testing();
