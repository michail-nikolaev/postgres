
# Copyright (c) 2025, PostgreSQL Global Development Group

# Test INSERT ON CONFLICT DO UPDATE behavior concurrent with
# CREATE INDEX CONCURRENTLY and REINDEX CONCURRENTLY.
#
# These tests verify the fix for "duplicate key value violates unique constraint"
# errors that occurred when infer_arbiter_indexes() only considered indisvalid
# indexes, causing different transactions to use different arbiter indexes.

use strict;
use warnings FATAL => 'all';

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

if ($ENV{enable_injection_points} ne 'yes')
{
	plan skip_all => 'Injection points not supported by this build';
}

# Node initialization
my $node = PostgreSQL::Test::Cluster->new('node');
$node->init();
$node->start;

# Check if the extension injection_points is available
if (!$node->check_extension('injection_points'))
{
	plan skip_all => 'Extension injection_points not installed';
}

$node->safe_psql('postgres', 'CREATE EXTENSION injection_points;');

# Helper: Wait for a session to hit an injection point.
# Optional second argument is timeout in seconds.
# Returns true if found, false if timeout.
# On timeout, logs diagnostic information about all active queries.
sub wait_for_injection_point
{
	my ($node, $point_name, $timeout) = @_;
	$timeout //= 120;

	for (my $elapsed = 0; $elapsed < $timeout; $elapsed++)
	{
		my $pid = $node->safe_psql('postgres', qq[
			SELECT pid FROM pg_stat_activity
			WHERE wait_event_type = 'InjectionPoint'
			  AND wait_event = '$point_name'
			LIMIT 1;
		]);
		return 1 if $pid ne '';
		sleep(1);
	}

	# Timeout - report diagnostic information
	my $activity = $node->safe_psql('postgres', q[
		SELECT format('pid=%s, state=%s, wait_event_type=%s, wait_event=%s, backend_xmin=%s, backend_xid=%s, query=%s',
			pid, state, wait_event_type, wait_event, backend_xmin, backend_xid, left(query, 100))
		FROM pg_stat_activity
		ORDER BY pid;
	]);
	diag("wait_for_injection_point timeout waiting for: $point_name\n" .
		"Current queries in pg_stat_activity:\n$activity");

	return 0;
}

# Helper: Wait for a specific backend to become idle.
# Returns true if idle, false if timeout.
sub wait_for_idle
{
	my ($node, $pid, $timeout) = @_;
	$timeout //= 15;

	for (my $elapsed = 0; $elapsed < $timeout; $elapsed++)
	{
		my $state = $node->safe_psql('postgres', qq[
			SELECT state FROM pg_stat_activity WHERE pid = $pid;
		]);
		return 1 if $state eq 'idle';
		sleep(1);
	}
	return 0;
}

# Helper: Detach and wakeup an injection point
sub wakeup_injection_point
{
	my ($node, $point_name) = @_;
	$node->safe_psql(
		'postgres', qq[
SELECT injection_points_detach('$point_name');
SELECT injection_points_wakeup('$point_name');
]);
}

# Wait for any pending query to complete, capture stderr, and close the session.
# Returns the stderr output (excluding internal markers).
sub safe_quit
{
	my ($session) = @_;

	# Send a marker and wait for it to ensure any pending query completes
	my $banner = "safe_quit_marker";
	my $banner_match = qr/(^|\n)$banner\r?\n/;

	$session->{stdin} .= "\\echo $banner\n\\warn $banner\n";

	pump_until($session->{run}, $session->{timeout},
		\$session->{stdout}, $banner_match);
	pump_until($session->{run}, $session->{timeout},
		\$session->{stderr}, $banner_match);

	# Capture stderr (excluding the banner)
	my $stderr = $session->{stderr};
	$stderr =~ s/$banner_match//;

	# Close the session
	$session->quit;

	return $stderr;
}

###############################################################################
# Test 1: REINDEX CONCURRENTLY + UPSERT (wakeup at set-dead phase)
# Based on reindex-concurrently-upsert.spec
###############################################################################

$node->safe_psql(
	'postgres', q[
CREATE SCHEMA test;
CREATE UNLOGGED TABLE test.tbl (i int PRIMARY KEY, updated_at timestamp);
ALTER TABLE test.tbl SET (parallel_workers=0);
]);

# Create sessions with on_error_stop => 0 so psql doesn't exit on SQL errors.
# This allows us to collect stderr and detect errors after the test completes.
my $s1 = $node->background_psql('postgres', on_error_stop => 0);
my $s2 = $node->background_psql('postgres', on_error_stop => 0);
my $s3 = $node->background_psql('postgres', on_error_stop => 0);

# Setup injection points for each session
$s1->query_safe(q[
SELECT injection_points_set_local();
SELECT injection_points_attach('check-exclusion-or-unique-constraint-no-conflict', 'wait');
]);

$s2->query_safe(q[
SELECT injection_points_set_local();
SELECT injection_points_attach('exec-insert-before-insert-speculative', 'wait');
]);

$s3->query_safe(q[
SELECT injection_points_set_local();
SELECT injection_points_attach('reindex-relation-concurrently-before-set-dead', 'wait');
]);

# s3 starts REINDEX (will block on reindex-relation-concurrently-before-set-dead)
$s3->query_until(qr/starting_reindex/, q[
\echo starting_reindex
REINDEX INDEX CONCURRENTLY test.tbl_pkey;
]);

# Wait for s3 to hit injection point
ok(wait_for_injection_point($node, 'reindex-relation-concurrently-before-set-dead'));

# s1 starts UPSERT (will block on check-exclusion-or-unique-constraint-no-conflict)
$s1->query_until(qr/starting_upsert_s1/, q[
\echo starting_upsert_s1
INSERT INTO test.tbl VALUES (13,now()) ON CONFLICT (i) DO UPDATE SET updated_at = now();
]);

# Wait for s1 to hit injection point
ok(wait_for_injection_point($node, 'check-exclusion-or-unique-constraint-no-conflict'));

# Wakeup s3 to continue (reindex-relation-concurrently-before-set-dead)
wakeup_injection_point($node, 'reindex-relation-concurrently-before-set-dead');

# s2 starts UPSERT (will block on exec-insert-before-insert-speculative)
$s2->query_until(qr/starting_upsert_s2/, q[
\echo starting_upsert_s2
INSERT INTO test.tbl VALUES (13,now()) ON CONFLICT (i) DO UPDATE SET updated_at = now();
]);

# Wait for s2 to hit injection point
ok(wait_for_injection_point($node, 'exec-insert-before-insert-speculative'));

# Wakeup s1 (check-exclusion-or-unique-constraint-no-conflict)
wakeup_injection_point($node, 'check-exclusion-or-unique-constraint-no-conflict');

# Wakeup s2 (exec-insert-before-insert-speculative)
wakeup_injection_point($node, 'exec-insert-before-insert-speculative');

is(safe_quit($s1), '', 'Test 1 (REINDEX set-dead): session s1 quit successfully');
is(safe_quit($s2), '', 'Test 1 (REINDEX set-dead): session s2 quit successfully');
is(safe_quit($s3), '', 'Test 1 (REINDEX set-dead): session s3 quit successfully');

# Cleanup test 1
$node->safe_psql('postgres', 'DROP SCHEMA test CASCADE;');

###############################################################################
# Test 2: REINDEX CONCURRENTLY + UPSERT (wakeup at swap phase)
###############################################################################

$node->safe_psql(
	'postgres', q[
CREATE SCHEMA test;
CREATE UNLOGGED TABLE test.tbl (i int PRIMARY KEY, updated_at timestamp);
ALTER TABLE test.tbl SET (parallel_workers=0);
]);

$s1 = $node->background_psql('postgres', on_error_stop => 0);
$s2 = $node->background_psql('postgres', on_error_stop => 0);
$s3 = $node->background_psql('postgres', on_error_stop => 0);

$s1->query_safe(q[
SELECT injection_points_set_local();
SELECT injection_points_attach('check-exclusion-or-unique-constraint-no-conflict', 'wait');
]);

$s2->query_safe(q[
SELECT injection_points_set_local();
SELECT injection_points_attach('exec-insert-before-insert-speculative', 'wait');
]);

$s3->query_safe(q[
SELECT injection_points_set_local();
SELECT injection_points_attach('reindex-relation-concurrently-before-swap', 'wait');
]);

$s3->query_until(qr/starting_reindex/, q[
\echo starting_reindex
REINDEX INDEX CONCURRENTLY test.tbl_pkey;
]);

ok(wait_for_injection_point($node, 'reindex-relation-concurrently-before-swap'));

$s1->query_until(qr/starting_upsert_s1/, q[
\echo starting_upsert_s1
INSERT INTO test.tbl VALUES (13,now()) ON CONFLICT (i) DO UPDATE SET updated_at = now();
]);

ok(wait_for_injection_point($node, 'check-exclusion-or-unique-constraint-no-conflict'));

wakeup_injection_point($node, 'reindex-relation-concurrently-before-swap');

$s2->query_until(qr/starting_upsert_s2/, q[
\echo starting_upsert_s2
INSERT INTO test.tbl VALUES (13,now()) ON CONFLICT (i) DO UPDATE SET updated_at = now();
]);

ok(wait_for_injection_point($node, 'exec-insert-before-insert-speculative'));

wakeup_injection_point($node, 'exec-insert-before-insert-speculative');
wakeup_injection_point($node, 'check-exclusion-or-unique-constraint-no-conflict');

is(safe_quit($s1), '', 'Test 2 (REINDEX swap): session s1 quit successfully');
is(safe_quit($s2), '', 'Test 2 (REINDEX swap): session s2 quit successfully');
is(safe_quit($s3), '', 'Test 2 (REINDEX swap): session s3 quit successfully');

$node->safe_psql('postgres', 'DROP SCHEMA test CASCADE;');

###############################################################################
# Test 2b: REINDEX CONCURRENTLY + UPSERT (permutation 3: s1 wakes before reindex)
# Different timing: s2 starts, then s1 wakes, then reindex wakes, then s2 wakes
###############################################################################

$node->safe_psql(
	'postgres', q[
CREATE SCHEMA test;
CREATE UNLOGGED TABLE test.tbl (i int PRIMARY KEY, updated_at timestamp);
ALTER TABLE test.tbl SET (parallel_workers=0);
]);

$s1 = $node->background_psql('postgres', on_error_stop => 0);
$s2 = $node->background_psql('postgres', on_error_stop => 0);
$s3 = $node->background_psql('postgres', on_error_stop => 0);

$s1->query_safe(q[
SELECT injection_points_set_local();
SELECT injection_points_attach('check-exclusion-or-unique-constraint-no-conflict', 'wait');
]);

$s2->query_safe(q[
SELECT injection_points_set_local();
SELECT injection_points_attach('exec-insert-before-insert-speculative', 'wait');
]);

$s3->query_safe(q[
SELECT injection_points_set_local();
SELECT injection_points_attach('reindex-relation-concurrently-before-set-dead', 'wait');
]);

$s3->query_until(qr/starting_reindex/, q[
\echo starting_reindex
REINDEX INDEX CONCURRENTLY test.tbl_pkey;
]);

ok(wait_for_injection_point($node, 'reindex-relation-concurrently-before-set-dead'));

$s1->query_until(qr/starting_upsert_s1/, q[
\echo starting_upsert_s1
INSERT INTO test.tbl VALUES (13,now()) ON CONFLICT (i) DO UPDATE SET updated_at = now();
]);

ok(wait_for_injection_point($node, 'check-exclusion-or-unique-constraint-no-conflict'));

# Start s2 BEFORE waking reindex (key difference from permutation 1)
$s2->query_until(qr/starting_upsert_s2/, q[
\echo starting_upsert_s2
INSERT INTO test.tbl VALUES (13,now()) ON CONFLICT (i) DO UPDATE SET updated_at = now();
]);

ok(wait_for_injection_point($node, 'exec-insert-before-insert-speculative'));

# Wake s1 first, then reindex, then s2
wakeup_injection_point($node, 'check-exclusion-or-unique-constraint-no-conflict');
wakeup_injection_point($node, 'reindex-relation-concurrently-before-set-dead');
wakeup_injection_point($node, 'exec-insert-before-insert-speculative');

is(safe_quit($s1), '', 'Test 2b (REINDEX perm3): session s1 quit successfully');
is(safe_quit($s2), '', 'Test 2b (REINDEX perm3): session s2 quit successfully');
is(safe_quit($s3), '', 'Test 2b (REINDEX perm3): session s3 quit successfully');

$node->safe_psql('postgres', 'DROP SCHEMA test CASCADE;');

###############################################################################
# Test 3: REINDEX + UPSERT ON CONSTRAINT (set-dead phase)
# Based on reindex-concurrently-upsert-on-constraint.spec
###############################################################################

$node->safe_psql(
	'postgres', q[
CREATE SCHEMA test;
CREATE UNLOGGED TABLE test.tbl (i int PRIMARY KEY, updated_at timestamp);
ALTER TABLE test.tbl SET (parallel_workers=0);
]);

$s1 = $node->background_psql('postgres', on_error_stop => 0);
$s2 = $node->background_psql('postgres', on_error_stop => 0);
$s3 = $node->background_psql('postgres', on_error_stop => 0);

$s1->query_safe(q[
SELECT injection_points_set_local();
SELECT injection_points_attach('check-exclusion-or-unique-constraint-no-conflict', 'wait');
]);

$s2->query_safe(q[
SELECT injection_points_set_local();
SELECT injection_points_attach('exec-insert-before-insert-speculative', 'wait');
]);

$s3->query_safe(q[
SELECT injection_points_set_local();
SELECT injection_points_attach('reindex-relation-concurrently-before-set-dead', 'wait');
]);

$s3->query_until(qr/starting_reindex/, q[
\echo starting_reindex
REINDEX INDEX CONCURRENTLY test.tbl_pkey;
]);

ok(wait_for_injection_point($node, 'reindex-relation-concurrently-before-set-dead'));

$s1->query_until(qr/starting_upsert_s1/, q[
\echo starting_upsert_s1
INSERT INTO test.tbl VALUES (13, now()) ON CONFLICT ON CONSTRAINT tbl_pkey DO UPDATE SET updated_at = now();
]);

ok(wait_for_injection_point($node, 'check-exclusion-or-unique-constraint-no-conflict'));

wakeup_injection_point($node, 'reindex-relation-concurrently-before-set-dead');

$s2->query_until(qr/starting_upsert_s2/, q[
\echo starting_upsert_s2
INSERT INTO test.tbl VALUES (13, now()) ON CONFLICT ON CONSTRAINT tbl_pkey DO UPDATE SET updated_at = now();
]);

ok(wait_for_injection_point($node, 'exec-insert-before-insert-speculative'));

wakeup_injection_point($node, 'check-exclusion-or-unique-constraint-no-conflict');
wakeup_injection_point($node, 'exec-insert-before-insert-speculative');

is(safe_quit($s1), '', 'Test 3 (ON CONSTRAINT set-dead): session s1 quit successfully');
is(safe_quit($s2), '', 'Test 3 (ON CONSTRAINT set-dead): session s2 quit successfully');
is(safe_quit($s3), '', 'Test 3 (ON CONSTRAINT set-dead): session s3 quit successfully');

$node->safe_psql('postgres', 'DROP SCHEMA test CASCADE;');

###############################################################################
# Test 4: REINDEX + UPSERT ON CONSTRAINT (swap phase)
###############################################################################

$node->safe_psql(
	'postgres', q[
CREATE SCHEMA test;
CREATE UNLOGGED TABLE test.tbl (i int PRIMARY KEY, updated_at timestamp);
ALTER TABLE test.tbl SET (parallel_workers=0);
]);

$s1 = $node->background_psql('postgres', on_error_stop => 0);
$s2 = $node->background_psql('postgres', on_error_stop => 0);
$s3 = $node->background_psql('postgres', on_error_stop => 0);

$s1->query_safe(q[
SELECT injection_points_set_local();
SELECT injection_points_attach('check-exclusion-or-unique-constraint-no-conflict', 'wait');
]);

$s2->query_safe(q[
SELECT injection_points_set_local();
SELECT injection_points_attach('exec-insert-before-insert-speculative', 'wait');
]);

$s3->query_safe(q[
SELECT injection_points_set_local();
SELECT injection_points_attach('reindex-relation-concurrently-before-swap', 'wait');
]);

$s3->query_until(qr/starting_reindex/, q[
\echo starting_reindex
REINDEX INDEX CONCURRENTLY test.tbl_pkey;
]);

ok(wait_for_injection_point($node, 'reindex-relation-concurrently-before-swap'));

$s1->query_until(qr/starting_upsert_s1/, q[
\echo starting_upsert_s1
INSERT INTO test.tbl VALUES (13, now()) ON CONFLICT ON CONSTRAINT tbl_pkey DO UPDATE SET updated_at = now();
]);

ok(wait_for_injection_point($node, 'check-exclusion-or-unique-constraint-no-conflict'));

wakeup_injection_point($node, 'reindex-relation-concurrently-before-swap');

$s2->query_until(qr/starting_upsert_s2/, q[
\echo starting_upsert_s2
INSERT INTO test.tbl VALUES (13, now()) ON CONFLICT ON CONSTRAINT tbl_pkey DO UPDATE SET updated_at = now();
]);

ok(wait_for_injection_point($node, 'exec-insert-before-insert-speculative'));

wakeup_injection_point($node, 'exec-insert-before-insert-speculative');
wakeup_injection_point($node, 'check-exclusion-or-unique-constraint-no-conflict');

is(safe_quit($s1), '', 'Test 4 (ON CONSTRAINT swap): session s1 quit successfully');
is(safe_quit($s2), '', 'Test 4 (ON CONSTRAINT swap): session s2 quit successfully');
is(safe_quit($s3), '', 'Test 4 (ON CONSTRAINT swap): session s3 quit successfully');

$node->safe_psql('postgres', 'DROP SCHEMA test CASCADE;');

###############################################################################
# Test 4b: REINDEX + UPSERT ON CONSTRAINT (permutation 3: s1 wakes before reindex)
###############################################################################

$node->safe_psql(
	'postgres', q[
CREATE SCHEMA test;
CREATE UNLOGGED TABLE test.tbl (i int PRIMARY KEY, updated_at timestamp);
ALTER TABLE test.tbl SET (parallel_workers=0);
]);

$s1 = $node->background_psql('postgres', on_error_stop => 0);
$s2 = $node->background_psql('postgres', on_error_stop => 0);
$s3 = $node->background_psql('postgres', on_error_stop => 0);

$s1->query_safe(q[
SELECT injection_points_set_local();
SELECT injection_points_attach('check-exclusion-or-unique-constraint-no-conflict', 'wait');
]);

$s2->query_safe(q[
SELECT injection_points_set_local();
SELECT injection_points_attach('exec-insert-before-insert-speculative', 'wait');
]);

$s3->query_safe(q[
SELECT injection_points_set_local();
SELECT injection_points_attach('reindex-relation-concurrently-before-set-dead', 'wait');
]);

$s3->query_until(qr/starting_reindex/, q[
\echo starting_reindex
REINDEX INDEX CONCURRENTLY test.tbl_pkey;
]);

ok(wait_for_injection_point($node, 'reindex-relation-concurrently-before-set-dead'));

$s1->query_until(qr/starting_upsert_s1/, q[
\echo starting_upsert_s1
INSERT INTO test.tbl VALUES (13, now()) ON CONFLICT ON CONSTRAINT tbl_pkey DO UPDATE SET updated_at = now();
]);

ok(wait_for_injection_point($node, 'check-exclusion-or-unique-constraint-no-conflict'));

# Start s2 BEFORE waking reindex
$s2->query_until(qr/starting_upsert_s2/, q[
\echo starting_upsert_s2
INSERT INTO test.tbl VALUES (13, now()) ON CONFLICT ON CONSTRAINT tbl_pkey DO UPDATE SET updated_at = now();
]);

ok(wait_for_injection_point($node, 'exec-insert-before-insert-speculative'));

# Wake s1 first, then reindex, then s2
wakeup_injection_point($node, 'check-exclusion-or-unique-constraint-no-conflict');
wakeup_injection_point($node, 'reindex-relation-concurrently-before-set-dead');
wakeup_injection_point($node, 'exec-insert-before-insert-speculative');

is(safe_quit($s1), '', 'Test 4b (ON CONSTRAINT perm3): session s1 quit successfully');
is(safe_quit($s2), '', 'Test 4b (ON CONSTRAINT perm3): session s2 quit successfully');
is(safe_quit($s3), '', 'Test 4b (ON CONSTRAINT perm3): session s3 quit successfully');

$node->safe_psql('postgres', 'DROP SCHEMA test CASCADE;');

###############################################################################
# Test 5: REINDEX on partitioned table (set-dead phase)
# Based on reindex-concurrently-upsert-partitioned.spec
###############################################################################

$node->safe_psql(
	'postgres', q[
CREATE SCHEMA test;
CREATE TABLE test.tbl(i int primary key, updated_at timestamp) PARTITION BY RANGE (i);
CREATE TABLE test.tbl_partition PARTITION OF test.tbl
    FOR VALUES FROM (0) TO (10000)
    WITH (parallel_workers = 0);
]);

$s1 = $node->background_psql('postgres', on_error_stop => 0);
$s2 = $node->background_psql('postgres', on_error_stop => 0);
$s3 = $node->background_psql('postgres', on_error_stop => 0);

$s1->query_safe(q[
SELECT injection_points_set_local();
SELECT injection_points_attach('check-exclusion-or-unique-constraint-no-conflict', 'wait');
]);

$s2->query_safe(q[
SELECT injection_points_set_local();
SELECT injection_points_attach('exec-insert-before-insert-speculative', 'wait');
]);

$s3->query_safe(q[
SELECT injection_points_set_local();
SELECT injection_points_attach('reindex-relation-concurrently-before-set-dead', 'wait');
]);

$s3->query_until(qr/starting_reindex/, q[
\echo starting_reindex
REINDEX INDEX CONCURRENTLY test.tbl_partition_pkey;
]);

ok(wait_for_injection_point($node, 'reindex-relation-concurrently-before-set-dead'));

$s1->query_until(qr/starting_upsert_s1/, q[
\echo starting_upsert_s1
INSERT INTO test.tbl VALUES (13, now()) ON CONFLICT (i) DO UPDATE SET updated_at = now();
]);

ok(wait_for_injection_point($node, 'check-exclusion-or-unique-constraint-no-conflict'));

wakeup_injection_point($node, 'reindex-relation-concurrently-before-set-dead');

$s2->query_until(qr/starting_upsert_s2/, q[
\echo starting_upsert_s2
INSERT INTO test.tbl VALUES (13, now()) ON CONFLICT (i) DO UPDATE SET updated_at = now();
]);

ok(wait_for_injection_point($node, 'exec-insert-before-insert-speculative'));

wakeup_injection_point($node, 'check-exclusion-or-unique-constraint-no-conflict');
wakeup_injection_point($node, 'exec-insert-before-insert-speculative');

is(safe_quit($s1), '', 'Test 5 (partitioned set-dead): session s1 quit successfully');
is(safe_quit($s2), '', 'Test 5 (partitioned set-dead): session s2 quit successfully');
is(safe_quit($s3), '', 'Test 5 (partitioned set-dead): session s3 quit successfully');

$node->safe_psql('postgres', 'DROP SCHEMA test CASCADE;');

###############################################################################
# Test 6: REINDEX on partitioned table (swap phase)
###############################################################################

$node->safe_psql(
	'postgres', q[
CREATE SCHEMA test;
CREATE TABLE test.tbl(i int primary key, updated_at timestamp) PARTITION BY RANGE (i);
CREATE TABLE test.tbl_partition PARTITION OF test.tbl
    FOR VALUES FROM (0) TO (10000)
    WITH (parallel_workers = 0);
]);

$s1 = $node->background_psql('postgres', on_error_stop => 0);
$s2 = $node->background_psql('postgres', on_error_stop => 0);
$s3 = $node->background_psql('postgres', on_error_stop => 0);

$s1->query_safe(q[
SELECT injection_points_set_local();
SELECT injection_points_attach('check-exclusion-or-unique-constraint-no-conflict', 'wait');
]);

$s2->query_safe(q[
SELECT injection_points_set_local();
SELECT injection_points_attach('exec-insert-before-insert-speculative', 'wait');
]);

$s3->query_safe(q[
SELECT injection_points_set_local();
SELECT injection_points_attach('reindex-relation-concurrently-before-swap', 'wait');
]);

$s3->query_until(qr/starting_reindex/, q[
\echo starting_reindex
REINDEX INDEX CONCURRENTLY test.tbl_partition_pkey;
]);

ok(wait_for_injection_point($node, 'reindex-relation-concurrently-before-swap'));

$s1->query_until(qr/starting_upsert_s1/, q[
\echo starting_upsert_s1
INSERT INTO test.tbl VALUES (13, now()) ON CONFLICT (i) DO UPDATE SET updated_at = now();
]);

ok(wait_for_injection_point($node, 'check-exclusion-or-unique-constraint-no-conflict'));

wakeup_injection_point($node, 'reindex-relation-concurrently-before-swap');

$s2->query_until(qr/starting_upsert_s2/, q[
\echo starting_upsert_s2
INSERT INTO test.tbl VALUES (13, now()) ON CONFLICT (i) DO UPDATE SET updated_at = now();
]);

ok(wait_for_injection_point($node, 'exec-insert-before-insert-speculative'));

wakeup_injection_point($node, 'exec-insert-before-insert-speculative');
wakeup_injection_point($node, 'check-exclusion-or-unique-constraint-no-conflict');

is(safe_quit($s1), '', 'Test 6 (partitioned swap): session s1 quit successfully');
is(safe_quit($s2), '', 'Test 6 (partitioned swap): session s2 quit successfully');
is(safe_quit($s3), '', 'Test 6 (partitioned swap): session s3 quit successfully');

$node->safe_psql('postgres', 'DROP SCHEMA test CASCADE;');

###############################################################################
# Test 6b: REINDEX on partitioned table (permutation 3: s1 wakes before reindex)
###############################################################################

$node->safe_psql(
	'postgres', q[
CREATE SCHEMA test;
CREATE TABLE test.tbl(i int primary key, updated_at timestamp) PARTITION BY RANGE (i);
CREATE TABLE test.tbl_partition PARTITION OF test.tbl
    FOR VALUES FROM (0) TO (10000)
    WITH (parallel_workers = 0);
]);

$s1 = $node->background_psql('postgres', on_error_stop => 0);
$s2 = $node->background_psql('postgres', on_error_stop => 0);
$s3 = $node->background_psql('postgres', on_error_stop => 0);

$s1->query_safe(q[
SELECT injection_points_set_local();
SELECT injection_points_attach('check-exclusion-or-unique-constraint-no-conflict', 'wait');
]);

$s2->query_safe(q[
SELECT injection_points_set_local();
SELECT injection_points_attach('exec-insert-before-insert-speculative', 'wait');
]);

$s3->query_safe(q[
SELECT injection_points_set_local();
SELECT injection_points_attach('reindex-relation-concurrently-before-set-dead', 'wait');
]);

$s3->query_until(qr/starting_reindex/, q[
\echo starting_reindex
REINDEX INDEX CONCURRENTLY test.tbl_partition_pkey;
]);

ok(wait_for_injection_point($node, 'reindex-relation-concurrently-before-set-dead'));

$s1->query_until(qr/starting_upsert_s1/, q[
\echo starting_upsert_s1
INSERT INTO test.tbl VALUES (13, now()) ON CONFLICT (i) DO UPDATE SET updated_at = now();
]);

ok(wait_for_injection_point($node, 'check-exclusion-or-unique-constraint-no-conflict'));

# Start s2 BEFORE waking reindex
$s2->query_until(qr/starting_upsert_s2/, q[
\echo starting_upsert_s2
INSERT INTO test.tbl VALUES (13, now()) ON CONFLICT (i) DO UPDATE SET updated_at = now();
]);

ok(wait_for_injection_point($node, 'exec-insert-before-insert-speculative'));

# Wake s1 first, then reindex, then s2
wakeup_injection_point($node, 'check-exclusion-or-unique-constraint-no-conflict');
wakeup_injection_point($node, 'reindex-relation-concurrently-before-set-dead');
wakeup_injection_point($node, 'exec-insert-before-insert-speculative');

is(safe_quit($s1), '', 'Test 6b (partitioned perm3): session s1 quit successfully');
is(safe_quit($s2), '', 'Test 6b (partitioned perm3): session s2 quit successfully');
is(safe_quit($s3), '', 'Test 6b (partitioned perm3): session s3 quit successfully');

$node->safe_psql('postgres', 'DROP SCHEMA test CASCADE;');

###############################################################################
# Test 7: CREATE INDEX CONCURRENTLY + UPSERT
# Based on index-concurrently-upsert.spec
# Uses invalidate-catalog-snapshot-end to test catalog invalidation during UPSERT
###############################################################################

$node->safe_psql(
	'postgres', q[
CREATE SCHEMA test;
CREATE UNLOGGED TABLE test.tbl(i int primary key, updated_at timestamp);
ALTER TABLE test.tbl SET (parallel_workers=0);
]);

$s1 = $node->background_psql('postgres', on_error_stop => 0);
$s2 = $node->background_psql('postgres', on_error_stop => 0);
$s3 = $node->background_psql('postgres', on_error_stop => 0);

my $s1_pid = $s1->query_safe('SELECT pg_backend_pid()');

# s1 attaches BOTH injection points - the unique constraint check AND catalog snapshot
$s1->query_safe(q[
SELECT injection_points_set_local();
SELECT injection_points_attach('check-exclusion-or-unique-constraint-no-conflict', 'wait');
]);

$s1->query_until(qr/attaching_injection_point/, q[
\echo attaching_injection_point
SELECT injection_points_attach('invalidate-catalog-snapshot-end', 'wait');
]);
# In case of CLOBBER_CACHE_ALWAYS - s1 may hit the injection point during attach.
# Wait for s1 to become idle (attach completed) or wakeup if stuck on injection point.
if (!wait_for_idle($node, $s1_pid))
{
	ok(wait_for_injection_point($node, 'invalidate-catalog-snapshot-end'),
		'Test 7: s1 hit injection point during attach (CLOBBER_CACHE_ALWAYS)');
	$node->safe_psql('postgres', q[
		SELECT injection_points_wakeup('invalidate-catalog-snapshot-end');
	]);
}

$s2->query_safe(q[
SELECT injection_points_set_local();
SELECT injection_points_attach('exec-insert-before-insert-speculative', 'wait');
]);

$s3->query_safe(q[
SELECT injection_points_set_local();
SELECT injection_points_attach('define-index-before-set-valid', 'wait');
]);

# s3: Start CREATE INDEX CONCURRENTLY (blocks on define-index-before-set-valid)
$s3->query_until(qr/starting_create_index/, q[
\echo starting_create_index
CREATE UNIQUE INDEX CONCURRENTLY tbl_pkey_duplicate ON test.tbl(i);
]);

ok(wait_for_injection_point($node, 'define-index-before-set-valid'));

# s1: Start UPSERT (blocks on invalidate-catalog-snapshot-end)
$s1->query_until(qr/starting_upsert_s1/, q[
\echo starting_upsert_s1
INSERT INTO test.tbl VALUES (13,now()) ON CONFLICT (i) DO UPDATE SET updated_at = now();
]);

ok(wait_for_injection_point($node, 'invalidate-catalog-snapshot-end'));

# Wakeup s3 (CREATE INDEX continues, triggers catalog invalidation)
wakeup_injection_point($node, 'define-index-before-set-valid');

# s2: Start UPSERT (blocks on exec-insert-before-insert-speculative)
$s2->query_until(qr/starting_upsert_s2/, q[
\echo starting_upsert_s2
INSERT INTO test.tbl VALUES (13,now()) ON CONFLICT (i) DO UPDATE SET updated_at = now();
]);

ok(wait_for_injection_point($node, 'exec-insert-before-insert-speculative'));

wakeup_injection_point($node, 'invalidate-catalog-snapshot-end');

ok(wait_for_injection_point($node, 'check-exclusion-or-unique-constraint-no-conflict'));

wakeup_injection_point($node, 'exec-insert-before-insert-speculative');

wakeup_injection_point($node, 'check-exclusion-or-unique-constraint-no-conflict');

is(safe_quit($s1), '', 'Test 7 (CREATE INDEX): session s1 quit successfully');
is(safe_quit($s2), '', 'Test 7 (CREATE INDEX): session s2 quit successfully');
is(safe_quit($s3), '', 'Test 7 (CREATE INDEX): session s3 quit successfully');

$node->safe_psql('postgres', 'DROP SCHEMA test CASCADE;');

###############################################################################
# Test 8: CREATE INDEX CONCURRENTLY on partial index + UPSERT
# Based on index-concurrently-upsert-predicate.spec
# Uses invalidate-catalog-snapshot-end to test catalog invalidation during UPSERT
###############################################################################

$node->safe_psql(
	'postgres', q[
CREATE SCHEMA test;
CREATE UNLOGGED TABLE test.tbl(i int, updated_at timestamp);
CREATE UNIQUE INDEX tbl_pkey_special ON test.tbl(abs(i)) WHERE i < 1000;
ALTER TABLE test.tbl SET (parallel_workers=0);
]);

$s1 = $node->background_psql('postgres', on_error_stop => 0);
$s2 = $node->background_psql('postgres', on_error_stop => 0);
$s3 = $node->background_psql('postgres', on_error_stop => 0);

$s1_pid = $s1->query_safe('SELECT pg_backend_pid()');

# s1 attaches BOTH injection points - the unique constraint check AND catalog snapshot
$s1->query_safe(q[
SELECT injection_points_set_local();
SELECT injection_points_attach('check-exclusion-or-unique-constraint-no-conflict', 'wait');
]);

$s1->query_until(qr/attaching_injection_point/, q[
\echo attaching_injection_point
SELECT injection_points_attach('invalidate-catalog-snapshot-end', 'wait');
]);
# In case of CLOBBER_CACHE_ALWAYS - s1 may hit the injection point during attach.
# Wait for s1 to become idle (attach completed) or wakeup if stuck on injection point.
if (!wait_for_idle($node, $s1_pid))
{
	ok(wait_for_injection_point($node, 'invalidate-catalog-snapshot-end'),
		'Test 8: s1 hit injection point during attach (CLOBBER_CACHE_ALWAYS)');
	$node->safe_psql('postgres', q[
		SELECT injection_points_wakeup('invalidate-catalog-snapshot-end');
	]);
}

$s2->query_safe(q[
SELECT injection_points_set_local();
SELECT injection_points_attach('exec-insert-before-insert-speculative', 'wait');
]);

$s3->query_safe(q[
SELECT injection_points_set_local();
SELECT injection_points_attach('define-index-before-set-valid', 'wait');
]);

# s3: Start CREATE INDEX CONCURRENTLY (blocks on define-index-before-set-valid)
$s3->query_until(qr/starting_create_index/, q[
\echo starting_create_index
CREATE UNIQUE INDEX CONCURRENTLY tbl_pkey_special_duplicate ON test.tbl(abs(i)) WHERE i < 10000;
]);

ok(wait_for_injection_point($node, 'define-index-before-set-valid'));

# s1: Start UPSERT (blocks on invalidate-catalog-snapshot-end)
$s1->query_until(qr/starting_upsert_s1/, q[
\echo starting_upsert_s1
INSERT INTO test.tbl VALUES(13,now()) ON CONFLICT (abs(i)) WHERE i < 100 DO UPDATE SET updated_at = now();
]);

ok(wait_for_injection_point($node, 'invalidate-catalog-snapshot-end'));

# Wakeup s3 (CREATE INDEX continues, triggers catalog invalidation)
wakeup_injection_point($node, 'define-index-before-set-valid');

# s2: Start UPSERT (blocks on exec-insert-before-insert-speculative)
$s2->query_until(qr/starting_upsert_s2/, q[
\echo starting_upsert_s2
INSERT INTO test.tbl VALUES(13,now()) ON CONFLICT (abs(i)) WHERE i < 100 DO UPDATE SET updated_at = now();
]);

ok(wait_for_injection_point($node, 'exec-insert-before-insert-speculative'));

wakeup_injection_point($node, 'invalidate-catalog-snapshot-end');

ok(wait_for_injection_point($node, 'check-exclusion-or-unique-constraint-no-conflict'));

wakeup_injection_point($node, 'exec-insert-before-insert-speculative');

wakeup_injection_point($node, 'check-exclusion-or-unique-constraint-no-conflict');

is(safe_quit($s1), '', 'Test 8 (CREATE INDEX predicate): session s1 quit successfully');
is(safe_quit($s2), '', 'Test 8 (CREATE INDEX predicate): session s2 quit successfully');
is(safe_quit($s3), '', 'Test 8 (CREATE INDEX predicate): session s3 quit successfully');

$node->safe_psql('postgres', 'DROP SCHEMA test CASCADE;');

done_testing();
