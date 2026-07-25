# Copyright (c) 2026, PostgreSQL Global Development Group

# Stress test for CONCURRENTLY commands under tight shared-memory lock
# limits.
#
# The lock table lives in shared memory sized from
# max_locks_per_transaction, and CONCURRENTLY commands are heavy users
# of it: they take and hold predicate locks, relation locks on the
# table and all of its indexes, and virtual-xid locks they wait on
# between phases.  Running them with a deliberately small lock table,
# against a table with many indexes and many concurrent sessions, keeps
# the shared lock table close to full, exercising the paths that run
# when it fills up.
#
# "out of shared memory" is an expected outcome here, not a failure, so
# the DDL is driven from Perl and those errors are tolerated; anything
# that does complete must leave the data and indexes consistent.  The
# writers keep the sum invariant and must never see it broken.
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
	'skipping disabled lock exhaustion stress test');

my $duration = 6 * $stressval;
my $nrows = 5000;
my $nindexes = 8;

my $node;

#
# Test set-up.  A small lock table plus many indexes and sessions keeps
# it under pressure.
#
$node = stress_init_node('lock_exhaustion',
	extra_conf => [ 'max_locks_per_transaction = 16', 'max_connections = 60', 'max_pred_locks_per_transaction = 16' ]);

$node->safe_psql('postgres', q(CREATE EXTENSION amcheck));
$node->safe_psql('postgres',
	q(CREATE TABLE tbl(id int PRIMARY KEY, val int)));
# Several indexes, so each CONCURRENTLY command on the table locks many
# relations at once.
foreach my $i (1 .. $nindexes)
{
	$node->safe_psql('postgres',
		"CREATE INDEX tbl_idx_$i ON tbl((val + $i))");
}
$node->safe_psql('postgres',
	qq(INSERT INTO tbl SELECT g, g FROM generate_series(1, $nrows) g));

my $sum = $node->safe_psql('postgres', q(SELECT SUM(val) FROM tbl));

# Writers keep the sum invariant, under SERIALIZABLE part of the time so
# that predicate locks pile into the small lock table too.
my $ops_sql = $node->basedir . '/writers.sql';
PostgreSQL::Test::Utils::append_to_file($ops_sql,
	qq(\\set num_a random(1, $nrows)
\\set num_b random(1, $nrows)
\\set diff random(1, 10000)
\\set use_serializable random(0, 1)
\\if :use_serializable
	BEGIN ISOLATION LEVEL SERIALIZABLE;
\\else
	BEGIN;
\\endif
UPDATE tbl SET val = val + :diff WHERE id = :num_a;
\\sleep 1 ms
UPDATE tbl SET val = val - :diff WHERE id = :num_b;
COMMIT;

SELECT stress_assert(COALESCE(SUM(val), 0) = $sum,
	format('sum is %s, not $sum', COALESCE(SUM(val), 0))) FROM tbl;
));

my @cmd = (
	'pgbench', '--no-vacuum', '--client=40', '--jobs=40',
	'--exit-on-abort', '--max-tries=0', '-T', $duration,
	'-p', $node->port, '-h', $node->host, '-f', $ops_sql, 'postgres');

my ($out, $err) = ('', '');
my $h = start \@cmd, '>', \$out, '2>', \$err;

my @ddl = (
	'REPACK (CONCURRENTLY) tbl',
	'REINDEX TABLE CONCURRENTLY tbl',
	'REINDEX INDEX CONCURRENTLY tbl_idx_1',
	'DROP INDEX CONCURRENTLY tbl_idx_1',
	'CREATE INDEX CONCURRENTLY tbl_idx_1 ON tbl((val + 1))',
);

my $attempts = 0;
my $exhausted = 0;
my $deadline = time() + $duration;
while (time() < $deadline)
{
	my $stmt = $ddl[ int(rand(scalar @ddl)) ];
	my ($ret, $stdout, $stderr) =
	  $node->psql('postgres', $stmt, on_error_stop => 0);
	$attempts++;

	next if $stderr eq '';

	# Running out of the (deliberately tiny) lock table is expected, as
	# are the follow-on complaints from a command a previous run left in
	# an odd state.
	$exhausted++ if $stderr =~ /out of shared memory|You might need to increase/;
	like(
		$stderr,
		qr/out of shared memory|You might need to increase.*max_locks_per_transaction|(?:relation|index) "[^"]+" (?:already exists|does not exist)|canceling statement|deadlock detected|could not serialize/,
		'interrupted DDL failed only in expected ways')
	  or diag("unexpected error: $stderr");
}

finish $h;

like($out, qr/actually processed/, 'writers completed');
is($err, '', 'writers reported no errors');
note "$attempts DDL attempts, $exhausted hit the lock table limit";

is( $node->safe_psql('postgres', q(SELECT SUM(val) FROM tbl)),
	$sum, 'sum invariant holds under lock pressure');
is( $node->safe_psql('postgres', q(SELECT COUNT(*) FROM tbl)),
	"$nrows", 'no rows lost');

# Clean up any invalid index a failed build left behind, then check the
# survivors.
$node->safe_psql(
	'postgres', q(
	DO $$
	DECLARE
		idx regclass;
	BEGIN
		FOR idx IN SELECT indexrelid::regclass FROM pg_index
			WHERE indrelid = 'tbl'::regclass AND NOT indisvalid
		LOOP
			EXECUTE format('DROP INDEX %s', idx);
		END LOOP;
	END;
	$$;
));
$node->safe_psql('postgres',
	q(CREATE INDEX IF NOT EXISTS tbl_idx_1 ON tbl((val + 1))));

$node->safe_psql('postgres',
	q(SELECT bt_index_parent_check('tbl_pkey', heapallindexed => true)));

$node->stop;

done_testing();
