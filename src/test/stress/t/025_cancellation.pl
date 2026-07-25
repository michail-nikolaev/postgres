# Copyright (c) 2026, PostgreSQL Global Development Group

# Stress test for CONCURRENTLY commands that are interrupted partway.
#
# 012_crash_recovery.pl kills the whole cluster; here individual
# commands are cancelled instead, which exercises their own cleanup
# paths rather than recovery.  Each DDL session runs with a randomly
# chosen, very short statement_timeout, so the command is cancelled at
# an arbitrary point in its execution -- while building an index, while
# waiting for other transactions, while swapping relations at the end,
# and so on.  Some sessions get a generous timeout instead, so that
# commands also complete normally.
#
# Whatever happens, afterwards:
# - the sum invariant must still hold,
# - no replication slot may have leaked (REPACK (CONCURRENTLY) uses a
#   transient one internally),
# - logical decoding must have been switched off again, so
#   effective_wal_level must fall back to replica,
# - leftover invalid indexes must be droppable, and
# - the surviving indexes must pass amcheck.
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
	'skipping disabled cancellation stress test');

my $duration = 6 * $stressval;
my $nrows = 10_000;

my $node;

#
# Test set-up.  wal_level = replica, so that the transient slot taken by
# REPACK (CONCURRENTLY) really does toggle logical decoding on and off.
#
$node = stress_init_node('cancellation',
	init => { allows_streaming => 1 },
	extra_conf => [ 'max_connections = 50' ]);

$node->safe_psql(
	'postgres', qq(
	CREATE EXTENSION amcheck;
	CREATE TABLE tbl(id int PRIMARY KEY, val int);
	CREATE INDEX tbl_val_idx ON tbl(val);
	INSERT INTO tbl SELECT g, g FROM generate_series(1, $nrows) g;
));

my $sum = $node->safe_psql('postgres', q(SELECT SUM(val) AS sum FROM tbl));

# Writers only; the DDL is driven from Perl below so that its errors can
# be tolerated.
my $ops_sql = $node->basedir . '/writers.sql';
PostgreSQL::Test::Utils::append_to_file($ops_sql,
	qq(\\set num_a random(1, $nrows)
\\set num_b random(1, $nrows)
\\set diff random(1, 10000)
BEGIN;
UPDATE tbl SET val = val + :diff WHERE id = :num_a;
\\sleep 1 ms
UPDATE tbl SET val = val - :diff WHERE id = :num_b;
COMMIT;

SELECT stress_assert(COALESCE(SUM(val), 0) = $sum,
	format('sum is %s, not $sum', COALESCE(SUM(val), 0))) FROM tbl;
));

my @cmd = (
	'pgbench', '--no-vacuum', '--client=20', '--jobs=20',
	'--exit-on-abort', '-T', $duration,
	'-p', $node->port, '-h', $node->host, '-f', $ops_sql, 'postgres');

my ($out, $err) = ('', '');
my $h = start \@cmd, '>', \$out, '2>', \$err;

my @ddl = (
	'REPACK (CONCURRENTLY) tbl',
	'REINDEX INDEX CONCURRENTLY tbl_val_idx',
	'REINDEX TABLE CONCURRENTLY tbl',
	'DROP INDEX CONCURRENTLY tbl_val_idx',
	'CREATE INDEX CONCURRENTLY tbl_val_idx ON tbl(val)',
);

my $attempts = 0;
my $cancelled = 0;
my $deadline = time() + $duration;
while (time() < $deadline)
{
	my $stmt = $ddl[ int(rand(scalar @ddl)) ];

	# Mostly cancel at a random point; sometimes let it finish.
	my $timeout =
	  (int(rand(4)) == 0) ? 0 : 1 + int(rand(200));

	my ($ret, $stdout, $stderr) = $node->psql(
		'postgres',
		"SET statement_timeout = $timeout; $stmt",
		on_error_stop => 0);

	$attempts++;

	# Note that psql exits successfully even when a statement fails
	# here, since on_error_stop is off, so go by what it reported.
	if ($stderr ne '')
	{
		$cancelled++;

		# The only errors expected are the cancellation itself, and the
		# complaints that follow from a previous cancellation having
		# left the indexes in an unexpected state.
		like(
			$stderr,
			qr/canceling statement due to statement timeout|(?:relation|index) "[^"]+" (?:already exists|does not exist)|skipping reindex of invalid index|deadlock detected/,
			'interrupted DDL failed only in expected ways')
		  or diag("unexpected error: $stderr");
	}
}

finish $h;

like($out, qr/actually processed/, 'writers completed');
is($err, '', 'writers reported no errors');
note "$attempts DDL attempts, $cancelled of them interrupted";
cmp_ok($cancelled, '>', 0, 'some DDL was interrupted');

# The writers keep this invariant at every commit, so it must hold no
# matter where the DDL was cut off.
is( $node->safe_psql('postgres', q(SELECT SUM(val) FROM tbl)),
	$sum, 'sum invariant holds after interrupted DDL');

# A cancelled REPACK must not leave its transient slot behind.
is( $node->safe_psql('postgres',
		q(SELECT COUNT(*) FROM pg_replication_slots)),
	'0', 'no replication slot leaked');

# ... and logical decoding must have been switched off again.
$node->poll_query_until('postgres',
	q(SELECT current_setting('effective_wal_level') = 'replica'))
  or die 'timed out waiting for logical decoding to be disabled';
pass('effective_wal_level fell back to replica');

# Interrupted concurrent index builds may leave invalid indexes behind;
# that is documented.  They must be droppable, though.
my $invalid = $node->safe_psql(
	'postgres', q(
	SELECT COUNT(*) FROM pg_index
	WHERE indrelid = 'tbl'::regclass AND NOT indisvalid));
note "$invalid invalid indexes left behind";

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

# The DDL rotation may have been cut off between the drop and the
# re-creation of the secondary index.
$node->safe_psql('postgres',
	q(CREATE INDEX IF NOT EXISTS tbl_val_idx ON tbl(val)));

$node->safe_psql(
	'postgres', q(
	SELECT bt_index_parent_check('tbl_pkey', heapallindexed => true);
	SELECT bt_index_parent_check('tbl_val_idx', heapallindexed => true);
));

$node->stop;

done_testing();
