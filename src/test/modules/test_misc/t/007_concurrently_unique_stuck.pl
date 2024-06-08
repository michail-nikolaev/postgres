# Copyright (c) 2024, PostgreSQL Global Development Group

# Test REINDEX CONCURRENTLY with concurrent modifications and HOT updates
use strict;
use warnings;

use Config;
use Errno;

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Time::HiRes qw(usleep);
use IPC::SysV;
use threads;
use Time::HiRes qw( time );
use Test::More;
use Test::Builder;

if ($@ || $windows_os)
{
	plan skip_all => 'Fork and shared memory are not supported by this platform';
}

my ($pid, $shmem_id, $shmem_key,  $shmem_size);
eval 'sub IPC_CREAT {0001000}' unless defined &IPC_CREAT;
$shmem_size = 4;
$shmem_key = rand(1000000);
$shmem_id = shmget($shmem_key, $shmem_size, &IPC_CREAT | 0777) or die "Can't shmget: $!";
shmwrite($shmem_id, "wait", 0, $shmem_size) or die "Can't shmwrite: $!";

my $psql_timeout = IPC::Run::timer($PostgreSQL::Test::Utils::timeout_default);
#
# Test set-up
#
my ($node, $result);
$node = PostgreSQL::Test::Cluster->new('RC_test');
$node->init;
$node->append_conf('postgresql.conf',
	'lock_timeout = ' . (1000 * $PostgreSQL::Test::Utils::timeout_default));
$node->append_conf('postgresql.conf', 'fsync = off');
$node->append_conf('postgresql.conf', 'autovacuum = off');
$node->start;
$node->safe_psql('postgres', q(CREATE UNLOGGED TABLE tbl(i int primary key, n int)));


#######################################################################################################################
#######################################################################################################################
#######################################################################################################################

# IT IS **REQUIRED** TO REPRODUCE THE ISSUE
$node->safe_psql('postgres', q(CREATE INDEX idx ON tbl(i, n)));
$node->safe_psql('postgres', q(INSERT INTO tbl VALUES(13,1)));

#######################################################################################################################
#######################################################################################################################
#######################################################################################################################

my $builder = Test::More->builder;
$builder->use_numbers(0);
$builder->no_plan();

my $child  = $builder->child("pg_bench");

if(!defined($pid = fork())) {
	# fork returned undef, so unsuccessful
	die "Cannot fork a child: $!";
} elsif ($pid == 0) {

	$pid = fork();
	if ($pid == 0) {
		$node->pgbench(
			'--no-vacuum --client=30 -j 2 --transactions=1000000',
			0,
			[qr{actually processed}],
			[qr{^$}],
			'concurrent INSERTs, UPDATES and RC',
			{
				'002_pgbench_concurrent_transaction_updates' => q(
					BEGIN;
					INSERT INTO tbl VALUES(13,1) on conflict(i) do update set n = tbl.n + EXCLUDED.n;
					COMMIT;
				),
			});

		if ($child->is_passing()) {
			shmwrite($shmem_id, "done", 0, $shmem_size) or die "Can't shmwrite: $!";
		} else {
			shmwrite($shmem_id, "fail", 0, $shmem_size) or die "Can't shmwrite: $!";
		}

		my $pg_bench_fork_flag;
		while (1) {
			shmread($shmem_id, $pg_bench_fork_flag, 0, $shmem_size) or die "Can't shmread: $!";
			sleep(0.1);
			last if $pg_bench_fork_flag eq "stop";
		}
	}
	else {
		my ($result, $stdout, $stderr, $n, $prev_n, $pg_bench_fork_flag);
		while (1) {
			sleep(1);
			$prev_n = $n;
			($result, $n, $stderr) = $node->psql('postgres', q(SELECT n from tbl WHERE i = 13;));
			diag(" current n is " . $n . ', ' . ($n - $prev_n) . ' per one second');
			shmread($shmem_id, $pg_bench_fork_flag, 0, $shmem_size) or die "Can't shmread: $!";
			last if $pg_bench_fork_flag eq "stop";
		}
	}
} else {
	my $pg_bench_fork_flag;
	shmread($shmem_id, $pg_bench_fork_flag, 0, $shmem_size) or die "Can't shmread: $!";

	subtest 'reindex run subtest' => sub {
		is($pg_bench_fork_flag, "wait", "pg_bench_fork_flag is correct");

		my %psql = (stdin => '', stdout => '', stderr => '');
		$psql{run} = IPC::Run::start(
			[ 'psql', '-XA', '-f', '-', '-d', $node->connstr('postgres') ],
			'<',
			\$psql{stdin},
			'>',
			\$psql{stdout},
			'2>',
			\$psql{stderr},
			$psql_timeout);

		my ($result, $stdout, $stderr, $n, $begin_time, $end_time, $before_reindex, $after_reindex);

		# IT IS NOT REQUIRED, JUST FOR CONSISTENCY
		($result, $stdout, $stderr) = $node->psql('postgres', q(ALTER TABLE tbl SET (parallel_workers=0);));
		is($result, '0', 'ALTER TABLE is correct');

		while (1)
		{
			my $sql = q(REINDEX INDEX CONCURRENTLY tbl_pkey;);

			($result, $before_reindex, $stderr) = $node->psql('postgres', q(SELECT n from tbl WHERE i = 13;));

			diag('going to start reindex, num tuples in table is ' . $before_reindex);
			$begin_time = time();
			($result, $stdout, $stderr) = $node->psql('postgres', $sql);
			is($result, '0', 'REINDEX INDEX CONCURRENTLY is correct');

			$end_time = time();
			($result, $after_reindex, $stderr) = $node->psql('postgres', q(SELECT n from tbl WHERE i = 13;));
			diag('reindex ' . $n++ . ' done in ' . ($end_time - $begin_time) . ' seconds, num inserted during reindex tuples is ' . (int($after_reindex) - int($before_reindex)) . ' speed is ' . ((int($after_reindex) - int($before_reindex)) / ($end_time - $begin_time)) . ' per second');

			shmread($shmem_id, $pg_bench_fork_flag, 0, $shmem_size) or die "Can't shmread: $!";
			last if $pg_bench_fork_flag ne "wait";
		}

		# explicitly shut down psql instances gracefully
		$psql{stdin} .= "\\q\n";
		$psql{run}->finish;

		is($pg_bench_fork_flag, "done", "pg_bench_fork_flag is correct");
	};

	$child->finalize();
	$child->summary();
	$node->stop;
	done_testing();

	shmwrite($shmem_id, "stop", 0, $shmem_size) or die "Can't shmwrite: $!";
}
