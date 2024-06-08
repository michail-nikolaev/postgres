
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
$node->safe_psql('postgres', q(CREATE UNLOGGED TABLE tbl(i int primary key, updated_at timestamp)));


#######################################################################################################################
#######################################################################################################################
#######################################################################################################################

# IT IS NOT REQUIRED TO REPRODUCE THE ISSUE BUT MAKES IT TO HAPPEN FASTER
$node->safe_psql('postgres', q(CREATE INDEX idx ON tbl(i, updated_at)));

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

	$node->pgbench(
		'--no-vacuum --client=20 -j 2 --transactions=100000',
		0,
		[qr{actually processed}],
		[qr{^$}],
		'concurrent INSERTs, UPDATES and RC',
		{
			# Ensure some HOT updates happen
			'002_pgbench_concurrent_transaction_updates' => q(
				BEGIN;
				INSERT INTO tbl VALUES(13,now()) on conflict(i) do update set updated_at = now();
				COMMIT;
			),
			'003_pgbench_concurrent_transaction_updates' => q(
				BEGIN;
				INSERT INTO tbl VALUES(42,now()) on conflict(i) do update set updated_at = now();
				COMMIT;
			),
			'004_pgbench_concurrent_transaction_updates' => q(
				BEGIN;
				INSERT INTO tbl VALUES(69,now()) on conflict(i) do update set updated_at = now();
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

		my ($result, $stdout, $stderr, $n, $begin_time, $end_time);

		# IT IS NOT REQUIRED, JUST FOR CONSISTENCY
		($result, $stdout, $stderr) = $node->psql('postgres', q(ALTER TABLE tbl SET (parallel_workers=0);));
		is($result, '0', 'ALTER TABLE is correct');

		$begin_time = time();
		while (1)
		{
			my $sql = q(REINDEX INDEX CONCURRENTLY tbl_pkey;);

			($result, $stdout, $stderr) = $node->psql('postgres', $sql);
			is($result, '0', 'REINDEX INDEX CONCURRENTLY is correct');

			$end_time = time();
			diag('waiting for an about 3000, now is ' . $n++ . ', seconds passed : ' . int($end_time - $begin_time));

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
