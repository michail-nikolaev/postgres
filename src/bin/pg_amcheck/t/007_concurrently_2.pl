
# Copyright (c) 2024, PostgreSQL Global Development Group

# Test REINDEX CONCURRENTLY with concurrent modifications and HOT updates
use strict;
use warnings;

use Config;
use Errno;



use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Time::HiRes qw(usleep);

use threads;
use Test::More;
use Test::Builder;


eval {
	require IPC::SysV;
	IPC::SysV->import(qw(IPC_CREAT IPC_EXCL S_IRUSR S_IWUSR));
};

if ($@ || $windows_os)
{
	plan skip_all => 'Fork and shared memory are not supported by this platform';
}

# TODO: refactor to https://metacpan.org/pod/IPC%3A%3AShareable
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
$node = PostgreSQL::Test::Cluster->new('RC_test_2');
$node->init;
$node->append_conf('postgresql.conf',
	'lock_timeout = ' . (1000 * $PostgreSQL::Test::Utils::timeout_default));
$node->append_conf('postgresql.conf', 'fsync = off');
$node->start;
$node->safe_psql('postgres', q(CREATE EXTENSION amcheck));
$node->safe_psql('postgres', q(CREATE UNLOGGED TABLE tbl(i int primary key,
								c1 money default 0, c2 money default 0,
								c3 money default 0, updated_at timestamp)));

my $builder = Test::More->builder;
$builder->use_numbers(0);
$builder->no_plan();

my $child  = $builder->child("pg_bench");

if(!defined($pid = fork())) {
	# fork returned undef, so unsuccessful
	die "Cannot fork a child: $!";
} elsif ($pid == 0) {

	$node->pgbench(
		'--no-vacuum --client=20 --transactions=1000',
		0,
		[qr{actually processed}],
		[qr{^$}],
		'concurrent INSERTs, UPDATES and RC',
		{
			'001_pgbench_concurrent_transaction_inserts' => q(
				BEGIN;
				INSERT INTO tbl VALUES(random()*10000,0,0,0,now())
					on conflict(i) do update set updated_at = now();
				INSERT INTO tbl VALUES(random()*10000,0,0,0,now())
					on conflict(i) do update set updated_at = now();
				INSERT INTO tbl VALUES(random()*10000,0,0,0,now())
					on conflict(i) do update set updated_at = now();
				INSERT INTO tbl VALUES(random()*10000,0,0,0,now())
					on conflict(i) do update set updated_at = now();
				INSERT INTO tbl VALUES(random()*10000,0,0,0,now())
					on conflict(i) do update set updated_at = now();
				COMMIT;
			  ),
			'002_pgbench_concurrent_transaction_inserts' => q(
				BEGIN;
				INSERT INTO tbl VALUES(random()*100000,0,0,0,now())
					on conflict(i) do update set updated_at = now();
				INSERT INTO tbl VALUES(random()*100000,0,0,0,now())
					on conflict(i) do update set updated_at = now();
				INSERT INTO tbl VALUES(random()*100000,0,0,0,now())
					on conflict(i) do update set updated_at = now();
				INSERT INTO tbl VALUES(random()*100000,0,0,0,now())
					on conflict(i) do update set updated_at = now();
				INSERT INTO tbl VALUES(random()*100000,0,0,0,now())
					on conflict(i) do update set updated_at = now();
				COMMIT;
			  ),
			# Ensure some HOT updates happen
			'003_pgbench_concurrent_transaction_updates' => q(
				BEGIN;
				INSERT INTO tbl VALUES(random()*1000,0,0,0,now())
					on conflict(i) do update set updated_at = now();
				INSERT INTO tbl VALUES(random()*1000,0,0,0,now())
					on conflict(i) do update set updated_at = now();
				INSERT INTO tbl VALUES(random()*1000,0,0,0,now())
					on conflict(i) do update set updated_at = now();
				INSERT INTO tbl VALUES(random()*1000,0,0,0,now())
					on conflict(i) do update set updated_at = now();
				INSERT INTO tbl VALUES(random()*1000,0,0,0,now())
					on conflict(i) do update set updated_at = now();
				COMMIT;
			  )
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
	shmwrite($shmem_id, "exit", 0, $shmem_size) or die "Can't shmwrite: $!";
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

		my ($result, $stdout, $stderr, $n, $stderr_saved);
		$n = 0;

		while (1)
		{
			if (1)
			{
				my $sql = q(CREATE INDEX CONCURRENTLY idx_2 ON tbl(i, updated_at););

				#diag('#' . $sql);
				($result, $stdout, $stderr) = $node->psql('postgres', $sql);
				is($result, '0', 'CREATE INDEX is correct');
				$stderr_saved = $stderr;

				($result, $stdout, $stderr) = $node->psql('postgres', q(SELECT bt_index_parent_check('idx_2', heapallindexed => true, rootdescend => true, checkunique => true);));
				is($result, '0', 'bt_index_check for new index is correct');
				if ($result)
				{
					diag($stderr);
					diag($stderr_saved);
					#BAIL_OUT($stderr);
				} else {
					#diag('#create:)' . $n++);
				}


				($result, $stdout, $stderr) = $node->psql('postgres', q(DROP INDEX CONCURRENTLY idx_2;));
				is($result, '0', 'DROP INDEX is correct');
			}
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

	shmwrite($shmem_id, "stop", 0, $shmem_size) or die "Can't shmwrite: $!";

    while (1)
    {
        sleep(0.1);
        shmread($shmem_id, $pg_bench_fork_flag, 0, $shmem_size) or die "Can't shmread: $!";
        last if $pg_bench_fork_flag eq "exit";
    }
    is($pg_bench_fork_flag, "exit", "pg_bench_fork_flag is correct");
    waitpid($pid,0);
}

# Send query, wait until string matches
sub send_query_and_wait
{
	my ($psql, $query, $untl) = @_;
	my $ret;

	# For each query we run, we'll restart the timeout.  Otherwise the timeout
	# would apply to the whole test script, and would need to be set very high
	# to survive when running under Valgrind.
	$psql_timeout->reset();
	$psql_timeout->start();

	# send query
	$$psql{stdin} .= $query;
	$$psql{stdin} .= "\n";

	# wait for query results
	$$psql{run}->pump_nb();
	while (1)
	{
		last if $$psql{stdout} =~ /$untl/;
		if ($psql_timeout->is_expired)
		{
			diag("aborting wait: program timed out\n"
				  . "stream contents: >>$$psql{stdout}<<\n"
				  . "pattern searched for: $untl\n");
			return 0;
		}
		if (not $$psql{run}->pumpable())
		{
			diag("aborting wait: program died\n"
				  . "stream contents: >>$$psql{stdout}<<\n"
				  . "pattern searched for: $untl\n");
			return 0;
		}
		$$psql{run}->pump();
	}

	$$psql{stdout} = '';

	return 1;
}
