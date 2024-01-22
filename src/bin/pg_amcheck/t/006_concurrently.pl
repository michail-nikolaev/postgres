
# Copyright (c) 2023, PostgreSQL Global Development Group

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
use Test::More;
use Test::Builder;


# TODO: refactor to https://metacpan.org/pod/IPC%3A%3AShareable
my ($pid, $shmem_id, $shmem_key,  $shmem_size);
eval 'sub IPC_CREAT {0001000}' unless defined &IPC_CREAT;
$shmem_size = 4;
$shmem_key = rand(1000000);
$shmem_id = shmget($shmem_key, $shmem_size, &IPC_CREAT | 0777) or die "Can't shmget: $!";
shmwrite($shmem_id, "wait", 0, $shmem_size) or die "Can't shmwrite: $!";

#
# Test set-up
#
my ($node, $result);
$node = PostgreSQL::Test::Cluster->new('RC_test');
$node->init;
$node->append_conf('postgresql.conf',
	'lock_timeout = ' . (1000 * $PostgreSQL::Test::Utils::timeout_default));
$node->append_conf('postgresql.conf', 'fsync = off');
$node->start;
$node->safe_psql('postgres', q(CREATE EXTENSION amcheck));
$node->safe_psql('postgres', q(CREATE TABLE tbl(i int primary key,
								c1 money default 0,c2 money default 0,
								c3 money default 0, updated_at timestamp)));
$node->safe_psql('postgres', q(CREATE INDEX idx ON tbl(i)));

my $builder = Test::More->builder;
$builder->use_numbers(0);
$builder->no_plan();

my $child  = $builder->child("pg_bench");

if(!defined($pid = fork())) {
   # fork returned undef, so unsuccessful
   die "Cannot fork a child: $!";
} elsif ($pid == 0) {

    $node->pgbench(
        '--no-vacuum --client=15 --transactions=2500',
        0,
        [qr{actually processed}],
        [qr{^$}],
        'concurrent INSERTs, UPDATES and RC',
        {
            '002_pgbench_concurrent_transaction_inserts' => q(
                BEGIN;
                INSERT INTO tbl VALUES(random()*10000,0,0,0,now())
                    on conflict(i) do update set updated_at = now();
                INSERT INTO tbl VALUES(random()*10000,0,0,0,now())
                    on conflict(i) do update set updated_at = now();
                INSERT INTO tbl VALUES(random()*10000,0,0,0,now())
                    on conflict(i) do update set updated_at = now();
                INSERT INTO tbl VALUES(random()*10000,0,0,0,now())
                    on conflict(i) do update set updated_at = now();
                SELECT pg_sleep(case when random()<0.05 then 0.01 else 0 end);
                INSERT INTO tbl VALUES(random()*10000,0,0,0,now())
                    on conflict(i) do update set updated_at = now();
                COMMIT;
              ),
            # Ensure some HOT updates happen
            '002_pgbench_concurrent_transaction_updates' => q(
                BEGIN;
                INSERT INTO tbl VALUES(random()*1000,0,0,0,now())
                    on conflict(i) do update set updated_at = now();
                INSERT INTO tbl VALUES(random()*1000,0,0,0,now())
                    on conflict(i) do update set updated_at = now();
                INSERT INTO tbl VALUES(random()*1000,0,0,0,now())
                    on conflict(i) do update set updated_at = now();
                INSERT INTO tbl VALUES(random()*1000,0,0,0,now())
                    on conflict(i) do update set updated_at = now();
                SELECT pg_sleep(case when random()<0.05 then 0.01 else 0 end);
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

    sleep(1);
} else {
    my $pg_bench_fork_flag;
    shmread($shmem_id, $pg_bench_fork_flag, 0, $shmem_size) or die "Can't shmread: $!";

    subtest 'reindex run subtest' => sub {
        is($pg_bench_fork_flag, "wait", "pg_bench_fork_flag is correct");

        while (1)
        {
            $node->safe_psql('postgres', q(REINDEX INDEX CONCURRENTLY idx;));

            my ($result, $stdout, $stderr) = $node->psql('postgres', q(SELECT bt_index_parent_check('idx', true, true);));
            is($result, '0', 'bt_index_check is correct');
            if ($result)
            {
                diag($stderr);
            }
            else
            {
                diag(':)');
            }

            usleep(10 * 1000);

            shmread($shmem_id, $pg_bench_fork_flag, 0, $shmem_size) or die "Can't shmread: $!";
            last if $pg_bench_fork_flag eq "done";
            last if $pg_bench_fork_flag eq "fail";
        }

        is($pg_bench_fork_flag, "done", "pg_bench_fork_flag is correct");
    };


    $child->finalize();
    $child->summary();
    $node->stop;
    done_testing();
}

