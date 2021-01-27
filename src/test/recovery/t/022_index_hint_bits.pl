# Checks that index hints on standby work as excepted.
use strict;
use warnings;

use PostgresNode;
use TestLib;
use Test::More tests => 15;
use Config;

# Initialize primary node
my $node_primary = get_new_node('primary');
$node_primary->init(allows_streaming => 1);
$node_primary->append_conf('postgresql.conf', qq{
    autovacuum = off
    enable_seqscan = off
    enable_indexonlyscan = off
});
$node_primary->start;

$node_primary->safe_psql('postgres', 'CREATE EXTENSION pageinspect');
# Create test table with primary index
$node_primary->safe_psql(
    'postgres', 'CREATE TABLE test_index_hint (id int, value int)');
$node_primary->safe_psql(
    'postgres', 'CREATE INDEX test_index ON test_index_hint (value, id)');
# Fill some data to it, note to not put a lot of records to avoid
# heap_page_prune_opt call which cause conflict on recovery hiding conflict
# caused due index hint bits
$node_primary->safe_psql('postgres',
    'INSERT INTO test_index_hint VALUES (generate_series(1, 30), 0)');
# And vacuum to allow index hint bits to be set
$node_primary->safe_psql('postgres', 'VACUUM test_index_hint');
# For fail-fast in case FPW from primary
$node_primary->safe_psql('postgres', 'CHECKPOINT');

# Take backup
my $backup_name = 'my_backup';
$node_primary->backup($backup_name);

# Restore standby node from backup backup
my $node_standby_1 = get_new_node('standby_1');
$node_standby_1->init_from_backup($node_primary, $backup_name,
    has_streaming => 1);

my $standby_settings = qq{
    max_standby_streaming_delay = 1
    wal_receiver_status_interval = 1
    hot_standby_feedback = on
    enable_seqscan = off
    enable_indexonlyscan = off
};
$node_standby_1->append_conf('postgresql.conf', $standby_settings);
$node_standby_1->start;

$node_standby_1->backup($backup_name);

# Create second standby node linking to standby 1
my $node_standby_2 = get_new_node('standby_2');
$node_standby_2->init_from_backup($node_standby_1, $backup_name,
    has_streaming => 1);
$node_standby_2->append_conf('postgresql.conf', $standby_settings);
$node_standby_2->start;

# Make sure sender_propagates_feedback_to_primary is set on standbys
wait_hfs($node_primary, 1);
wait_hfs($node_standby_1, 1);

# To avoid hanging while expecting some specific input from a psql
# instance being driven by us, add a timeout high enough that it
# should never trigger even on very slow machines, unless something
# is really wrong.
my $psql_timeout = IPC::Run::timer(30);

# One psql to run command in repeatable read isolation level
my %psql_standby_repeatable_read = ('stdin' => '', 'stdout' => '', 'stderr' => '');
$psql_standby_repeatable_read{run} =
    IPC::Run::start(
        [ 'psql', '-XAb', '-f', '-', '-d', $node_standby_1->connstr('postgres') ],
        '<', \$psql_standby_repeatable_read{stdin},
        '>', \$psql_standby_repeatable_read{stdout},
        '2>', \$psql_standby_repeatable_read{stderr},
        $psql_timeout);

# Another psql to run command in read committed isolation level
my %psql_standby_read_committed = ('stdin' => '', 'stdout' => '', 'stderr' => '');
$psql_standby_read_committed{run} =
    IPC::Run::start(
        [ 'psql', '-XAb', '-f', '-', '-d', $node_standby_1->connstr('postgres') ],
        '<', \$psql_standby_read_committed{stdin},
        '>', \$psql_standby_read_committed{stdout},
        '2>', \$psql_standby_read_committed{stderr},
        $psql_timeout);

# Start RR transaction and read first row from index
ok(send_query_and_wait(\%psql_standby_repeatable_read,
    q[
BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ;
SELECT id FROM test_index_hint WHERE value = 0 ORDER BY id LIMIT 1;
],
    qr/1\n\(1 row\)/m),
    'row is visible in repeatable read');

# Start RC transaction and read first row from index
ok(send_query_and_wait(\%psql_standby_read_committed,
    q[
BEGIN TRANSACTION ISOLATION LEVEL READ COMMITTED;
SELECT id FROM test_index_hint WHERE value = 0 ORDER BY id LIMIT 1;
],
    qr/1\n\(1 row\)/m),
    'row is visible in read committed');

# Now delete first 10 rows in index
$node_primary->safe_psql('postgres',
    'UPDATE test_index_hint SET value = 1 WHERE id <= 10');

# Make sure hint bits are not set on primary
is(hints_num($node_primary), qq(0), 'no index hint bits are set on primary yet');

# Make sure page is not processed by heap_page_prune_opt
is(non_normal_num($node_primary), qq(0), 'all items are normal in heap');

# Wait for standbys to catch up transaction
wait_for_catchup_all();

# Disable hot_standby_feedback to trigger conflicts later
$node_standby_1->safe_psql('postgres',
    'ALTER SYSTEM SET hot_standby_feedback = off;');
$node_standby_1->reload;

# Make sure sender_propagates_feedback_to_primary is not set on standby
wait_hfs($node_primary, 0);
wait_hfs($node_standby_1, 1);

# Try to set hint bits in index on standby
try_to_set_hint_bits();

# Make sure read committed transaction is able to see correct data
ok(send_query_and_wait(\%psql_standby_read_committed,
    q/SELECT id FROM test_index_hint WHERE value = 0 ORDER BY id LIMIT 1;/,
    qr/11\n\(1 row\)/m),
    'session is not canceled for read committed');

# Make sure previous queries not set the hints on standby because
# of parallel transaction running
ok(send_query_and_wait(\%psql_standby_repeatable_read,
    q/SELECT id FROM test_index_hint WHERE value = 0 ORDER BY id LIMIT 1;/,
    qr/1\n\(1 row\)/m),
    'hints on standby are not set');

is(hints_num($node_standby_1), qq(0), 'no index hint bits are set on standby yet');


# Set index hint bits and replicate to standby
$node_primary->safe_psql('postgres',
    'SELECT id FROM test_index_hint WHERE value = 0 ORDER BY id LIMIT 1;');

# Make sure page is not processed by heap_page_prune_opt
is(non_normal_num($node_primary), qq(0), 'all items are normal in heap');
# Make sure hint bits are set
is(hints_num($node_primary), qq(10), 'hint bits are set on primary already');

## Wait for standbys to catch up hint bits
wait_for_catchup_all();

# Make sure read committed transaction is able to see correct data
ok(send_query_and_wait(\%psql_standby_read_committed,
    q/SELECT id FROM test_index_hint WHERE value = 0 ORDER BY id LIMIT 1;/,
    qr/11\n\(1 row\)/m),
    'session is not canceled for read committed');

# Make sure repeatable read transaction is canceled because of XLOG_INDEX_HINT_BITS_HORIZON from primary
ok((send_query_and_wait(\%psql_standby_repeatable_read,
    q/SELECT id FROM test_index_hint WHERE value = 0 ORDER BY id LIMIT 1;/,
    qr/.*terminating connection due to conflict with recovery.*/m)),
    'session is canceled for repeatable read');

# Try to set hint bits in index on standby
try_to_set_hint_bits();

is(hints_num($node_standby_1), qq(0),
    'hints are not set on standby1 because hs feedback is off');
is(hints_num($node_standby_2), qq(0),
    'hint bits are not set on standby2 because hs feedback chain is broker');

# Enable hot_standby_feedback to allow hint bits to be set
$node_standby_1->safe_psql('postgres',
    'ALTER SYSTEM SET hot_standby_feedback = on;');
$node_standby_1->reload;

# Make sure sender_propagates_feedback_to_primary is now set on standbys
wait_hfs($node_primary, 1);
wait_hfs($node_standby_1, 1);

# Try to set hint bits in index on standby
try_to_set_hint_bits();

is(hints_num($node_standby_1), qq(10),
    'hint bits are set on standby 1 yet because feedback is on');
is(hints_num($node_standby_2), qq(10),
    'hint bits are set on standby 2 yet because feedback chain is uninterrupted');

$node_primary->stop();
$node_standby_1->stop();
$node_standby_2->stop();

# Send query, wait until string matches
sub send_query_and_wait {
    my ($psql, $query, $untl) = @_;

    # send query
    $$psql{stdin} .= $query;
    $$psql{stdin} .= "\n";

    # wait for query results
    $$psql{run}->pump_nb();
    while (1) {
        # See PostgresNode.pm's psql()
        $$psql{stdout} =~ s/\r\n/\n/g if $Config{osname} eq 'msys';

        #diag("\n" . $$psql{stdout}); # for debugging
        #diag("\n" . $$psql{stderr}); # for debugging

        last if $$psql{stdout} =~ /$untl/;
        last if $$psql{stderr} =~ /$untl/;

        if ($psql_timeout->is_expired) {
            BAIL_OUT("aborting wait: program timed out \n" .
                "stream contents: >>$$psql{stdout}<< \n" .
                "pattern searched for: $untl");
            return 0;
        }
        if (not $$psql{run}->pumpable()) {
            # This is fine for some tests, keep running
            return 0;
        }
        $$psql{run}->pump();
        select(undef, undef, undef, 0.01); # sleep a little

    }

    $$psql{stdout} = '';

    return 1;
}

sub try_to_set_hint_bits {
    # Try to set hint bits in index on standby
    foreach (0 .. 3) {
        $node_standby_1->safe_psql('postgres',
            'SELECT * FROM test_index_hint WHERE value = 0 ORDER BY id LIMIT 1;');
        $node_standby_2->safe_psql('postgres',
            'SELECT * FROM test_index_hint WHERE value = 0 ORDER BY id LIMIT 1;');
    }
}

sub wait_for_catchup_all {
    $node_primary->wait_for_catchup($node_standby_1, 'replay',
        $node_primary->lsn('insert'));
    $node_standby_1->wait_for_catchup($node_standby_2, 'replay',
        $node_standby_1->lsn('replay'));
}

sub hints_num {
    my ($node) = @_;
    return $node->safe_psql('postgres',
        "SELECT count(*) FROM bt_page_items('test_index', 1) WHERE dead = true");
}

sub non_normal_num {
    my ($node) = @_;
    return $node->safe_psql('postgres',
        "SELECT COUNT(*) FROM heap_page_items(get_raw_page('test_index_hint', 0)) WHERE lp_flags != 1");
}

sub wait_hfs {
    my ($node, $n) = @_;
    $node->poll_query_until('postgres',
        "SELECT (SELECT COUNT(*) FROM (SELECT * FROM pg_stat_replication WHERE backend_xmin IS NOT NULL) AS X) = $n")
            or die 'backend_xmin is invalid';
    # Make sure we have received reply to feedback message
    sleep(2);
}