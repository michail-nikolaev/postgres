# Checks that snapshots on standbys behave in a minimally reasonable
# way.
use strict;
use warnings;

use PostgresNode;
use TestLib;
use Test::More tests => 4;
use Config;


# Initialize primary node
my $node_primary = get_new_node('primary');
$node_primary->init(allows_streaming => 1);
$node_primary->append_conf('postgresql.conf', qq{
    autovacuum = off
});
$node_primary->start;

# Create test table with primary index
$node_primary->safe_psql(
    'postgres', 'CREATE TABLE test_index_hint (id int PRIMARY KEY)');
# Fill some data to it
$node_primary->safe_psql('postgres',
    'INSERT INTO test_index_hint VALUES (generate_series(1, 10000))');
# And vacuum to allow index hints to be set
$node_primary->safe_psql('postgres', 'VACUUM test_index_hint');

# Take backup
my $backup_name = 'my_backup';
$node_primary->backup($backup_name);

# Restore secondary node from backup backup
my $node_standby = get_new_node('standby');
$node_standby->init_from_backup($node_primary, $backup_name,
    has_streaming => 1);
$node_standby->append_conf('postgresql.conf', 'max_standby_streaming_delay=3s');
$node_standby->start;


# To avoid hanging while expecting some specific input from a psql
# instance being driven by us, add a timeout high enough that it
# should never trigger even on very slow machines, unless something
# is really wrong.
my $psql_timeout = IPC::Run::timer(30);

# One psql to run command in repeatable read isolation level
my %psql_standby_repeatable_read = ('stdin' => '', 'stdout' => '', 'stderr' => '');
$psql_standby_repeatable_read{run} =
    IPC::Run::start(
        [ 'psql', '-XAb', '-f', '-', '-d', $node_standby->connstr('postgres') ],
        '<', \$psql_standby_repeatable_read{stdin},
        '>', \$psql_standby_repeatable_read{stdout},
        '2>', \$psql_standby_repeatable_read{stderr},
        $psql_timeout);

# Another psql to run command in repeatable read isolation level
my %psql_standby_read_committed = ('stdin' => '', 'stdout' => '', 'stderr' => '');
$psql_standby_read_committed{run} =
    IPC::Run::start(
        [ 'psql', '-XAb', '-f', '-', '-d', $node_standby->connstr('postgres') ],
        '<', \$psql_standby_read_committed{stdin},
        '>', \$psql_standby_read_committed{stdout},
        '2>', \$psql_standby_read_committed{stderr},
        $psql_timeout);

# Start RR transaction and read first row from index
ok(send_query_and_wait(\%psql_standby_repeatable_read,
    q[
BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ;
SELECT id FROM test_index_hint ORDER BY id LIMIT 1;
],
    qr/1\n\(1 row\)/m),
    'row is visible without errors in repeatable read');

# Start RC transaction and read first row from index
ok(send_query_and_wait(\%psql_standby_read_committed,
    q[
BEGIN TRANSACTION ISOLATION LEVEL READ COMMITTED;
SELECT id FROM test_index_hint ORDER BY id LIMIT 1;
],
    qr/1\n\(1 row\)/m),
    'row is visible without errors in read committed');

# Now delete first 1000 rows in index
$node_primary->safe_psql('postgres',
    'DELETE FROM test_index_hint WHERE id <= 1000');

# Set index hint and replicate to standby
$node_primary->safe_psql('postgres',
    'SELECT * FROM test_index_hint ORDER BY id LIMIT 1000');

# Wait for standbys to catch up index hints
$node_primary->wait_for_catchup($node_standby, 'replay',
    $node_primary->lsn('insert'));

# Make sure repeatable read transaction is canceled because of index hint
ok(not send_query_and_wait(\%psql_standby_repeatable_read,
    q/SELECT id FROM test_index_hint ORDER BY id LIMIT 1;/,
    qr/1001\n\(1 row\)/m) && not $psql_standby_repeatable_read{run}->pumpable(),
    'session is canceled for repeatable read');

# Make sure read committed transaction is able to see correct data
ok(send_query_and_wait(\%psql_standby_read_committed,
    q/SELECT id FROM test_index_hint ORDER BY id LIMIT 1;/,
    qr/1001\n\(1 row\)/m),
    'session is not canceled for read commited');

$node_primary->stop();
$node_standby->stop();

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

        last if $$psql{stdout} =~ /$untl/;

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
    }

    $$psql{stdout} = '';

    return 1;
}
