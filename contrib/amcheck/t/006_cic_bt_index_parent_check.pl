# Copyright (c) 2025, PostgreSQL Global Development Group

# Test bt_index_parent_check with index created with CREATE INDEX CONCURRENTLY
use strict;
use warnings FATAL => 'all';

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;

use Test::More;

my ($node, $result);

#
# Test set-up
#
$node = PostgreSQL::Test::Cluster->new('CIC_bt_index_parent_check_test');
$node->init;
$node->start;
$node->safe_psql('postgres', q(CREATE EXTENSION amcheck));
$node->safe_psql('postgres', q(CREATE TABLE tbl(i int primary key)));
# Insert two rows into index
$node->safe_psql('postgres', q(INSERT INTO tbl SELECT i FROM generate_series(1, 2) s(i);));

# start background transaction
my $in_progress_h = $node->background_psql('postgres');
$in_progress_h->query_safe(q(BEGIN; SELECT pg_current_xact_id();));

# delete one row from table, while background transaction is in progress
$node->safe_psql('postgres', q(DELETE FROM tbl WHERE i = 1;));
# create index concurrently, which will skip the deleted row
$node->safe_psql('postgres', q(CREATE INDEX CONCURRENTLY idx ON tbl(i);));

# check index using bt_index_parent_check
$result = $node->psql('postgres', q(SELECT bt_index_parent_check('idx', heapallindexed => true)));
is($result, '0', 'bt_index_parent_check for CIC after removed row');

$in_progress_h->quit;
done_testing();
