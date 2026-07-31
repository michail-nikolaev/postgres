
# Copyright (c) 2026, PostgreSQL Global Development Group

# Enabling data checksums while the relcache is being invalidated.
#
# ProcessSingleRelationByOid() opens the relation, calls RelationGetSmgr()
# once for its side effect, and then reads rel->rd_smgr directly on each
# iteration of the loop over forks.  Only RelationGetSmgr() is authorized
# to read that field, because a relcache invalidation resets it to NULL --
# and the body of the loop invites one: ProcessSingleRelationFork() reads
# catalogs through get_namespace_name(), pins and dirties buffers, enters
# and leaves critical sections, and checks for interrupts.
#
# When an invalidation lands between two forks of the same relation, the
# next iteration passes NULL to smgrexists() and the worker dies:
#
#   background worker "datachecksums worker" was terminated by signal 11
#   DETAIL:  Failed process was running: processing:
#            pg_catalog.pg_db_role_setting (main, 0 blocks)
#
# The relation named varies from run to run and always has no blocks: one
# with pages spends long enough in the block loop to be invalidated there
# instead, where rd_smgr is not read again.
#
# debug_discard_caches makes the invalidation arrive at every opportunity
# rather than waiting for concurrent DDL to supply one, so no concurrency
# is needed here at all -- an idle cluster and one function call are
# enough.
use strict;
use warnings FATAL => 'all';

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

use FindBin;
use lib $FindBin::RealBin;

use DataChecksums::Utils;

# debug_discard_caches only does anything where DISCARD_CACHES_ENABLED is
# defined, which follows USE_ASSERT_CHECKING; elsewhere the GUC rejects
# any value above zero and the server would refuse to start.
if (!check_pg_config('#define USE_ASSERT_CHECKING 1'))
{
	plan skip_all => 'this build does not have debug_discard_caches';
}

my $node = PostgreSQL::Test::Cluster->new('discard_caches');
$node->init(no_data_checksums => 1);
$node->append_conf('postgresql.conf', 'debug_discard_caches = 1');
$node->start;

test_checksum_state($node, 'off');

# A little user data, so the worker has relations of its own to walk
# besides the catalogs.  It is the empty ones that expose this, but there
# is no need to depend on that.
$node->safe_psql('postgres',
	'CREATE TABLE t AS SELECT generate_series(1, 1000) AS a');

# The whole test: the worker must survive walking the cluster while every
# catalog access flushes the caches.
enable_data_checksums($node, wait => 'on');

test_checksum_state($node, 'on');

# And the cluster must still be there.  Without the fix the worker
# segfaults, the postmaster takes the cluster down with it, and this is
# the check that says so in as many words rather than leaving a bare
# connection failure above.
is($node->safe_psql('postgres', 'SELECT count(*) FROM t'),
	'1000', 'the cluster survived enabling checksums');

ok(!$node->log_contains(qr/was terminated by signal/, undef),
	'no backend was terminated by a signal');

$node->stop;

done_testing();
