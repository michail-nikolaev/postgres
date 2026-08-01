
# Copyright (c) 2026, PostgreSQL Global Development Group

# VACUUM of a hash index, with the index's relcache entry invalidated
# while the bucket scan is under way.
#
# hashbulkdelete() takes the metapage from _hash_getcachedmetap(), which
# hands back the relcache's own copy in rel->rd_amcache, and holds that
# pointer across its loop over buckets -- and gives the same pointer to
# the read stream callback, which dereferences it while prefetching.
#
# The loop accepts invalidation messages, and an invalidation for this
# index pfrees rd_amcache.  From then on BUCKET_TO_BLKNO() is reading
# freed memory, and the block number it computes is whatever happens to
# be there:
#
#   ERROR:  could not open file "base/5/16388.1" (target block 2139062145):
#           previous segment is only 66 blocks
#   CONTEXT:  while scanning relation "public.t"
#
# 2139062145 is 0x7F7F7F81, which is CLOBBER_FREED_MEMORY's fill pattern
# with the bucket arithmetic applied to it -- the value says outright
# that the metapage had already been freed.
#
# No concurrency and no second session are needed.  debug_discard_caches
# makes AcceptInvalidationMessages() flush unconditionally, so the
# invalidation does not have to be supplied by anyone; and
# vacuum_cost_delay is what makes the bucket loop yield often enough to
# absorb one.  Both are load-bearing: without the delay this does not
# reproduce at all.
use strict;
use warnings FATAL => 'all';

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

# debug_discard_caches does nothing unless DISCARD_CACHES_ENABLED is
# defined, which follows USE_ASSERT_CHECKING; elsewhere the GUC rejects
# any value above zero.
if (!check_pg_config('#define USE_ASSERT_CHECKING 1'))
{
	plan skip_all => 'this build does not have debug_discard_caches';
}

my $node = PostgreSQL::Test::Cluster->new('hash_vacuum');
$node->init;
$node->start;

# Two hash indexes on the same column, so a parallel vacuum has one for
# each worker.  Ten thousand rows is enough to give each index about
# sixty buckets, which is more than enough loop to be interrupted in.
$node->safe_psql(
	'postgres', q[
	CREATE TABLE t (a int);
	INSERT INTO t SELECT g FROM generate_series(1, 10000) g;
	CREATE INDEX h1 ON t USING hash (a);
	CREATE INDEX h2 ON t USING hash (a);
]);

# Dead tuples, so that ambulkdelete actually runs and walks the buckets.
$node->safe_psql('postgres', 'UPDATE t SET a = a WHERE a <= 200');

# The vacuum itself.  A failure is reported by the parallel worker and
# re-thrown by the leader, so it arrives as an ordinary statement error.
my ($rc, $out, $err) = $node->psql(
	'postgres', q[
	SET debug_discard_caches = 1;
	SET vacuum_cost_delay = 20;
	SET vacuum_cost_limit = 8;
	VACUUM (PARALLEL 2) t;
], on_error_stop => 0);

unlike(
	$err,
	qr/could not open file/,
	'vacuum did not compute a block number from freed memory');
is($rc, 0, 'vacuum of the hash indexes succeeded') or diag($err);

# The server log is checked as well as the client: the error is raised in
# a parallel worker, and this is what the failure looks like there.
ok( !$node->log_contains(qr/could not open file .* previous segment is only/,
		undef),
	'no worker reported a block beyond the end of the relation');

# And the indexes still work.
is( $node->safe_psql(
		'postgres', q[
		SET enable_seqscan = off;
		SELECT count(*) FROM t WHERE a = 5000]),
	'1',
	'the hash index still answers');

$node->stop;
done_testing();
