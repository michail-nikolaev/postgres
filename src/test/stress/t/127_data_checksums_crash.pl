
# Copyright (c) 2026, PostgreSQL Global Development Group

# Turning data checksums on and off while the relations are being
# rewritten underneath.
#
# Enabling checksums is not a switch.  A background worker walks every
# page of every relation, reads it, dirties it, logs a full page image
# and moves on, and the cluster reports "inprogress-on" until it has
# finished.  What it walks is the block count taken when it opened the
# relation -- and REPACK, CLUSTER and VACUUM FULL all swap a relation's
# relfilenode while it is open, while CREATE INDEX CONCURRENTLY adds
# relations it never enumerated.
#
# So this crosses the newest whole-database rewrite with the rewrites
# this suite already drives, and asks the only question that matters
# afterwards: with checksums on, does every page still verify?  The
# detector is the server's own counter -- pg_stat_database's
# checksum_failures, which nothing but a mismatch increments -- read
# after every page has been pulled through shared buffers, since a page
# is only verified when it is read.
#
# The chaos profile widens the gap between counting a relation's blocks
# and writing each of them, which is the window a concurrent swap has to
# land in.
use strict;
use warnings FATAL => 'all';

use FindBin;
use lib $FindBin::RealBin;
use Stress::Run;

run_scenario(
	'data_checksums_crash',
	{
		schema => [ 'pgbench', 'data_checksum_helper' ],
		pgbench_scale => 1,
		indexes => [ 'btree_abalance', 'btree_history_delta' ],
		load => ['tpcb_like'],
		ddl => [
			'repack_concurrently', 'reindex_table_concurrently',
			'drop_create_index', 'vacuum', 'toggle_data_checksums'
		],
		ddl_concurrency => 1,
		checks => [ 'balances', 'amcheck', 'no_checksum_failures' ],
		env => 'crash_loop',
		clients => 20,
		chaos => 'checksums',
		# The cluster has to start without checksums for the enabling
		# worker to have anything to do on the first turn.
		modifier => 'no_checksums',
	});
