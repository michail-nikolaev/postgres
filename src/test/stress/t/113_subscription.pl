
# Copyright (c) 2026, PostgreSQL Global Development Group

# Logical replication applying the workload while the published tables
# are rebuilt under the decoding, and the subscriber's own indexes --
# including the one behind its replica identity -- rebuilt under apply.
#
# Without the fix in FindReplTupleInLocalRel() that takes the identity
# index from the relation cache rather than trusting the OID it was
# handed, this fails with the subscriber going down on
#
#   TRAP: failed Assert("GetRelationIdentityOrPK(localrel) ==
#   localidxoid || ...")
#
# twice in sixteen runs at stress_concurrently=6 with the identity
# rebuild weighted up (stress_repl_identity_rebuild=1), and none in
# twenty-nine afterwards.  Without assertions the same staleness aims
# the lookup at an index that REINDEX CONCURRENTLY has dropped.
#
# Without the fix in execReplication.c that looks the tuple up under an
# MVCC snapshot rather than a dirty one, this fails twice in eight runs
# at stress_concurrently=4 -- and not at all at the default duration --
# with the apply and tablesync workers logging
#
#   conflict detected on relation "public.pgbench_tellers":
#   conflict=update_missing
#
# because a dirty index scan can miss a tuple updated concurrently when
# the new version lands in a part of the scan already visited.
use strict;
use warnings FATAL => 'all';

use FindBin;
use lib $FindBin::RealBin;
use Stress::Run;

run_scenario(
	'subscription',
	{
		schema => ['pgbench'],
		indexes => ['btree_abalance'],
		load => [
			'tpcb_like', 'subscriber_churn',
			'subscriber_delete_reinsert'
		],
		ddl => [@STANDARD_DDL],
		ddl_concurrency => 1,
		checks => [ 'balances', 'amcheck' ],
		env => 'subscription',
		clients => 20,
		tags => ['ci'],
	});
