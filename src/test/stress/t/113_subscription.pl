
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
