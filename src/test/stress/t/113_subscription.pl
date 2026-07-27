
# Copyright (c) 2026, PostgreSQL Global Development Group

# Logical replication applying the workload while the published tables
# are rebuilt under the decoding, and the subscriber's own indexes --
# including the one behind its replica identity -- rebuilt under apply.
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
