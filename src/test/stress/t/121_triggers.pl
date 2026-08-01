
# Copyright (c) 2026, PostgreSQL Global Development Group

# Triggers, and the two things they bring that the rest of the suite
# does not have.
#
# Rows that arrive from inside a trigger.  Every row of pgb_audit is
# written by an after-update trigger on pgbench_accounts rather than by
# a statement pgbench sent, so its indexes are maintained from the
# after-trigger queue while the rotation rebuilds them.  A deferred
# constraint trigger then reads each row back at commit time, through
# an index a rebuild may have swapped since the insert; the row is the
# transaction's own, so not finding it means the index lost it.
#
# Queries that run from a cached plan.  Everything else in this suite is
# parsed afresh and can only see the catalog as it stands; a plpgsql
# body is planned once and reused, so a rebuild that swaps or drops the
# index the plan chose has to reach that plan and make it replan.  The
# upsert in pgb_calls_trigger has to re-infer its arbiter index every
# time from such a plan, and audit_probe reads through an index that
# drop_create_index takes away underneath it.
#
# The DDL forms are the trigger ones: created and dropped as an ordinary
# trigger and as a constraint trigger, switched off and on again, and
# the trigger functions altered -- which takes no table lock at all and
# so invalidates every cached plan in every session at whatever rate the
# rotation can manage.
use strict;
use warnings FATAL => 'all';

use FindBin;
use lib $FindBin::RealBin;
use Stress::Run;

run_scenario(
	'triggers',
	{
		indexes => [ 'btree_abalance', 'btree_audit_aid' ],
		load => [ 'tpcb_like', 'trigger_upsert_log', 'audit_probe' ],
		ddl => [
			'repack_concurrently', 'reindex_table_concurrently',
			'reindex_pkey_concurrently', 'drop_create_index',
			'create_drop_trigger', 'create_drop_constraint_trigger',
			'toggle_trigger', 'alter_trigger_function'
		],
		clients => 20,
		tags => ['ci'],
	});
