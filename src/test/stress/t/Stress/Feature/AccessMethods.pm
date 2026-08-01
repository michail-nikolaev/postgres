
# Copyright (c) 2026, PostgreSQL Global Development Group

# Columns every non-btree access method has opclasses for, so the
# rebuilds can be driven against all of them.
#
# See Stress::Registry for what each declaration means.

package Stress::Feature::AccessMethods;

use strict;
use warnings FATAL => 'all';

use Test::More;
use Stress::Registry ':declare';

# Columns the non-btree access methods have opclasses for, so that
# CREATE INDEX CONCURRENTLY and its rebuilds can be driven against
# every AM rather than btree alone -- and against the table the
# workload is hammering, rather than one standing still.
schema am_columns => {
		setup => q(
			ALTER TABLE pgbench_accounts
				ADD COLUMN tags text[] DEFAULT ARRAY['tag'],
				ADD COLUMN p point DEFAULT point(0, 0),
				ADD COLUMN n int DEFAULT 0,
				ADD COLUMN ip inet DEFAULT '10.0.0.1';
		),
		indexes => [
			{
				table => 'pgbench_accounts',
				name => 'pgb_am_gin_idx',
				am => 'gin',
				defn => 'ON pgbench_accounts USING gin (tags)',
			},
			{
				table => 'pgbench_accounts',
				name => 'pgb_am_gist_idx',
				am => 'gist',
				defn => 'ON pgbench_accounts USING gist (p)',
			},
			{
				table => 'pgbench_accounts',
				name => 'pgb_am_brin_idx',
				am => 'brin',
				defn => 'ON pgbench_accounts USING brin (n)',
			},
			{
				table => 'pgbench_accounts',
				name => 'pgb_am_hash_idx',
				am => 'hash',
				defn => 'ON pgbench_accounts USING hash (n)',
			},
			{
				table => 'pgbench_accounts',
				name => 'pgb_am_spgist_idx',
				am => 'spgist',
				defn => 'ON pgbench_accounts USING spgist (ip)',
			},
		],
};

# Upserts over every column the access methods index, so each of them
# has insertions to absorb while it is being rebuilt.
load am_churn => {
		weight => 2,
		requires => { schema => ['am_columns'] },
		script => q(
			\set id random(1, :naccounts)
			\set n random(1, 1000000)
			UPDATE pgbench_accounts SET tags = ARRAY[md5(random()::text)],
				p = point(:n, :n), n = :n,
				ip = ('10.0.0.' || (:n % 255))::inet
				WHERE aid = :id;
		),
};

1;
