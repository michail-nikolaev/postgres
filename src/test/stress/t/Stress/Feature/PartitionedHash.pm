
# Copyright (c) 2026, PostgreSQL Global Development Group

# Hash partitioning, whose detach leaves a different substitute
# constraint from range's.
#
# See Stress::Registry for what each declaration means.

package Stress::Feature::PartitionedHash;

use strict;
use warnings FATAL => 'all';

use Test::More;
use Stress::Registry ':declare';

# Hash partitioning, which detaches differently from range.  The
# substitute constraint DETACH CONCURRENTLY used to leave behind
# names the parent's OID inside satisfies_hash_partition(), so it is
# only observable on a hash partition -- the equivalent constraint on
# a range partition is harmless and indistinguishable from the
# partition bound.
schema partitioned_hash => {
		setup => q(
			CREATE TABLE pgb_hash(id int PRIMARY KEY, val int)
				PARTITION BY HASH (id);
			CREATE TABLE pgb_hash_0 PARTITION OF pgb_hash
				FOR VALUES WITH (MODULUS 4, REMAINDER 0);
			CREATE TABLE pgb_hash_1 PARTITION OF pgb_hash
				FOR VALUES WITH (MODULUS 4, REMAINDER 1);
			CREATE TABLE pgb_hash_2 PARTITION OF pgb_hash
				FOR VALUES WITH (MODULUS 4, REMAINDER 2);
			CREATE TABLE pgb_hash_3 PARTITION OF pgb_hash
				FOR VALUES WITH (MODULUS 4, REMAINDER 3);
			INSERT INTO pgb_hash
				SELECT aid, 0 FROM pgbench_accounts ORDER BY aid LIMIT 4000;
		),
		tables => [qw(pgb_hash_0 pgb_hash_1 pgb_hash_2 pgb_hash_3)],
};

# Writes routed through the hash-partitioned parent, so a detach of
# one of its partitions has something to race.
load hash_dml => {
		weight => 2,
		requires => { schema => ['partitioned_hash'] },
		script => q(
			\set id random(1, 4000)
			\set d random(1, 100)
			-- Pruning sends this to one partition; while that partition
			-- is detached it matches nothing, which is not an error.
			UPDATE pgb_hash SET val = val + :d WHERE id = :id;
		),
};

# Detach and re-attach a hash partition.  Same command as the range
# case, different bound syntax, and a different substitute
# constraint to leave behind if the server gets it wrong.
ddl detach_hash_partition => {
		requires => { schema => ['partitioned_hash'] },
		checks => ['no_substitute_constraints'],
		# Names the parent while removing one of its partitions, so a
		# command gated on that partition could find it gone.
		solo => 1,
		variants => sub {
			return map {
				{
					table => 'pgb_hash',
					stmts => [
						"ALTER TABLE pgb_hash DETACH PARTITION pgb_hash_$_ CONCURRENTLY;",
						"SELECT pgb_ddl_bounded('ALTER TABLE pgb_hash "
						  . "ATTACH PARTITION pgb_hash_$_ FOR VALUES WITH "
						  . "(MODULUS 4, REMAINDER $_)');"
					]
				}
			} (0 .. 3);
		},
};

# DETACH CONCURRENTLY must not leave a substitute constraint behind.
# The one it used to create on a hash partition carries the OID of
# the parent the table is no longer related to, which breaks a dump
# and outlives the parent.
check no_substitute_constraints => {
		final => sub {
			my ($node, $ctx) = @_;
			my $bad = $node->safe_psql(
				'postgres', q(
				SELECT count(*) FROM pg_constraint c
				WHERE c.contype = 'c'
				  AND pg_get_constraintdef(c.oid) LIKE '%satisfies_hash_partition%'));
			Test::More::is($bad, '0',
				'no partition constraint left behind by DETACH');
		},
};

1;
