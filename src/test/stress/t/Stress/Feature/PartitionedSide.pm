
# Copyright (c) 2026, PostgreSQL Global Development Group

# A partitioned table of its own, for the detaches that remove
# rows the invariant counts.
#
# See Stress::Registry for what each declaration means.

package Stress::Feature::PartitionedSide;

use strict;
use warnings FATAL => 'all';

use Test::More;
use Stress::Registry ':declare';

# Rows in the side table.  Small enough to stay cheap, large enough that
# an index over one has more than a single page.
my $NROWS = 10_000;

# A partitioned table of its own, for the dimensions that need to
# detach a partition holding rows the invariant counts.
schema partitioned_side => {
		setup => qq(
			CREATE TABLE pgb_part(id int NOT NULL, val int) PARTITION BY RANGE (id);
			CREATE TABLE pgb_part_1 PARTITION OF pgb_part
				FOR VALUES FROM (1) TO (2501);
			CREATE TABLE pgb_part_2 PARTITION OF pgb_part
				FOR VALUES FROM (2501) TO (5001);
			CREATE TABLE pgb_part_3 PARTITION OF pgb_part
				FOR VALUES FROM (5001) TO (7501);
			CREATE TABLE pgb_part_4 PARTITION OF pgb_part
				FOR VALUES FROM (7501) TO ($NROWS + 1);
			ALTER TABLE pgb_part ADD PRIMARY KEY (id);
			-- See upsert_keys: two inferable arbiters rather than one,
			-- here on the partitioned table, so the per-partition
			-- mapping has to match both.
			CREATE UNIQUE INDEX pgb_part_id_uniq ON pgb_part(id);
			-- The first sixteen ids are the contention band: they carry
			-- no value, so partition_upsert_contend can delete and
			-- re-insert them without moving the sum the checks watch.
			INSERT INTO pgb_part
				SELECT g, CASE WHEN g <= 16 THEN 0 ELSE g END
				FROM generate_series(1, $NROWS) g;

			-- An upsert routed through the parent has nowhere to put the
			-- row while the partition covering it is detached, which the
			-- rotation does deliberately.  That is not what this load is
			-- looking for, so swallow it and let the arbiter-index work
			-- happen on every other attempt.
			CREATE FUNCTION pgb_part_upsert(p_id int) RETURNS boolean
			LANGUAGE plpgsql AS \$\$
			BEGIN
				INSERT INTO pgb_part VALUES (p_id, 0)
					ON CONFLICT (id) DO UPDATE SET val = pgb_part.val;
				RETURN true;
			EXCEPTION WHEN check_violation THEN
				RETURN false;
			END;
			\$\$;
		),
		# The parent is where the DML goes; the partitions are what the
		# CONCURRENTLY commands can be pointed at, since most of them
		# refuse a partitioned table.
		tables => [qw(pgb_part_1 pgb_part_2 pgb_part_3 pgb_part_4)],
		context => sub {
			my ($node) = @_;
			return {
				part_rows => $NROWS,
				part_sum =>
				  $node->safe_psql('postgres', 'SELECT SUM(val) FROM pgb_part'),
			};
		},
};

# The partitioned counterpart of upsert_contend: several clients
# racing to insert the same absent key through the parent, so the
# arbiter indexes have to be mapped onto a partition while one of
# them is being rebuilt.  partition_upsert never reaches that path,
# because the row it upserts always exists already.  Confined to the
# contention band, whose rows carry no value, so the sum the checks
# watch does not move -- and partition_sum only asserts when every
# row is present anyway.
load partition_upsert_contend => {
		weight => 3,
		requires => { schema => ['partitioned_side'] },
		script => q(
			\set id random(1, 16)
			\set mode random(0, 3)
			\if :mode = 0
				-- No routing needed: with the partition detached this
				-- prunes to nothing rather than failing.
				DELETE FROM pgb_part WHERE id = :id;
			\else
				-- Through the same wrapper partition_upsert uses, which
				-- swallows the check violation an upsert gets while the
				-- partition covering the row is detached.
				SELECT pgb_part_upsert(:id);
			\endif
		),
};

# DML through the partitioned parent, routed across all partitions,
# with both rows of the pair in the same partition so that each
# partition's sum is invariant on its own.
#
# Both rows are moved by a single statement on purpose.  A partition
# can be detached between two statements of one transaction, and then
# the second would match nothing and leave the pair unbalanced --
# which is a property of the test, not a bug in the server.  One
# statement sees one partition descriptor throughout.
load partition_dml => {
		weight => 3,
		requires => { schema => ['partitioned_side'] },
		script => q(
			\set part random(0, 3)
			\set a random(17, 2500)
			\set b random(17, 2500)
			\set lo least(:a, :b) + :part * 2500
			\set hi greatest(:a, :b) + :part * 2500
			\set diff random(1, 10000)
			-- The casts are what make this work under the prepared
			-- protocol, where a pgbench variable arrives as an untyped
			-- parameter and unary minus has nothing to resolve against.
			UPDATE pgb_part SET val = val
					+ CASE WHEN id = :lo THEN (:diff::int) ELSE 0 END
					+ CASE WHEN id = :hi THEN -(:diff::int) ELSE 0 END
				WHERE id IN (:lo, :hi);
		),
};

# ON CONFLICT routed through a partitioned table, where the arbiter
# indexes for each partition are worked out from the parent's.  An
# index that REINDEX CONCURRENTLY built on a partition has no parent
# of its own until the swap finishes, and has to be recognized as an
# arbiter anyway.
#
# Every id already exists, so this always takes the conflict path,
# and the update leaves val alone: the partitioned sum is untouched.
load partition_upsert => {
		weight => 3,
		requires => { schema => ['partitioned_side'] },
		script => q(
			\set id random(17, :part_rows)
			SELECT pgb_part_upsert(:id);
		),
};

# REINDEX of the partitioned index itself, as opposed to a leaf's.
# It rebuilds one child per partition, and each new child is
# unparented until the swap, so this is what drives the code that
# treats an unparented index as an additional arbiter.  The
# rotation's other reindex entries only ever name a leaf.
ddl reindex_partitioned_index => {
		requires => { schema => ['partitioned_side'] },
		variants => sub {
			return map {
				{
					table => 'pgb_part',
					stmts => ["REINDEX INDEX CONCURRENTLY $_;"]
				}
			} (qw(pgb_part_pkey pgb_part_id_uniq));
		},
};

# Detaching a partition concurrently leaves behind a CHECK constraint
# matching the bound, so the re-attach needs no validation scan.
ddl detach_partition_concurrently => {
		requires => { schema => ['partitioned_side'] },
		variants => sub {
			my ($ctx) = @_;
			my @v;
			my @bounds = (
				[ 1, 2501 ], [ 2501, 5001 ], [ 5001, 7501 ],
				[ 7501, $NROWS + 1 ]);
			for my $i (0 .. 3)
			{
				my $p = 'pgb_part_' . ($i + 1);
				push @v,
				  {
					table => 'pgb_part',
					stmts => [
						"ALTER TABLE pgb_part DETACH PARTITION $p CONCURRENTLY;",
						'\sleep 10 ms',
						"SELECT pgb_ddl_bounded('ALTER TABLE pgb_part "
						  . "ATTACH PARTITION $p FOR VALUES FROM "
						  . "($bounds[$i][0]) TO ($bounds[$i][1])');"
					]
				  };
			}
			return @v;
		},
};

# Detach a partition and then drop it outright.  That is the shape
# that reaches a partition descriptor being rebuilt for a cached plan
# whose partition no longer exists at all -- the catalog scan finds
# nothing where the descriptor expects a tuple, and the planner opens
# a relation that has gone.
#
# The rows are carried across to a replacement table before the drop,
# so the partitioned sum is the same on the other side.  Both ids a
# balanced update touches fall in the same partition, so while this
# one is detached that update moves neither of them.
ddl detach_drop_recreate_partition => {
		requires => { schema => ['partitioned_side'] },
		# The per-relation gate serializes commands that name the same
		# relation, and this one names the parent while destroying a
		# partition -- so a command gated on that partition can find it
		# gone.  That is a collision the suite arranged, not a race worth
		# reporting, so this variant only makes sense when one command
		# runs at a time.
		solo => 1,
		variants => sub {
			my ($ctx) = @_;
			my @v;
			my @bounds = (
				[ 1, 2501 ], [ 2501, 5001 ], [ 5001, 7501 ],
				[ 7501, $NROWS + 1 ]);
			for my $i (0 .. 3)
			{
				my $p = 'pgb_part_' . ($i + 1);
				push @v,
				  {
					table => 'pgb_part',
					stmts => [
						"ALTER TABLE pgb_part DETACH PARTITION $p CONCURRENTLY;",
						"CREATE TABLE ${p}_next (LIKE $p INCLUDING ALL);",
						"INSERT INTO ${p}_next SELECT * FROM $p;",
						"DROP TABLE $p;",
						"ALTER TABLE ${p}_next RENAME TO $p;",
						"SELECT pgb_ddl_bounded('ALTER TABLE pgb_part "
						  . "ATTACH PARTITION $p FOR VALUES FROM "
						  . "($bounds[$i][0]) TO ($bounds[$i][1])');"
					]
				  };
			}
			return @v;
		},
};

# CREATE INDEX CONCURRENTLY refuses a partitioned table; the
# documented way to build one without blocking writes is to create it
# on ONLY the parent, build a matching index on every partition, and
# attach them one by one, at which point the parent index becomes
# valid.
ddl partitionwise_index_build => {
		requires => { schema => ['partitioned_side'] },
		variants => sub {
			my ($ctx) = @_;

			# Start by clearing up after a previous attempt: this
			# sequence is several statements long and the run can end
			# anywhere in it, leaving the parent index, or a partition
			# index that was built but not yet attached, behind.
			# Dropping the parent takes its attached indexes with it.
			# IF EXISTS says so with a NOTICE when there is nothing to
			# drop, which is the normal case and would go to pgbench's
			# stderr, where the run insists on silence.
			my @stmts = (
				'SET client_min_messages = warning;',
				'DROP INDEX IF EXISTS pgb_part_val_idx;');
			push @stmts, "DROP INDEX IF EXISTS pgb_part_${_}_val_idx;"
			  for (1 .. 4);
			push @stmts, 'RESET client_min_messages;';

			push @stmts, 'CREATE INDEX pgb_part_val_idx ON ONLY pgb_part(val);';
			for my $i (1 .. 4)
			{
				push @stmts,
				  "CREATE INDEX CONCURRENTLY pgb_part_${i}_val_idx "
				  . "ON pgb_part_$i(val);",
				  "ALTER INDEX pgb_part_val_idx ATTACH PARTITION pgb_part_${i}_val_idx;";
			}
			push @stmts, '\sleep 10 ms', 'DROP INDEX pgb_part_val_idx;';
			return ({ table => 'pgb_part', stmts => [@stmts] });
		},
};

# Each partition's sum is invariant on its own, whether it is
# currently attached or not, and the parent must stay queryable while
# the partition descriptor changes underneath it.
check partition_sum => {
		weight => 1,
		requires => { schema => ['partitioned_side'] },
		script => q(
			SELECT COALESCE(SUM(val), 0) AS s, COUNT(*) AS c FROM pgb_part \gset p_
			\if :p_c = :part_rows
				-- Both sides are pgbench variables, so under the prepared
				-- protocol they arrive as parameters with nothing to
				-- resolve their type against unless they are cast.
				SELECT stress_assert(:p_s::bigint = :part_sum::bigint,
					format('partitioned sum is %s, not %s',
						:p_s::bigint, :part_sum::bigint));
			\endif
		),
};

1;
