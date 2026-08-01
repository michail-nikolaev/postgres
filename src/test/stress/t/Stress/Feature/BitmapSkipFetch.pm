
# Copyright (c) 2026, PostgreSQL Global Development Group

# A bitmap heap scan allowed to skip fetching pages, racing the
# vacuum that empties them.
#
# See Stress::Registry for what each declaration means.

package Stress::Feature::BitmapSkipFetch;

use strict;
use warnings FATAL => 'all';

use Test::More;
use Stress::Registry ':declare';

# A table shaped for a bitmap heap scan to skip fetching pages.
#
# The skip_fetch optimization lets a bitmap heap scan that needs no
# columns -- count(*) with an indexable qual and nothing else -- avoid
# reading a page at all when the visibility map says every tuple on it
# is visible, taking the tuple count from the bitmap instead.  The
# bitmap was built earlier, though, and a vacuum running since can
# have removed the dead TIDs in it and marked those pages
# all-visible.  The scan then counts TIDs that are no longer there.
#
# Wide rows at fillfactor 10 put roughly one row on a page, so a few
# thousand rows make a few thousand pages: enough that a scan takes
# long enough for a vacuum to get ahead of it, which is what the race
# needs.  Autovacuum is off so the vacuum that matters is the one the
# rotation runs.
schema bitmap_skip_fetch => {
		setup => q(
			CREATE TABLE pgb_bmskip (b int NOT NULL, pad char(1024) DEFAULT '')
				WITH (autovacuum_enabled = false, fillfactor = 10);
			INSERT INTO pgb_bmskip(b) SELECT g FROM generate_series(1, 4000) g;
			CREATE INDEX pgb_bmskip_b_idx ON pgb_bmskip(b);
			VACUUM (ANALYZE) pgb_bmskip;

			-- The same count taken two ways under one snapshot: once by a
			-- bitmap heap scan that is allowed to skip fetching pages, and
			-- once by a sequential scan that cannot.  They can only
			-- disagree if the bitmap scan counted TIDs that are not there.
			--
			-- The caller must be in a repeatable read transaction, or the
			-- two counts are taken under different snapshots.
			CREATE FUNCTION pgb_bmskip_check() RETURNS boolean
			LANGUAGE plpgsql AS $fn$
			DECLARE
				bitmap_count bigint;
				seq_count bigint;
				plan text;
			BEGIN
				-- No qual and no columns on the scan node is what makes
				-- the optimization applicable; the index condition does
				-- all the work.
				PERFORM set_config('enable_seqscan', 'off', true);
				PERFORM set_config('enable_indexscan', 'off', true);
				PERFORM set_config('enable_indexonlyscan', 'off', true);
				PERFORM set_config('enable_bitmapscan', 'on', true);
				-- Assert the shape rather than assume it: a bitmap heap
				-- scan is the whole point, and a modifier could take it
				-- away without anything noticing.
				EXECUTE 'EXPLAIN (COSTS OFF, FORMAT JSON) '
					'SELECT count(*) FROM pgb_bmskip WHERE b >= 0' INTO plan;
				IF plan NOT LIKE '%%Bitmap Heap Scan%%' THEN
					RAISE EXCEPTION 'not a bitmap heap scan: %', plan;
				END IF;

				SELECT count(*) INTO bitmap_count
					FROM pgb_bmskip WHERE b >= 0;

				PERFORM set_config('enable_seqscan', 'on', true);
				PERFORM set_config('enable_bitmapscan', 'off', true);
				SELECT count(*) INTO seq_count FROM pgb_bmskip;

				IF bitmap_count <> seq_count THEN
					RAISE EXCEPTION
						'bitmap heap scan counted % rows, the table has % under the same snapshot',
						bitmap_count, seq_count;
				END IF;
				RETURN true;
			END $fn$;
		),
		# Deliberately not in the rotation.  This table has no primary key
		# and no replica identity -- it is shaped for a bitmap heap scan,
		# not for being rewritten -- so REPACK (CONCURRENTLY) refuses it
		# and reindex_pkey_concurrently names an index that does not
		# exist.  The scenario's own DDL entries name it explicitly.
		# Found by soak, which combined it with the standard rotation.
		tables => [],
};

# The bitmap scan that has to agree with the heap.
load bmskip_check => {
		weight => 3,
		requires => { schema => ['bitmap_skip_fetch'] },
		script => q(
			BEGIN ISOLATION LEVEL REPEATABLE READ;
			SELECT pgb_bmskip_check();
			COMMIT;
		),
};

# Rows dying and coming back, so the bitmap has entries for TIDs a
# vacuum is about to remove.  The row count is left where it started.
load bmskip_churn => {
		weight => 4,
		requires => { schema => ['bitmap_skip_fetch'] },
		script => q(
			\set k random(1, 4000)
			BEGIN;
			DELETE FROM pgb_bmskip WHERE b = :k;
			INSERT INTO pgb_bmskip(b) VALUES (:k);
			COMMIT;
		),
};

# The vacuum that gets ahead of a bitmap scan and empties the pages
# its bitmap still refers to.
ddl vacuum_bmskip => {
		requires => { schema => ['bitmap_skip_fetch'] },
		variants => sub {
			return ({
				table => 'pgb_bmskip',
				stmts => ['VACUUM (TRUNCATE false) pgb_bmskip;']
			});
		},
};

1;
