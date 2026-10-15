# BRIN minmax-multi external TOAST reproducers

These standalone SQL scripts reproduce BRIN `numeric_minmax_multi_ops`
retaining external TOAST references after the referenced heap value is deleted
and vacuumed. They cover index construction and insertion into an existing
index. They are deliberately outside the standard regression suite, and do not
include a backend fix.

Verified on PostgreSQL 20devel at upstream commit
`294d2196c7abec253fd3959d3ca4c6dcae3068d6`, with the default 8 kB block size.

## Run

Use `psql` and `createdb` from the PostgreSQL installation under test, with a
running server from that installation. Set the usual connection options
(`PGHOST`, `PGPORT`, `PGUSER`) if needed. From the repository root:

```sh
createdb brin_toast_repro
psql -X -v ON_ERROR_STOP=1 -d brin_toast_repro -f reproducers/brin-minmax-multi-toast/repro-build.sql
psql -X -v ON_ERROR_STOP=1 -d brin_toast_repro -f reproducers/brin-minmax-multi-toast/repro-insert.sql
```

Run the two `psql` commands separately: each exits with status 3 when the bug is
reproduced. Use autocommit; do not pass `--single-transaction`, because `VACUUM`
must run outside a transaction block. The scripts use different table names
and can run independently or in either order in the same fresh database. To
repeat them, recreate this disposable database.

## Expected results

Both scripts insert a numeric with 5000 fractional digits. `STORAGE EXTERNAL`
forces it out of line without compression. Each query searching for `n = 2`
should return a count of zero.

In `repro-build.sql`, the large value is present when `CREATE INDEX` builds the
BRIN summary. The initial BRIN query returns zero. After `DELETE` and `VACUUM`
remove that value, the sequential query still returns zero, but the final BRIN
query fails.

In `repro-insert.sql`, `INSERT ... SELECT` copies the large value from a source
table into an already indexed destination. The initial BRIN query returns
zero. After deleting and vacuuming only the source table, the destination's
two numeric values remain readable, with text lengths 1 and 5002. The
sequential query returns zero, but the final BRIN query fails.

Immediately before the failing query, each script prints a plan containing a
`Bitmap Index Scan` on its BRIN index. The error is:

```text
ERROR:  missing chunk number 0 for toast value <oid> in pg_toast_<oid>
```

The OIDs vary between runs. With the bug fixed, the final query should also
return zero and `psql` should exit successfully.

Cleanup after both runs:

```sh
dropdb brin_toast_repro
```
