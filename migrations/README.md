# Migration policy

Refund-Go adopts the existing Node-owned PostgreSQL schema in place. Step 9.3
requires no schema mutation, so this directory intentionally contains no SQL
migration.

Future migrations must be:

1. additive and safe while Node and Go run against the same database;
2. idempotent (`IF NOT EXISTS` or an equivalent catalog guard);
3. reversible where PostgreSQL permits it;
4. tested against a copy of the Node schema before deployment; and
5. forbidden from renaming, dropping, rewriting, or changing the meaning of a
   Node-owned column until Node rollback is retired.

Startup readiness verifies required tables, columns, PostgreSQL enum labels,
and the two concurrency-critical unique indexes. Additional columns are
allowed so a newer Node deployment can add data without taking Go offline.
