# MoonBit PostgreSQL Bridge

A MoonBit adapter connecting `moonbit-community/postgres/client` to
`moonbitstack/moondb.AsyncDriver`. It enables a consumer written against the
moondb interface to use an existing community PostgreSQL client. It does not
implement another PostgreSQL wire protocol.

**Status:** experimental, not published. Tested locally on macOS with PostgreSQL
15.18, MoonBit 0.1.20261117, `moondb@0.3.0`, `postgres@0.1.3`, and
`async@0.22.5`. Native and classic wasm integration tests have run locally. No CI results or independent package publication are
claimed.

## Usage and ownership

Create the community client, spawn its executor, wait for `ready()`, then pass the
client to `Bridge::new` and call it through `&@moondb.AsyncDriver`. See the
[separate consumer](consumer/consumer_test.mbt). The bridge owns the client:
keep the executor running until the bridge closes, and do not call or share the
underlying client while the bridge is in use.

One operation at a time is accepted; overlapping calls return `QueryError` rather
than waiting. This prevents simultaneous manipulation of the handle, but does
**not** assign transaction ownership to a task. Callers must ensure all calls
between begin and commit/rollback belong to the same logical unit of work. Do not
share the handle across uncoordinated tasks or pool it. If a task is cancelled
while operating on the bridge, the connection is aborted and must be replaced;
server work may continue until the connection is closed. No automatic retry is
performed. A failed commit has an unknown outcome: callers must not assume the
transaction committed or rolled back.

Server errors retain SQLSTATE, severity, detail and hint. After an ordinary SQL
error inside a transaction, explicitly roll back; PostgreSQL generally marks the
transaction failed. A failed BEGIN, COMMIT or ROLLBACK invalidates the connection.
`close` is idempotent; closing while another call is active aborts the connection.

## Type and operation boundary

Inputs: `Null`, `Bool`, `Int`, `Int64`, `Double`, `Text`, `Blob`, `Json`. The upstream
client checks each parameter against the PostgreSQL type. Outputs: BOOL, INT2,
INT4, INT8, FLOAT4, FLOAT8, TEXT, VARCHAR, BYTEA, JSON and JSONB, including SQL NULL. Other
`moondb.Value` cases and PostgreSQL types, including numeric, dates, UUID
and arrays, are rejected. Columns retain their order and names. This is a
bounded subset, not full PostgreSQL type support.

SQL uses PostgreSQL `$1`, `$2`, … placeholders. `last_insert_id` is zero; use
`INSERT ... RETURNING`. Queries materialize all rows in memory. The bridge's
capabilities report transactions and RETURNING, but not server cursors,
savepoints, notifications, or true prepared-statement handles.

## Reproduce

Requires MoonBit 0.1.20261117 and PostgreSQL utilities (`initdb`, `pg_ctl`,
`pg_isready`) on PATH:

```sh
moon check --target native --deny-warn
moon test --target native
scripts/integration.sh
PG_BRIDGE_TARGET=wasm scripts/integration.sh
```

The ordinary test command skips the database scenario unless
`PG_BRIDGE_INTEGRATION=1`; a passing ordinary test is not a database result.
The integration script starts a temporary trust-authenticated database on
127.0.0.1:55438 (override with `PG_BRIDGE_PORT`), runs the database suite and copies the source and a separate
consumer into a temporary workspace to run its test. It refuses an occupied
PostgreSQL port, stops the server on exit, and leaves temporary files for
inspection. The consumer test uses local workspace source: it does **not** prove
registry installation or independent deployment.

The native and wasm real-database scenario covers binding and conversion of supported scalar
types, nulls, commits, rollback after SQL error, SQLSTATE, concurrent rejection,
cancellation and a fresh connection checking that cancelled work was not
committed. These are targeted checks, not exhaustive concurrency or failure
injection testing.

## Remaining work

Validate more cancellation timings, connection loss and transaction ownership;
test multiple PostgreSQL versions and operating systems; decide whether to add
more types and streaming; exercise actual downstream moondb consumers; publish a
reviewed release and verify installation from the registry. The project has not
been registered or submitted to the competition.
