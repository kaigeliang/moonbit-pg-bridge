# MoonBit PostgreSQL Bridge

A MoonBit adapter that lets code written against `moonbitstack/moondb.AsyncDriver`
use the `moonbit-community/postgres/client` PostgreSQL driver. The bridge maps
parameters, rows, and errors to moondb types and manages PostgreSQL transactions
and savepoints.

**Status** · Experimental. Verified locally on macOS with PostgreSQL 15.18,
MoonBit 0.1.20261117, `moondb@0.3.0`, `postgres@0.1.3`, and `async@0.22.5`.
The native and classic wasm targets each passed the PostgreSQL integration suite
and the independent consumer test.

## Usage and ownership

Create the community client, spawn its executor, wait for `ready()`, and pass the
client to `Bridge::new`. Keep the executor running until the bridge closes and
use the bridge as the exclusive owner of that client. The
[independent consumer](consumer/consumer_test.mbt) shows setup and calls through
`&@db.AsyncDriver`.

### Scoped transactions and savepoints

Use `Bridge::with_transaction` for a callback that owns the connection until it
returns. Call `execute` and `query` on the supplied `TransactionScope`. The bridge
commits on normal return and forwards the callback result. On failure it rolls
back and propagates the callback error.

`TransactionScope::with_savepoint` gives a nested callback its own handle. On
success it releases the savepoint. On failure it rolls back and releases the
savepoint. If the outer callback catches the error, it can continue and commit
its other work. The same name can be reused at different nesting levels. Names
are quoted PostgreSQL identifiers of 1–63 UTF-8 bytes and may contain spaces or
quotes. NUL is rejected.

```moonbit nocheck
///|
let rows = bridge.with_transaction(tx => {
  tx.execute("CREATE TEMP TABLE demo_items (id int) ON COMMIT DROP", [])
  |> ignore
  tx.with_savepoint("add item", child => {
    child.execute("INSERT INTO demo_items VALUES ($1)", [Int(7)]) |> ignore
  })
  tx.query("SELECT id FROM demo_items", [])
})
```

Pass `options=@pg.TransactionOptions(isolation_level=RepeatableRead, read_only=true)`
when the operation needs explicit transaction settings. While a scope runs, use
only the innermost active handle. The bridge rejects calls through the base
driver, suspended parent handles, expired handles, and overlapping SQL
operations. Transaction control in a scope goes through the callback API.

If PostgreSQL rejects a statement, the bridge marks that scope failed. Even if
the callback catches the statement error and returns successfully, the bridge
rolls back the failed scope and reports a `QueryError`. A cancellation between
database operations rolls back the scope. Cancellation during an active database
operation discards the connection. Keep any tasks using a scope within its
callback lifetime. If active work outlives that callback, the bridge aborts the
connection. A failure during commit can leave the outcome unknown.

### Explicit transactions

The moondb `AsyncDriver` methods `begin`, `commit`, and `rollback` are available
for callers that manage the transaction lifecycle. Within an explicit
transaction, `Bridge::with_savepoint` runs a bounded callback and then returns
control to the caller. The bridge also supports savepoint recovery through
moondb's nested `AsyncTxn::run`. Callers using explicit transactions are
responsible for coordinating access between `begin` and completion.

An unrecovered PostgreSQL error in an explicit transaction makes a later
`commit` roll back and return a `QueryError`. Server errors retain SQLSTATE,
severity, detail, and hint. Errors establishing or completing a transaction
discard the connection. `close` is idempotent and aborts a connection with
active work or an open transaction.

## Type and operation support

Inputs are `Null`, `Bool`, `Int`, `Int64`, `Double`, `Text`, `Blob`, and `Json`.
The upstream client checks each parameter against the PostgreSQL type. Outputs
include BOOL, INT2, INT4, INT8, FLOAT4, FLOAT8, TEXT, VARCHAR, BYTEA, JSON and
JSONB with SQL NULL. Other types, including NUMERIC, dates, UUID and arrays,
return a type error. JSON binding preserves number representations. JSONB may
normalize whitespace and key order. Columns keep their order and names.

SQL uses PostgreSQL `$1`, `$2`, … placeholders. Use `INSERT ... RETURNING` to
get an inserted key. `last_insert_id` is zero. Queries materialize their rows
in memory. The bridge reports support for transactions, savepoints, and
RETURNING.

## Reproduce the PostgreSQL tests

Use MoonBit 0.1.20261117 with `initdb`, `pg_ctl`, and `pg_isready` on PATH.

```sh
moon check --target native --deny-warn
moon check --target wasm --deny-warn
scripts/integration.sh
PG_BRIDGE_TARGET=wasm scripts/integration.sh
```

Each script run starts a temporary PostgreSQL cluster on loopback port 55438,
runs four database scenarios and one independent consumer test on the requested
target, and stops the server on exit. Set `PG_BRIDGE_PORT` to use another port.
The consumer imports the bridge from a temporary workspace populated from the
checkout. Direct `moon test` runs the database scenarios only when
`PG_BRIDGE_INTEGRATION=1` is set.

The database scenarios cover parameter and row conversions, transaction commit
and rollback, nested savepoint release, recovery from SQL errors, SQLSTATE
propagation, access from expired or suspended handles, and cancellation both
between and during SQL operations.

## MoonBit implementation size

The table counts implementation lines after excluding blank lines and
whole-line comments. It excludes tests, generated interfaces, and dependencies.

| File | Implementation lines |
| --- | ---: |
| `bridge.mbt` | 261 |
| `transaction.mbt` | 204 |
| `scope_sql.mbt` | 91 |
| **Total** | **556** |

```sh
awk 'NF && $1 !~ /^\/\// { ++lines[FILENAME]; ++total }
     END { for (i = 1; i < ARGC; ++i) print ARGV[i], lines[ARGV[i]]
           print "total", total }' bridge.mbt transaction.mbt scope_sql.mbt
```
