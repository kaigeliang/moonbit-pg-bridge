///|
/// A bridge owns an already-running community PostgreSQL client.
/// Callers must keep its executor alive and serialize access to this bridge.
pub struct Bridge {
  client : @pg.Client
  priv mut transaction : TransactionState?
  priv mut scope : TransactionScope?
  mut closed : Bool
  mut busy : Bool
}

///|
pub fn Bridge::new(client : @pg.Client) -> Bridge {
  { client, transaction: None, scope: None, closed: false, busy: false, }
}

///|
fn Bridge::check_open(self : Bridge) -> Unit raise @db.DbError {
  if self.closed || self.client.is_closed() {
    self.closed = true
    self.transaction = None
    self.scope = None
    raise @db.Closed
  }
}

///|
fn Bridge::enter(
  self : Bridge,
  scope? : TransactionScope,
) -> Unit raise @db.DbError {
  self.check_open()
  match (scope, self.scope) {
    (None, None) => ()
    (Some(requested), Some(active)) if physical_equal(requested, active) => ()
    (None, Some(_)) =>
      raise @db.QueryError("bridge has an active transaction scope")
    (Some(_), _) => raise @db.QueryError("transaction scope is not active")
  }
  if self.busy {
    raise @db.QueryError("bridge is busy")
  }
  self.busy = true
}

///|
fn Bridge::abort(self : Bridge) -> Unit {
  self.closed = true
  self.transaction = None
  self.scope = None
  self.client.abort()
}

///|
fn map_error(e : Error) -> @db.DbError {
  match e {
    @pg.ClientError::Database(d) =>
      @db.Server({
        kind: match d.code {
          Some(code) => @db.classify_sqlstate(code)
          None => @db.ErrorKind::Database
        },
        sqlstate: d.code,
        code: None,
        message: d.message,
        detail: d.detail,
        hint: d.hint,
        severity: d.severity,
      })
    @db.DbError::TypeError(m) => @db.TypeError(m)
    @pg.ClientError::Closed(_) => @db.Closed
    @pg.ClientError::WrongType(_)
    | @pg.ClientError::Encode(_)
    | @pg.ClientError::Decode(_) => @db.TypeError(e.to_string())
    _ => @db.QueryError(e.to_string())
  }
}

///|
fn parameters(values : Array[@db.Value]) -> Array[&@pg.ToSql] raise @db.DbError {
  let out : Array[&@pg.ToSql] = []
  for value in values {
    match value {
      Null => {
        let x : String? = None
        out.push(x as &@pg.ToSql)
      }
      Bool(x) => out.push(x as &@pg.ToSql)
      Int(x) => out.push(x as &@pg.ToSql)
      Int64(x) => out.push(x as &@pg.ToSql)
      Double(x) => out.push(x as &@pg.ToSql)
      Text(x) => out.push(x as &@pg.ToSql)
      Blob(x) => out.push(x as &@pg.ToSql)
      Json(text) => {
        let value = @json.parse(text, preserve_number_repr=true) catch {
          _ => raise @db.TypeError("invalid JSON parameter")
        }
        out.push(value as &@pg.ToSql)
      }
      _ => raise @db.TypeError("unsupported parameter: " + value.kind())
    }
  }
  out
}

///|
fn convert_row(row : @pg.Row) -> @db.Row raise {
  let names = []
  let values = []
  for i = 0; i < row.len(); i = i + 1 {
    let col = row.columns()[i]
    names.push(col.name)
    let ty = col.type_
    if !(ty == @pg.Type::bool() ||
      ty == @pg.Type::int2() ||
      ty == @pg.Type::int4() ||
      ty == @pg.Type::int8() ||
      ty == @pg.Type::float4() ||
      ty == @pg.Type::float8() ||
      ty == @pg.Type::text() ||
      ty == @pg.Type::varchar() ||
      ty == @pg.Type::bytea() ||
      ty == @pg.Type::json() ||
      ty == @pg.Type::jsonb()) {
      raise @db.TypeError(
        "unsupported PostgreSQL type \{ty.name} (OID \{ty.oid}) in column \{col.name} at index \{i}",
      )
    }
    let v : @db.Value = if row.get_raw(i) is None {
      Null
    } else if ty == @pg.Type::bool() {
      Bool(row.get(i))
    } else if ty == @pg.Type::int2() || ty == @pg.Type::int4() {
      Int(row.get(i))
    } else if ty == @pg.Type::int8() {
      Int64(row.get(i))
    } else if ty == @pg.Type::float4() {
      let f : Float = row.get(i)
      Double(f.to_double())
    } else if ty == @pg.Type::float8() {
      Double(row.get(i))
    } else if ty == @pg.Type::text() || ty == @pg.Type::varchar() {
      Text(row.get(i))
    } else if ty == @pg.Type::json() || ty == @pg.Type::jsonb() {
      let raw = row.get_raw(i).unwrap()
      let bytes = if ty == @pg.Type::jsonb() && col.format is Binary {
        guard raw.length() > 0 && raw[0] == b'\x01' else {
          raise @db.TypeError("invalid JSONB version")
        }
        raw[1:]
      } else {
        raw[:]
      }
      Json(@utf8.decode(bytes))
    } else if ty == @pg.Type::bytea() {
      Blob(row.get(i))
    } else {
      raise @db.TypeError(
        "unsupported PostgreSQL type \{ty.name} (OID \{ty.oid}) in column \{col.name} at index \{i}",
      )
    }
    values.push(v)
  }
  { columns: names, values, }
}

///|
async fn Bridge::execute_on(
  self : Bridge,
  tx : TransactionState?,
  sql : String,
  params : Array[@db.Value],
) -> @db.ExecResult raise @db.DbError {
  errdefer (if @async.is_being_cancelled() { self.abort() })
  let p = parameters(params)
  let n = try {
    match tx {
      Some(state) => state.handle.execute(sql, params=p)
      None => self.client.execute(sql, params=p)
    }
  } catch {
    e => raise operation_error(tx, e)
  }
  if n > 9223372036854775807UL {
    raise @db.QueryError("affected row count exceeds Int64")
  }
  { rows_affected: n.reinterpret_as_int64(), last_insert_id: 0L, }
}

///|
async fn Bridge::query_on(
  self : Bridge,
  tx : TransactionState?,
  sql : String,
  params : Array[@db.Value],
) -> Array[@db.Row] raise @db.DbError {
  errdefer (if @async.is_being_cancelled() { self.abort() })
  let p = parameters(params)
  try {
    let rows = match tx {
      Some(state) => {
        state.stream_error = None
        let stream = state.handle.query(sql, params=p)
        let rows = stream.collect() catch {
          @pg.ClientError::Database(payload) as error => {
            state.stream_error = Some(payload)
            raise error
          }
          error => raise error
        }
        if state.failed && is_rollback_to_savepoint(sql) {
          state.failed = false
        }
        rows
      }
      None => self.client.query(sql, params=p).collect()
    }
    rows.map(convert_row)
  } catch {
    e => raise operation_error(tx, e)
  }
}

///|
pub impl @db.AsyncDriver for Bridge with fn execute(self, sql, params) {
  self.enter()
  defer {
    self.busy = false
  }
  let tx = self.transaction
  if tx is Some(state) && state.failed && is_rollback_to_savepoint(sql) {
    // Register a new stream so the client forgets the completed stream
    // that reported the error before this savepoint recovery.
    self.query_on(tx, sql, params) |> ignore
    { rows_affected: 0L, last_insert_id: 0L, }
  } else {
    self.execute_on(tx, sql, params)
  }
}

///|
pub impl @db.AsyncDriver for Bridge with fn query(self, sql, params) {
  self.enter()
  defer {
    self.busy = false
  }
  self.query_on(self.transaction, sql, params)
}

///|
pub impl @db.AsyncDriver for Bridge with fn begin(self) {
  self.transaction = Some(self.start_transaction(@pg.TransactionOptions()))
}

///|
pub impl @db.AsyncDriver for Bridge with fn commit(self) {
  self.finish_manual_transaction(commit=true)
}

///|
pub impl @db.AsyncDriver for Bridge with fn rollback(self) {
  self.finish_manual_transaction(commit=false)
}

///|
pub impl @db.AsyncDriver for Bridge with fn close(self) {
  if !self.closed {
    if self.busy || self.scope is Some(_) || self.transaction is Some(_) {
      self.abort()
      return
    }
    self.closed = true
    @async.protect_from_cancel(() => self.client.close()) catch {
      _ => ()
    }
    self.transaction = None
  }
}

///|
pub impl @db.AsyncDriver for Bridge with fn dialect(_self) {
  @db.Dialect::Postgres
}

///|
/// Report only features exposed by this bridge's API.
pub impl @db.AsyncDriver for Bridge with fn capabilities(_self) {
  {
    dialect: Postgres,
    prepared_statements: false,
    named_parameters: false,
    server_cursors: false,
    streaming_copy: false,
    notifications: false,
    transactions: true,
    savepoints: true,
    multiple_result_sets: false,
    unsigned_integers: false,
    returning: true,
  }
}
