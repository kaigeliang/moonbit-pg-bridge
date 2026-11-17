///|
/// A bridge owns an already-running community PostgreSQL client.
/// Callers must keep its executor alive and serialize access to this bridge.
pub struct Bridge {
  client : @pg.Client
  mut transaction : @pg.Transaction?
  mut closed : Bool
  mut busy : Bool
}

///|
pub fn Bridge::new(client : @pg.Client) -> Bridge {
  { client, transaction: None, closed: false, busy: false, }
}

///|
fn Bridge::enter(self : Bridge) -> Unit raise @db.DbError {
  if self.closed || self.client.is_closed() {
    self.closed = true
    raise @db.Closed
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
pub impl @db.AsyncDriver for Bridge with fn execute(self, sql, params) {
  self.enter()
  defer {
    self.busy = false
  }
  errdefer (if @async.is_being_cancelled() { self.abort() })
  let p = parameters(params)
  let n = try {
    match self.transaction {
      Some(tx) => tx.execute(sql, params=p)
      None => self.client.execute(sql, params=p)
    }
  } catch {
    e => raise map_error(e)
  }
  if n > 9223372036854775807UL {
    raise @db.QueryError("affected row count exceeds Int64")
  }
  { rows_affected: n.reinterpret_as_int64(), last_insert_id: 0L, }
}

///|
pub impl @db.AsyncDriver for Bridge with fn query(self, sql, params) {
  self.enter()
  defer {
    self.busy = false
  }
  errdefer (if @async.is_being_cancelled() { self.abort() })
  let p = parameters(params)
  try {
    let rows = match self.transaction {
      Some(tx) => tx.query(sql, params=p).collect()
      None => self.client.query(sql, params=p).collect()
    }
    rows.map(convert_row)
  } catch {
    e => raise map_error(e)
  }
}

///|
pub impl @db.AsyncDriver for Bridge with fn begin(self) {
  self.enter()
  defer {
    self.busy = false
  }
  errdefer (if @async.is_being_cancelled() { self.abort() })
  if self.transaction is Some(_) {
    raise @db.QueryError("transaction already active")
  }
  self.transaction = Some(
    self.client.transaction() catch {
      e => {
        self.abort()
        raise map_error(e)
      }
    },
  )
}

///|
pub impl @db.AsyncDriver for Bridge with fn commit(self) {
  self.enter()
  defer {
    self.busy = false
  }
  errdefer (if @async.is_being_cancelled() { self.abort() })
  guard self.transaction is Some(tx) else {
    raise @db.QueryError("no active transaction")
  }
  self.transaction = None
  tx.commit() catch {
    e => {
      self.abort()
      raise map_error(e)
    }
  }
}

///|
pub impl @db.AsyncDriver for Bridge with fn rollback(self) {
  self.enter()
  defer {
    self.busy = false
  }
  errdefer (if @async.is_being_cancelled() { self.abort() })
  guard self.transaction is Some(tx) else {
    raise @db.QueryError("no active transaction")
  }
  self.transaction = None
  tx.rollback() catch {
    e => {
      self.abort()
      raise map_error(e)
    }
  }
}

///|
pub impl @db.AsyncDriver for Bridge with fn close(self) {
  if !self.closed {
    if self.busy {
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
    savepoints: false,
    multiple_result_sets: false,
    unsigned_integers: false,
    returning: true,
  }
}
