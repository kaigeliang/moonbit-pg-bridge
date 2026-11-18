///|
priv struct TransactionState {
  handle : @pg.Transaction
  kind : TransactionKind
  mut failed : Bool
  mut stream_error : @pg.DatabaseError?
}

///|
priv enum TransactionKind {
  Root
  Savepoint
}

///|
/// A transaction handle valid while its callback is running and no child
/// savepoint owns the connection.
pub struct TransactionScope {
  priv bridge : Bridge
  priv state : TransactionState
}

///|
fn operation_error(tx : TransactionState?, error : Error) -> @db.DbError {
  if tx is Some(state) && error is @pg.ClientError::Database(_) {
    state.failed = true
  }
  map_error(error)
}

///|
async fn Bridge::start_transaction(
  self : Bridge,
  options : @pg.TransactionOptions,
) -> TransactionState raise @db.DbError {
  self.enter()
  defer {
    self.busy = false
  }
  if self.transaction is Some(_) {
    raise @db.QueryError("transaction already active")
  }
  errdefer (if @async.is_being_cancelled() { self.abort() })
  let handle = self.client.transaction(options~) catch {
    error => {
      self.abort()
      raise map_error(error)
    }
  }
  { handle, kind: Root, failed: false, stream_error: None, }
}

///|
async fn Bridge::finish_manual_transaction(
  self : Bridge,
  commit~ : Bool,
) -> Unit raise @db.DbError {
  self.enter()
  defer {
    self.busy = false
  }
  guard self.transaction is Some(state) else {
    raise @db.QueryError("no active transaction")
  }
  self.transaction = None
  self.finish_transaction(state, commit~)
}

///|
/// Finish while holding the bridge operation slot. An error from the
/// completion command invalidates the connection; a successful rollback of
/// failed work leaves the surrounding transaction or connection usable.
async fn Bridge::finish_transaction(
  self : Bridge,
  state : TransactionState,
  commit~ : Bool,
) -> Unit raise @db.DbError {
  let rollback = !commit || state.failed
  try {
    errdefer (if @async.is_being_cancelled() { self.abort() })
    if !rollback {
      state.handle.commit()
    } else {
      state.handle.rollback()
    }
  } catch {
    // postgres 0.1.3 rethrows the same stream error after a successful
    // rollback. A new error from the rollback itself has a different payload.
    @pg.ClientError::Database(error) if rollback &&
      state.stream_error is Some(seen) &&
      physical_equal(error, seen) => ()
    error => {
      self.abort()
      raise map_error(error)
    }
  }
  if commit && state.failed {
    let message = match state.kind {
      Root => "transaction was rolled back after a PostgreSQL error"
      Savepoint => "savepoint was rolled back after a PostgreSQL error"
    }
    raise @db.QueryError(message)
  }
}

///|
/// Run a callback in a new transaction. The bridge commits on success and
/// rolls back on callback failure. Use the supplied handle for all SQL in
/// the transaction. The callback's return value or error is propagated.
pub async fn[T] Bridge::with_transaction(
  self : Bridge,
  f : async (TransactionScope) -> T,
  options~ : @pg.TransactionOptions = @pg.TransactionOptions(),
) -> T {
  let state = self.start_transaction(options)
  self.run_scope(state, f)
}

///|
/// Run a savepoint callback within a transaction opened through
/// `AsyncDriver::begin`. Finishing the savepoint leaves that transaction open.
pub async fn[T] Bridge::with_savepoint(
  self : Bridge,
  name : String,
  f : async (TransactionScope) -> T,
) -> T {
  self.check_open()
  guard self.transaction is Some(parent) else {
    raise @db.QueryError("no explicit transaction for savepoint")
  }
  let state = self.start_savepoint(parent, name)
  self.run_scope(state, f)
}

///|
/// Run a nested callback under a PostgreSQL savepoint. The parent handle is
/// suspended until the callback finishes and the savepoint is resolved.
pub async fn[T] TransactionScope::with_savepoint(
  self : TransactionScope,
  name : String,
  f : async (TransactionScope) -> T,
) -> T {
  let state = self.bridge.start_savepoint(self.state, name, scope=self)
  self.bridge.run_scope(state, f)
}

///|
async fn Bridge::start_savepoint(
  self : Bridge,
  parent : TransactionState,
  name : String,
  scope? : TransactionScope,
) -> TransactionState raise @db.DbError {
  self.enter(scope?)
  defer {
    self.busy = false
  }
  if parent.failed {
    raise @db.QueryError("cannot open a savepoint in a failed transaction")
  }
  let bytes = @utf8.encode(name)
  if bytes.is_empty() || bytes.length() > 63 || name.contains_char('\u0000') {
    raise @db.QueryError(
      "savepoint name must be 1–63 UTF-8 bytes without NUL",
    )
  }
  errdefer (if @async.is_being_cancelled() { self.abort() })
  let handle = parent.handle.savepoint(name) catch {
    @pg.ClientError::Database(_) as error =>
      raise operation_error(Some(parent), error)
    error => {
      self.abort()
      raise map_error(error)
    }
  }
  { handle, kind: Savepoint, failed: false, stream_error: None, }
}

///|
async fn[T] Bridge::run_scope(
  self : Bridge,
  state : TransactionState,
  f : async (TransactionScope) -> T,
) -> T {
  let parent = self.scope
  let scope : TransactionScope = { bridge: self, state, }
  self.scope = Some(scope)
  defer (if !self.closed { self.scope = parent })
  let result = {
    errdefer (@async.protect_from_cancel(() => scope.finish(commit=false)) catch {
      _ => self.abort()
    })
    let value = f(scope)
    // A callback can catch cancellation or return without another yield.
    // Observe pending cancellation before the completion command is sent.
    if @async.is_being_cancelled() {
      @async.pause()
    }
    value
  }
  scope.finish(commit=true)
  result
}

///|
async fn TransactionScope::finish(
  self : TransactionScope,
  commit~ : Bool,
) -> Unit raise @db.DbError {
  let bridge = self.bridge
  // Escaped work can still hold the operation slot or own a nested
  // savepoint. Discard the connection instead of committing around it.
  {
    errdefer bridge.abort()
    bridge.enter(scope=self)
  }
  defer {
    bridge.busy = false
  }
  bridge.finish_transaction(self.state, commit~)
}

///|
/// Execute a statement using this scope's connection and bound parameters.
pub async fn TransactionScope::execute(
  self : TransactionScope,
  sql : String,
  params : Array[@db.Value],
) -> @db.ExecResult raise @db.DbError {
  let bridge = self.bridge
  bridge.enter(scope=self)
  defer {
    bridge.busy = false
  }
  check_scoped_sql(sql)
  bridge.execute_on(Some(self.state), sql, params)
}

///|
/// Read and materialize all rows using this scope's connection.
pub async fn TransactionScope::query(
  self : TransactionScope,
  sql : String,
  params : Array[@db.Value],
) -> Array[@db.Row] raise @db.DbError {
  let bridge = self.bridge
  bridge.enter(scope=self)
  defer {
    bridge.busy = false
  }
  check_scoped_sql(sql)
  bridge.query_on(Some(self.state), sql, params)
}
