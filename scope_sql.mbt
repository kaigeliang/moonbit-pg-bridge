///|
/// The client uses PostgreSQL extended queries, which accept only one SQL
/// statement. Inspect its leading tokens so transaction commands go through
/// the bridge state machine, even when preceded by comments or semicolons.
fn check_scoped_sql(sql : String) -> Unit raise @db.DbError {
  let bytes = @utf8.encode(sql)
  let (keyword, end) = sql_keyword(bytes, 0)
  let transaction_control = match keyword {
    "BEGIN"
    | "START"
    | "COMMIT"
    | "END"
    | "ROLLBACK"
    | "ABORT"
    | "SAVEPOINT"
    | "RELEASE" => true
    "PREPARE" => {
      let (next, _) = sql_keyword(bytes, end)
      next == "TRANSACTION"
    }
    _ => false
  }
  if transaction_control {
    raise @db.QueryError("transaction control SQL is managed by the scope")
  }
}

///|
/// moondb's AsyncTxn issues this command through the manual driver. A
/// successful rollback to a savepoint clears the server's failed state.
fn is_rollback_to_savepoint(sql : String) -> Bool {
  let bytes = @utf8.encode(sql)
  let (first, end) = sql_keyword(bytes, 0)
  guard first == "ROLLBACK" else { return false }
  let (next, end) = sql_keyword(bytes, end)
  match next {
    "TO" => true
    "WORK" | "TRANSACTION" => {
      let (word, _) = sql_keyword(bytes, end)
      word == "TO"
    }
    _ => false
  }
}

///|
fn sql_keyword(bytes : Bytes, from : Int32) -> (String, Int32) {
  let start = skip_sql_trivia(bytes, from)
  let end = sql_keyword_end(bytes, start)
  (@utf8.decode_lossy(bytes[start:end]).to_upper(), end)
}

///|
fn sql_keyword_end(bytes : Bytes, start : Int32) -> Int32 {
  let mut i = start
  while i < bytes.length() {
    match bytes[i] {
      b'a'..<=b'z'
      | b'A'..<=b'Z'
      | b'0'..<=b'9'
      | b'_'
      | b'$'
      | b'\x80'..<=b'\xff' => i += 1
      _ => break
    }
  }
  i
}

///|
fn skip_sql_trivia(bytes : Bytes, start : Int32) -> Int32 {
  let mut i = start
  while i < bytes.length() {
    match bytes[i:] {
      [b'-', b'-', ..] => {
        i += 2
        while i < bytes.length() && bytes[i] != b'\r' && bytes[i] != b'\n' {
          i += 1
        }
      }
      [b'/', b'*', ..] => i = skip_sql_block_comment(bytes, i + 2)
      [b' ' | b'\t' | b'\r' | b'\n' | b'\x0c' | b'\x0b' | b';', ..] => i += 1
      _ => break
    }
  }
  i
}

///|
fn skip_sql_block_comment(bytes : Bytes, start : Int32) -> Int32 {
  let mut depth = 1
  let mut i = start
  while i < bytes.length() && depth > 0 {
    match bytes[i:] {
      [b'/', b'*', ..] => {
        depth += 1
        i += 2
      }
      [b'*', b'/', ..] => {
        depth -= 1
        i += 2
      }
      _ => i += 1
    }
  }
  i
}
