///|
test "unsupported parameter is a type error" {
  let result = try {
    parameters([@db.Numeric("1.25")]) |> ignore
    false
  } catch {
    e => e is @db.TypeError(_)
  }
  assert_true(result)
}

///|
test "supported parameters include a generic null" {
  let p = parameters([
    Null,
    Bool(true),
    Int(-1),
    Int64(2L),
    Double(0.5),
    Text("x"),
    Blob(b"x"),
  ])
  assert_eq(p.length(), 7)
}
