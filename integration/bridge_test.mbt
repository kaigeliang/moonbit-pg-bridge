///|
async test "postgres parameter binding and rollback" {
  if @env.get_env_var("PG_BRIDGE_INTEGRATION") != Some("1") {
    return
  }
  @async.with_task_group(group => {
    let cfg = @pg.Config::Config(
      "127.0.0.1",
      user="bridge_test",
      database="postgres",
      port=@string.parse_int(
        @env.get_env_var("PG_BRIDGE_PORT").unwrap_or("55438"),
      ),
      ssl_mode=Disable,
      application_name="bridge-test",
    )
    let (client, executor) = @pg.Client::create(cfg)
    let task = group.spawn(no_wait=true, () => executor.run())
    client.ready()
    let bridge = @bridge.Bridge::new(client)
    let driver : &@db.AsyncDriver = bridge
    driver.execute("CREATE TABLE bridge_test (id INT4, name TEXT)", [])
    |> ignore
    driver.begin()
    driver.execute("INSERT INTO bridge_test VALUES ($1, $2)", [
      Int(7),
      Text("hello"),
    ])
    |> ignore
    let rows = driver.query("SELECT id, name FROM bridge_test", [])
    assert_eq(rows.length(), 1)
    assert_eq(rows[0].values, [Int(7), Text("hello")])
    let typed = driver.query(
      "SELECT $1::bool AS b, $2::int2 AS s, $3::int8 AS l, $4::float8 AS f, $5::bytea AS bytes, $6::varchar AS v",
      [
        Bool(true),
        Int(-123),
        Int64(2147483648L),
        Double(3.5),
        Blob(b"a\x00b"),
        Text("世界"),
      ],
    )
    assert_eq(typed[0].values, [
      Bool(true),
      Int(-123),
      Int64(2147483648L),
      Double(3.5),
      Blob(b"a\x00b"),
      Text("世界"),
    ])
    let floats = driver.query(
      "SELECT 1.5::float4 AS f, '-2147483648'::int4 AS lo, 2147483647::int4 AS hi, NULL::bytea AS empty",
      [],
    )
    assert_eq(floats[0].values, [
      Double(1.5),
      Int(-2147483648),
      Int(2147483647),
      Null,
    ])
    let json = driver.query("SELECT $1::json AS j, $2::jsonb AS b", [
      Json("{\"a\":1}"),
      Json("[true,null]"),
    ])
    assert_eq(json[0].values, [Json("{\"a\":1}"), Json("[true,null]")])
    let bad_json = try {
      driver.query("SELECT $1::json", [Json("{")]) |> ignore
      false
    } catch {
      e => e is @db.TypeError(_)
    }
    assert_true(bad_json)
    let nulls = driver.query("SELECT $1::int4 AS n", [Null])
    assert_eq(nulls[0].values, [Null])
    driver.rollback()
    assert_eq(driver.query("SELECT id FROM bridge_test", []).length(), 0)
    driver.begin()
    driver.execute("INSERT INTO bridge_test VALUES ($1, $2)", [
      Int(9),
      Text("kept"),
    ])
    |> ignore
    driver.commit()
    assert_eq(driver.query("SELECT id FROM bridge_test", [])[0].values, [Int(9)])
    let diagnostic = try {
      driver.query("SELECT NULL::numeric AS price", []) |> ignore
      ""
    } catch {
      @db.TypeError(message) => message
      _ => "wrong error"
    }
    assert_true(diagnostic.contains("price"))
    assert_true(diagnostic.contains("numeric"))
    // A PostgreSQL error leaves the transaction aborted; rollback restores it.
    driver.begin()
    let sqlstate = try {
      driver.execute("SELECT 1 / 0", []) |> ignore
      None
    } catch {
      e => e.sqlstate()
    }
    assert_eq(sqlstate, Some("22012"))
    driver.rollback()
    assert_eq(driver.query("SELECT 42::int4", [])[0].values, [Int(42)])
    // A concurrently active operation must be rejected, not interleaved.
    let running = group.spawn(no_wait=true, () => {
      driver.query("SELECT 1::int4 FROM pg_sleep(0.2)", [])
    })
    @async.sleep(40)
    let busy = try {
      driver.query("SELECT 1::int4", []) |> ignore
      false
    } catch {
      _ => true
    }
    assert_true(busy)
    ignore(running.wait() catch { _ => [] })
    // Cancel while inside a transaction; the uncertain connection is discarded.
    driver.begin()
    driver.execute("INSERT INTO bridge_test VALUES (11, 'cancelled')", [])
    |> ignore
    let cancelled = group.spawn(
      no_wait=true,
      () => driver.query("SELECT 1::int4 FROM pg_sleep(0.2)", []),
      allow_failure=true,
    )
    @async.sleep(40)
    cancelled.cancel()
    ignore(cancelled.wait() catch { _ => [] })
    let closed = try {
      driver.query("SELECT 1::int4", []) |> ignore
      false
    } catch {
      e => e is @db.Closed
    }
    assert_true(closed)
    driver.close()
    task.wait() catch {
      _ => ()
    }
    // A fresh connection verifies cancellation did not commit the open transaction.
    let (second, exec2) = @pg.Client::create(cfg)
    let task2 = group.spawn(no_wait=true, () => exec2.run())
    second.ready()
    let check : Int64 = second
      .query_one("SELECT count(*)::int8 FROM bridge_test WHERE id = 11")
      .get(0)
    assert_eq(check, 0L)
    second.close()
    task2.wait()
  })
}
