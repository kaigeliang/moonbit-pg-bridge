///|
async fn[T] with_transaction_bridge(f : async (@bridge.Bridge) -> T) -> T {
  @async.with_task_group(group => {
    let (client, executor) = @pg.Client::create(
      @pg.Config::Config(
        "127.0.0.1",
        user="bridge_test",
        database="postgres",
        port=@string.parse_int(
          @env.get_env_var("PG_BRIDGE_PORT").unwrap_or("55438"),
        ),
        ssl_mode=Disable,
        application_name="bridge-transaction-test",
      ),
    )
    group.spawn_bg(no_wait=true, () => executor.run())
    let bridge = @bridge.Bridge::new(client)
    let driver : &@db.AsyncDriver = bridge
    defer @async.protect_from_cancel(() => driver.close())
    client.ready()
    f(bridge)
  })
}

///|
async fn[T] expect_query_error(f : async () -> T) -> Unit {
  try {
    f() |> ignore
    fail("expected QueryError")
  } catch {
    @db.QueryError(_) => ()
    error => raise error
  }
}

///|
async test "postgres scoped transactions release and recover savepoints" {
  if @env.get_env_var("PG_BRIDGE_INTEGRATION") != Some("1") {
    return
  }
  with_transaction_bridge(bridge => {
    let driver : &@db.AsyncDriver = bridge
    assert_true(driver.capabilities().savepoints)
    driver.execute("CREATE TEMP TABLE scoped_items (id int PRIMARY KEY)", [])
    |> ignore
    let (returned, expired) = bridge.with_transaction(root => {
      root.execute("INSERT INTO scoped_items VALUES (1)", []) |> ignore
      expect_query_error(() => driver.query("SELECT 1", []))
      expect_query_error(() => driver.commit())
      expect_query_error(() => bridge.with_transaction(_ => ()))
      expect_query_error(() => {
        root.execute(" ; /* outer /* nested */ end */ --x\n CoMmIt", [])
      })
      expect_query_error(() => {
        root.query("PREPARE /* x */ TRANSACTION 'prepared'", [])
      })
      expect_query_error(() => root.with_savepoint("", _ => ()))
      expect_query_error(() => root.with_savepoint("bad\u0000name", _ => ()))
      expect_query_error(() => {
        root.with_savepoint(
          "数据库数据库数据库数据库数据库数据库数据库数据库",
          _ => (),
        )
      })
      let original = try {
        root.with_savepoint("child \"quote\"", child => {
          expect_query_error(() => root.query("SELECT 1", []))
          child.execute("INSERT INTO scoped_items VALUES (2)", []) |> ignore
          child.with_savepoint("child \"quote\"", leaf => {
            leaf.execute("INSERT INTO scoped_items VALUES (3)", []) |> ignore
          })
          raise @db.QueryError("callback rejected work")
        })
        ""
      } catch {
        @db.QueryError(message) => message
        error => raise error
      }
      assert_eq(original, "callback rejected work")
      let sqlstate = try {
        root.with_savepoint("duplicate", child => {
          child.execute("INSERT INTO scoped_items VALUES (4), (1)", [])
          |> ignore
        })
        None
      } catch {
        @db.Server(error) => error.sqlstate
        error => raise error
      }
      assert_eq(sqlstate, Some("23505"))
      expect_query_error(() => {
        root.with_savepoint("caught_error", child => {
          child.execute("INSERT INTO scoped_items VALUES (5)", []) |> ignore
          try child.query("SELECT 1 / 0", []) |> ignore catch {
            @db.Server(_) => ()
            error => raise error
          }
          expect_query_error(() => child.with_savepoint("too_late", _ => ()))
        })
      })
      let child = root.with_savepoint("kept", child => {
        child.execute("INSERT INTO scoped_items VALUES ($1)", [Int(6)])
        |> ignore
        child
      })
      expect_query_error(() => child.query("SELECT 1", []))
      let rows = root.query("SELECT id FROM scoped_items ORDER BY id", [])
      (rows.map(row => row.values), root)
    })
    assert_eq(returned, [[Int(1)], [Int(6)]])
    expect_query_error(() => expired.query("SELECT 1", []))
    assert_eq(
      driver.query("SELECT count(*)::int8 FROM scoped_items", [])[0].values,
      [Int64(2L)],
    )
    expect_query_error(() => {
      bridge.with_transaction(root => {
        root.execute("INSERT INTO scoped_items VALUES (7)", []) |> ignore
        try root.execute("SELECT 1 / 0", []) |> ignore catch {
          @db.Server(_) => ()
          error => raise error
        }
      })
    })
    assert_eq(
      driver
      .query("SELECT id FROM scoped_items ORDER BY id", [])
      .map(row => row.values),
      [[Int(1)], [Int(6)]],
    )
    let settings = bridge.with_transaction(
      root => {
        root.query(
          "SELECT current_setting('transaction_isolation'), current_setting('transaction_read_only')",
          [],
        )[0].values
      },
      options=@pg.TransactionOptions(
        isolation_level=RepeatableRead,
        read_only=true,
      ),
    )
    assert_eq(settings, [Text("repeatable read"), Text("on")])
    expect_query_error(() => bridge.with_savepoint("missing", _ => ()))
    driver.begin()
    bridge.with_savepoint("manual", child => {
      child.execute("INSERT INTO scoped_items VALUES (8)", []) |> ignore
    })
    driver.rollback()
    assert_eq(
      driver.query("SELECT count(*)::int8 FROM scoped_items", [])[0].values,
      [Int64(2L)],
    )
    let tx = @db.AsyncTxn::new(bridge)
    tx.run(outer => {
      let outer_driver : &@db.AsyncDriver = outer.driver()
      outer_driver.execute("INSERT INTO scoped_items VALUES (10)", []) |> ignore
      outer.run(inner => {
        let inner_driver : &@db.AsyncDriver = inner.driver()
        inner_driver.execute("INSERT INTO scoped_items VALUES (11)", [])
        |> ignore
        inner_driver.query("SELECT 1 / 0", []) |> ignore
      }) catch {
        @db.Server(error) if error.sqlstate == Some("22012") => ()
        error => raise error
      }
      outer_driver.execute("INSERT INTO scoped_items VALUES (12)", []) |> ignore
    })
    assert_eq(
      driver
      .query("SELECT id FROM scoped_items ORDER BY id", [])
      .map(row => row.values),
      [[Int(1)], [Int(6)], [Int(10)], [Int(12)]],
    )
  })
}

///|
async test "postgres scoped transactions handle cancellation and escaped work" {
  if @env.get_env_var("PG_BRIDGE_INTEGRATION") != Some("1") {
    return
  }
  with_transaction_bridge(bridge => {
    let driver : &@db.AsyncDriver = bridge
    driver.execute("CREATE TEMP TABLE cancellable_items (id int)", []) |> ignore
    @async.with_task_group(group => {
      let ready : @async.Queue[Unit] = @async.Queue(kind=Unbounded)
      let parked : @async.Queue[Unit] = @async.Queue(kind=Unbounded)
      let running = group.spawn(allow_failure=true, () => {
        bridge.with_transaction(root => {
          root.execute("INSERT INTO cancellable_items VALUES (1)", []) |> ignore
          root.with_savepoint("cancellable", child => {
            child.execute("INSERT INTO cancellable_items VALUES (2)", [])
            |> ignore
            ready.put(())
            parked.get()
          })
        })
      })
      ready.get()
      expect_query_error(() => driver.query("SELECT 1", []))
      running.cancel()
      let cancelled = try {
        running.wait()
        false
      } catch {
        @async.WaitedTaskAlreadyCancelled => true
        error => raise error
      }
      assert_true(cancelled)
    })
    assert_eq(
      driver.query("SELECT count(*)::int8 FROM cancellable_items", [])[0].values,
      [Int64(0L)],
    )
    @async.with_task_group(group => {
      let started : @async.Queue[Unit] = @async.Queue(kind=Unbounded)
      let parked : @async.Queue[Unit] = @async.Queue(kind=Unbounded)
      expect_query_error(() => {
        bridge.with_transaction(root => {
          group.spawn_bg(no_wait=true, allow_failure=true, () => {
            root.with_savepoint("escaped", child => {
              child.execute("INSERT INTO cancellable_items VALUES (3)", [])
              |> ignore
              started.put(())
              parked.get()
            })
          })
          started.get()
        })
      })
      let closed = try {
        driver.query("SELECT 1", []) |> ignore
        false
      } catch {
        @db.Closed => true
        error => raise error
      }
      assert_true(closed)
    })
  })
}

///|
async test "postgres scoped cancellation during SQL discards the connection" {
  if @env.get_env_var("PG_BRIDGE_INTEGRATION") != Some("1") {
    return
  }
  with_transaction_bridge(bridge => {
    let driver : &@db.AsyncDriver = bridge
    @async.with_task_group(group => {
      let active : @async.Queue[@bridge.TransactionScope] = @async.Queue(
        kind=Unbounded,
      )
      let running = group.spawn(allow_failure=true, () => {
        bridge.with_transaction(root => {
          root.with_savepoint("running_query", child => {
            active.put(child)
            child.query("SELECT 1::int4 FROM pg_sleep(0.2)", []) |> ignore
          })
        })
      })
      let child = active.get()
      let busy = try {
        child.query("SELECT 1", []) |> ignore
        false
      } catch {
        @db.QueryError(message) => message == "bridge is busy"
        error => raise error
      }
      assert_true(busy)
      running.cancel()
      let cancelled = try {
        running.wait()
        false
      } catch {
        @async.WaitedTaskAlreadyCancelled => true
        error => raise error
      }
      assert_true(cancelled)
      let closed = try {
        driver.query("SELECT 1", []) |> ignore
        false
      } catch {
        @db.Closed => true
        error => raise error
      }
      assert_true(closed)
    })
  })
}
