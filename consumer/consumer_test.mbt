///|
async test "consumer uses the public driver interface" {
  @async.with_task_group(group => {
    let config = @pg.Config::Config("127.0.0.1", user="bridge_test", database="postgres", port=55438, ssl_mode=Disable, application_name="independent-consumer")
    let (client, executor) = @pg.Client::create(config)
    let runner = group.spawn(no_wait=true, () => executor.run())
    client.ready()
    let b = @bridge.Bridge::new(client)
    let d : &@db.AsyncDriver = b
    assert_eq(d.dialect(), @db.Dialect::Postgres)
    assert_false(d.capabilities().server_cursors)
    assert_eq(d.query("SELECT $1::int4 AS n", [Int(73)])[0].values, [Int(73)])
    let txn = @db.AsyncTxn::new(b)
    txn.run(t => {
      let tx_driver : &@db.AsyncDriver = t.driver()
      let rows = tx_driver.query("SELECT 9::int4", [])
      if rows.length() != 1 || rows[0].values != [Int(9)] { raise @db.QueryError("unexpected result") }
    })
    d.close()
    runner.wait()
  })
}
