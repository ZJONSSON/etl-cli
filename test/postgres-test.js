const tap = require('tap');
const { cli } = require('./util');
const etl = require('etl');
const pg = require('pg');
const waitPort = require('wait-port');


tap.before(async () => {
  await waitPort({ host: '127.0.0.1', port: 5432 });
  const pool = new pg.Pool({
    host: '127.0.0.1',
    port: 5432,
    database: 'postgres',
    user: 'postgres',
    password: 'example'
  });
  const p = etl.postgres.execute(pool);

  await p.query('create schema if not exists test_schema');
  await p.query('drop table if exists test_schema.test');
  await p.query(`
  create table test_schema.test (
    a integer,
    b integer,
    c integer
  )
  `);
  await pool.end();
});

tap.test('postgres', async t => {

  t.test('writing data', async t => {
    const cmd = `etl ${__dirname}/support/test.csv postgres/test_schema/test --password=example --user=postgres --upsert`;
    const res = await cli(cmd);
    t.same(res, { 'Σ_in': 2, 'Σ_out': 2 });
  });

  t.test('reading data', async t => {
    const cmd = 'etl postgres/test_schema/test test --password=example --user=postgres --upsert=true';
    const res = await cli(cmd);
    const data = res.data.sort((a, b) => (a.a > b.a) ? 1 : -1);
    const expected = require('./support/test_collect.json');
    t.same(data, expected);
  });

  t.test('querying data', async t => {
    const cmd = 'etl postgres test --password=example --user=postgres --source_query="select * from test_schema.test order by a"';
    const res = await cli(cmd);
    const data = res.data.sort((a, b) => (a.a > b.a) ? 1 : -1);
    const expected = require('./support/test_collect.json');
    t.same(data, expected);
  });

});