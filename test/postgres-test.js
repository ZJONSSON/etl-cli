const tap = require('tap');
const { cli } = require('./util');
const etl = require('etl');
const tmpDir = require('os').tmpdir()+'/';
console.log(tmpDir);
const pg = require('pg');


tap.before(async () => {
  const pool = new pg.Pool({
    host: 'localhost',
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
    const data = require('./support/test_collect.json');
    t.same(res.data, data);
  });

  t.test('querying data', async t => {
    const cmd = 'etl postgres test --password=example --user=postgres --source_query="select * from test_schema.test order by a"';
    const res = await cli(cmd);
    const data = require('./support/test_collect.json');
    t.same(res.data, data);
  });

});