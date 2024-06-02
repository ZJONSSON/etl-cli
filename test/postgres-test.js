const tap = require('tap');
const cli = require('../index');
const etl = require('etl');
const { path } = require('./util');
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
    const _ = [path('./support/test.csv'), 'postgres/test_schema/test'];
    const res = await cli({ _, password: 'example', user: 'postgres', upsert: true, test: true});
    t.same(res, true);
  });

  t.test('reading data', async t => {
    const res = await cli({ _: ['postgres/test_schema/test', 'test'], password: 'example', user: 'postgres', upsert: true, test: true});
    const data = require('./support/test_collect.json');
    t.same(res, data);
  });


});