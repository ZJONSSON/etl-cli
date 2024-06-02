const tap = require('tap');
const cli = require('../index');
const etl = require('etl');
const mysql = require('mysql');
const { path } = require('./util');

tap.before(async function() {
  const pool = mysql.createPool({
    host: 'localhost',
    connectionLimit : 10,
    user: 'root',
    password: 'example'
  });

  const p = etl.mysql.execute(pool);
  await p.query('DROP DATABASE IF EXISTS `test_schema`');
  await p.query('CREATE DATABASE IF NOT EXISTS `test_schema`');
  await p.query('DROP TABLE IF EXISTS `test_schema`.`test`');
  await p.query(`
  CREATE TABLE \`test_schema\`.\`test\` (
    a int(11),
    b int(11),
    c int(11)
  )`);
  pool.end();
});

tap.test('mysql', async t => {
  t.test('writing data', async t => {
    // this is the same as:  etl ./support/test.csv mysql/test_schema/test --target_user=root --target_password=example
    const _ = [path('./support/test.csv'), 'mysql/test_schema/test'];
    const res = await cli({ _, target_password: 'example', target_user: 'root', upsert: true, test: true});
    t.same(res, true);
  });

  t.test('reading data', async t => {
    let res = await cli({ _: ['mysql/test_schema/test', 'test'], password: 'example', user: 'root', upsert: true, test: true});
    res = res.map(d => {
      delete d._id;
      return {...d};
    });
    const data = require('./support/test_collect.json');
    t.same(res, data);
  });

});