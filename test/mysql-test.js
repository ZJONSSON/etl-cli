const tap = require('tap');
const { cli } = require('./util');
const etl = require('etl');
const mysql = require('mysql');


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
    const cmd = `etl ${__dirname}/support/test.csv mysql/test_schema/test --user=root --password=example --upsert=true`;
    const res = await cli(cmd);
    t.same(res, { 'Σ_in': 2, 'Σ_out': 2 });
  });

  t.test('reading data', async t => {
    const cmd = `etl ${__dirname}/support/test.csv test --user=root --password=example`;
    let res = await cli(cmd);
    res = res.data.map(d => {
      delete d._id;
      return { ...d };
    });
    const data = require('./support/test_collect.json');
    t.same(res, data);
  });
});