const tap = require('tap');
const { cli } = require('./util');
const etl = require('etl');
const mysql = require('mysql');
const waitPort= require('wait-port');

tap.before(async function() {
  await waitPort({ host: '127.0.0.1', port: 3306, interval: 10000 });
  const pool = mysql.createPool({
    host: '127.0.0.1',
    connectionLimit : 10,
    user: 'root',
    password: 'example'
  });

  let ready;

  const p = etl.mysql.execute(pool);

  while (!ready) {
    try {
      await p.query('select 1 as a');
      ready = true;
    } catch (e) {
      await new Promise(r => setTimeout(r, 1000));
    }
  }

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
    const res = await cli(cmd);
    const data = res.data.map(d => {
      delete d._id;
      return { ...d };
    }).sort((a, b) => (a.a > b.a) ? 1 : -1);

    const expected = require('./support/test_collect.json');
    t.same(data, expected);
  });
});