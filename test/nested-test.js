const tap = require('tap');
const { cli } = require('./util');

const nested = require('./support/nested_collect.json');

tap.test('nested data', async t => {
  t.test('nested json', async t => {
    const cmd = `etl ${__dirname}/support/nested.json test`;
    const res = await cli(cmd);
    t.same(res, { 'Σ_in': 2, 'Σ_out': 2, data: nested });
  });

  t.test('nested json', async t => {
    const cmd = `etl ${__dirname}/support/nested.csv test`;
    const res = await cli(cmd);
    t.same(res, { 'Σ_in': 2, 'Σ_out': 2, data: nested });
  });

  t.test('nested parquet', async t => {
    const cmd = `etl ${__dirname}/support/nested.parquet test`;
    const res = await cli(cmd);
    t.same(res, { 'Σ_in': 2, 'Σ_out': 2, data: nested });
  });
});
