const tap = require('tap');
const { cli } = require('./util');
const main = require('..');

tap.test('typescript', async t => {
  t.test('transform', async () => {
    const cmd = `etl ${__dirname}/support/typescript test --silent`;
    const res = await cli(cmd);
    t.same(res.data, [1, 2, 3, 4]);
  });

  t.test('chain', async () => {
    const cmd = `etl ${__dirname}/support/typescript --transform=${__dirname}/support/typescript-chain test --silent`;
    const res = await cli(cmd);
    t.same(res.data, [11, 12, 13, 14]);
  });

  t.test('export default stream', async () => {
    const cmd = `etl ${__dirname}/support/typescript-default test --silent`;
    const res = await cli(cmd);
    t.same(res.data, [1, 2, 3, 4]);
  });

  t.test('missing stream export resolves with 0 records', async () => {
    const res = await main({ _: [`${__dirname}/support/typescript-no-stream`, 'test'], silent: true });
    t.same(res['Σ_in'], 0);
    t.same(res['Σ_out'], 0);
  });
});