const tap = require('tap');
const { cli } = require('./util');

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

});