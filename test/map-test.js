const tap = require('tap');
const { cli } = require('./util');

tap.test('inline chain', async t => {
  t.test('chain', async () => {
    const cmd = `etl ${__dirname}/support/typescript --chain="d => d.map(d => 10+d)" test --silent`;
    const res = await cli(cmd);
    t.same(res.data, [11, 12, 13, 14]);
  });
});
