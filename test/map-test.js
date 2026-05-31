const tap = require('tap');
const { cli } = require('./util');

tap.test('inline chain', async t => {
  t.test('chain', async () => {
    const cmd = `etl ${__dirname}/support/typescript --chain="d => d.map(d => 10+d)" test --silent`;
    const res = await cli(cmd);
    t.same(res.data, [11, 12, 13, 14]);
  });
});

tap.test('async generator chain', async t => {
  t.test('--chain as async generator receives incoming stream', async t => {
    const cmd = `etl ${__dirname}/support/test.csv test --silent --chain=${__dirname}/support/chain-iterator --n=42`;
    const res = await cli(cmd);
    t.same(res.data, [
      { a: '1', b: '2', c: '3', chained: true, n: '42' },
      { a: '4', b: '5', c: '6', chained: true, n: '42' },
    ]);
    t.same(res.Σ_out, 2);
  });

  t.test('transform.chain as async generator receives incoming stream', async t => {
    const cmd = `etl ${__dirname}/support/test.csv test --silent --transform=${__dirname}/support/chain-iterator --n=7`;
    const res = await cli(cmd);
    t.same(res.data, [
      { a: '1', b: '2', c: '3', chained: true, n: '7' },
      { a: '4', b: '5', c: '6', chained: true, n: '7' },
    ]);
    t.same(res.Σ_out, 2);
  });
});
