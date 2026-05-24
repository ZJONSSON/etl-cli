const tap = require('tap');
const { cli } = require('./util');

tap.test('iterate transform', async t => {
  t.test('fans out each record into N records via async generator', async () => {
    const cmd = `etl ${__dirname}/support/test.csv test --silent --transform=${__dirname}/support/iterate`;
    const res = await cli(cmd);
    // test.csv has rows {a:1,...} and {a:4,...} so 1 + 4 = 5 records out
    t.same(res.data, [
      { i: 0, parent: '1' },
      { i: 0, parent: '4' },
      { i: 1, parent: '4' },
      { i: 2, parent: '4' },
      { i: 3, parent: '4' }
    ]);
    t.same(res.Σ_in, 2);
    t.same(res.Σ_out, 5);
  });

  t.test('argv is passed through to iterate', async t => {
    const cmd = `etl ${__dirname}/support/test.csv test --silent --transform=${__dirname}/support/iterate --n=2`;
    const res = await cli(cmd);
    t.same(res.Σ_out, 4);
    t.same(res.data.map(d => d.i), [0, 1, 0, 1]);
  });

  t.test('finalize runs after a function transform', async t => {
    const cmd = `etl ${__dirname}/support/test.csv test --silent --transform=${__dirname}/support/finalize`;
    const res = await cli(cmd);
    t.same(res.data, [
      { a: '1', b: '2', c: '3' },
      { a: '4', b: '5', c: '6' },
      { finalized: 2 },
      { returned: 2 }
    ]);
    t.same(res.Σ_out, 4);
  });

  t.test('finalize can be an async generator after an iterator transform', async t => {
    const cmd = `etl ${__dirname}/support/test.csv test --silent --transform=${__dirname}/support/iterateFinalize`;
    const res = await cli(cmd);
    t.same(res.data, [
      { parent: '1' },
      { parent: '4' },
      { finalized: 2 },
      { done: true }
    ]);
    t.same(res.Σ_out, 4);
  });
});
