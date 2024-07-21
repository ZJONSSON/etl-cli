const tap = require('tap');
const { cli } = require('./util');

tap.test('selectors', async t => {
  t.test('filter', async t => {
    const cmd = `etl ${__dirname}/support/test.csv test --silent --filter="d => d.a == '4'"`;
    const res = await cli(cmd);
    t.same(res.data, [
      { a: '4', b: '5', c : '6', __line: 3 }
    ]);
  });
});

