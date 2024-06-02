const tap = require('tap');
const etl = require('../index');
const { path } = require('./util');

tap.test('typescript', async t => {
  t.test('transform', async () => {
    const res = await etl({ _: [path('./support/typescript')], target_type: 'test', silent: 'true'});
    t.same(res, [1, 2, 3, 4]);
  });

  t.test('chain', async () => {
    const res = await etl({ _: [path('./support/typescript')], transform: path('./support/typescript-chain'), target_type: 'test', silent: 'true'});
    t.same(res, [11, 12, 13, 14]);
  });

});