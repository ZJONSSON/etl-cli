const tap = require('tap');
const { cli } = require('./util');

tap.test('modules', async t => {
  t.test('broken js', async () => {
    const cmd = `etl ${__dirname}/support/broken_js test --silent`;
    t.rejects( cli(cmd), { message:'Broken JS file' });
  });

  t.test('broken ts', async () => {
    const cmd = `etl ${__dirname}/support/broken_ts test --silent`;
    t.rejects( cli(cmd).catch((e) => {
      throw { message: e.message.split('\n')[0] };
    }), { message:'Transform failed with 1 error:' });
  });
});