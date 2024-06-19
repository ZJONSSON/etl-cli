const tap = require('tap');
const { cli } = require('./util');
const { path, requireAll } = require('./util');
const { readFileSync } = require('fs');
const tmpDir = require('os').tmpdir() + '/';


requireAll(path('../targets'));


tap.test('outputs', async t => {

  t.test('json', async t => {
    const cmd = `etl ${__dirname}/support/test.csv ${tmpDir}test.json`;
    await cli(cmd);
    const data = readFileSync(tmpDir + 'test.json', 'utf8');
    t.same(data, '{"a":"1","b":"2","c":"3"}\n{"a":"4","b":"5","c":"6"}\n');
  });

  t.test('json with json_collect', async t => {
    const cmd = `etl ${__dirname}/support/test.csv ${tmpDir}test_collect.json --json_collect`;
    await cli(cmd);
    const data = require(tmpDir + 'test_collect.json');
    t.same(data, require('./support/test_collect.json'));

  });

  t.test('csv', async t => {
    const cmd = `etl ${__dirname}/support/test.csv ${tmpDir}test.csv`;
    await cli(cmd);
    const data = readFileSync(tmpDir + 'test.csv', 'utf8');
    t.same(data, 'a,b,c\n1,2,3\n4,5,6\n');
  });

});