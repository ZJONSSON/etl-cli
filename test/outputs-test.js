const tap = require('tap');
const etl = require('etl');
const output = require('../output');
const { cli } = require('./util');
const { path, requireAll } = require('./util');
const { join } = require('path');
const { readFileSync } = require('fs');
const { randomBytes } = require('crypto');
const fs = require('fs-extra');
const tmpDir = join(require('os').tmpdir(), randomBytes(8).toString('base64'));

requireAll(path('../targets'));

tap.before(async () => fs.ensureDir(tmpDir));

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

  t.test('csv omits function columns', async t => {
    const csvFile = join(tmpDir, 'functions.csv');
    await output({
      stream: etl.toStream([{
        a: '1',
        body: () => 'skip',
        getClient: () => 'skip',
        nested: {
          b: '2',
          fn: () => 'skip'
        }
      }])
    }, {
      target: csvFile,
      silent: true,
      throw: true
    });

    const data = readFileSync(csvFile, 'utf8');
    t.same(data, 'a,nested>b\n1,2\n');
  });

  t.test('files', async t => {
    let data;
    const cmd = `etl test/support/generateFiles files --target_dir ${tmpDir}`;
    await cli(cmd);
    data = readFileSync(join(tmpDir, '/test1.json'), 'utf8');
    t.same(data, 'This is file test1.json');
    data = readFileSync(join(tmpDir, 'a/b/c/test2.json'), 'utf8');
    t.same(data, 'This is file test2.json');
    data = readFileSync(join(tmpDir, 'a/b/c/test3.json'), 'utf8');
    t.same(data, 'This is file test3.json');
  });

});
