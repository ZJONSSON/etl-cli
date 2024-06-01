const tap = require('tap');
const cli = require('../index');
const etl = require('etl');
const { path, requireAll } = require('./util');
const { readFileSync } = require('fs');
const tmpDir = require('os').tmpdir()+'/';
console.log(tmpDir)


requireAll(path('../targets'));


tap.test('outputs', async t => {
  t.test('json', async t => {
    const res = await cli({ _: [path('./support/test.csv'), tmpDir+'test.json'], test: true});
    let data = readFileSync(tmpDir + 'test.json', 'utf8');
    t.same(data, '{"a":"1","b":"2","c":"3"}\n{"a":"4","b":"5","c":"6"}\n');
  });

  t.test('json with json_collect', async t => {
    const res = await cli({ _: [path('./support/test.csv'), tmpDir+'test_collect.json'], json_collect: true, test: true});
    const data = require(tmpDir+'test_collect.json')
    t.same(data, require('./support/test_collect.json'))

  })

  t.test('csv', async t => {
    console.log('CSV');
    const res = await cli({ _: [path('./support/test.csv'), tmpDir+'test.csv'], test: true})
    const data = readFileSync(tmpDir + 'test.csv', 'utf8')
    t.same(data, 'a,b,c\n1,2,3\n4,5,6\n');
  });

  t.test('files', async t => {
    //const res = await cli({ _:[], source_type: 'files', source_dir: path('support/testfiles'), target_type: 'files', target_dir: tmpDir, test: true})
   
  })

});