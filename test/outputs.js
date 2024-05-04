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
    t.same(data, '{"a":"1","b":"2","c":"3","__line":2}\n{"a":"4","b":"5","c":"6","__line":3}\n');
  });

  t.test('csv', async t => {
    console.log('CSV');
    const res = await cli({ _: [path('./support/test.csv'), tmpDir+'test.csv'], test: true})
    const data = readFileSync(tmpDir + 'test.csv', 'utf8')
    t.same(data, 'a,b,c,__line\n1,2,3,2\n4,5,6,3\n');
  });

  t.test('files', async t => {
    //const res = await cli({ _:[], source_type: 'files', source_dir: path('support/testfiles'), target_type: 'files', target_dir: tmpDir, test: true})
   
  })

});