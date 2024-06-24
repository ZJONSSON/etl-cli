const tap = require('tap');
const { cli } = require('./util');
const etl = require('etl');

tap.test('files', async t => {

  t.test('files body', async () => {
    const cmd = `etl files/${__dirname}/support/testfiles test --silent`;
    const res = await cli(cmd);
    const data = res.data;

    function getBuffer(body) {
      return new Promise((resolve) => {
        body(true).pipe(etl.map(Object)).promise().then(data => {
          resolve(String(Buffer.concat(data)));
        });
      });
    }
    t.same(data[0].filename, 'fileB.txt');
    t.same(data[1].filename, 'folderA/fileA.txt');
    t.same( await getBuffer(data[0].body), 'This is file B');
    t.same( await getBuffer(data[1].body), 'This is file A');
  });

  t.test('files buffer', async () => {
    const cmd = `etl files/${__dirname}/support/testfiles test --silent`;
    const res = await cli(cmd);
    const data = res.data;
    t.same(data[0].filename, 'fileB.txt');
    t.same(data[1].filename, 'folderA/fileA.txt');
    t.same( String(await data[0].buffer()), 'This is file B');
    t.same( String(await data[1].buffer()), 'This is file A');
  });
});