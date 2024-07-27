const tap = require('tap');
const { cli } = require('./util');
const etl = require('etl');
const { path, requireAll } = require('./util');

requireAll(path('../sources'));

tap.test('inputs', async t => {
  t.test('csv', async () => {
    const cmd = `etl ${__dirname}/support/test.csv test --silent`;
    const res = await cli(cmd);
    t.same(res.data, [
      { a: '1', b: '2', c: '3' },
      { a: '4', b: '5', c : '6' }
    ]);
  });

  t.test('json', async () => {
    const cmd = `etl ${__dirname}/support/test.json test --silent`;
    const res = await cli(cmd);
    t.same(res.data, [
      { "a":"1", "b":"2", "c":"3" },
      { "a":"4", "b":"5", "c":"6" }
    ]);
  });

  t.test('excel', async () => {
    const cmd = `etl ${__dirname}/support/test.xlsx test --silent`;
    const res = await cli(cmd);
    t.same(res.data, [
      { "a":1, "b":2, "c":3 },
      { "a":4, "b":5, "c":6 }
    ]);
  });

  t.test('parquet', async () => {
    const cmd = `etl ${__dirname}/support/test.parquet test --silent`;
    const res = await cli(cmd);
    t.same(res.data, [
      { "a":"1", "b":"2", "c":"3" },
      { "a":"4", "b":"5", "c":"6" }
    ]);
  });

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

  t.test('xml', async () => {
    const cmd = `etl ${__dirname}/support/test.xml test --silent`;
    const res = await cli(cmd);
    t.same(res.data, [{
      "tag": "xml",
      "path": [
        "xml"
      ],
      "depth": 0,
      "attr": {},
      "value": {
        "a": [
          "1",
          "4"
        ],
        "b": [
          "2",
          "5"
        ],
        "c": [
          "3",
          "6"
        ]
      }
    }]);
  });
});