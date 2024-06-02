const tap = require('tap');
const cli = require('../index');
const etl = require('etl');
const { path, requireAll } = require('./util');

requireAll(path('../sources'));

tap.test('inputs', async t => {
  t.test('csv', async () => {
    const res = await cli({ _: [path('./support/test.csv')], target_type: 'test', silent: 'true'});
    t.same(res, [
      {a: '1', b: '2', c: '3', __line: 2 },
      {a: '4', b: '5', c : '6', __line: 3 }
    ]);
  });

  t.test('json', async () => {
    const res = await cli({ _: [path('./support/test.json')], target_type: 'test', silent: 'true'});
    t.same(res, [
      {"a":"1", "b":"2", "c":"3"},
      {"a":"4", "b":"5", "c":"6"}
    ]);
  });

  t.test('excel', async () => {
    const res = await cli({ _: [path('./support/test.xlsx')], target_type: 'test', silent: 'true'});
    t.same(res, [
      {"a":1, "b":2, "c":3},
      {"a":4, "b":5, "c":6}
    ]);
  });

  t.test('parquet', async () => {
    const res = await cli({ _: [path('./support/test.parquet')], target_type: 'test', silent: 'true'});
    t.same(res, [
      {"a":"1", "b":"2", "c":"3"},
      {"a":"4", "b":"5", "c":"6"}
    ]);
  });

  t.test('files body', async () => {
    const res = await cli({ _:[], source_type: 'files', source_dir: path('support/testfiles'), target_type: 'test', silent: 'true'});

    function getBuffer(body) {
      return new Promise((resolve) => {
        body(true).pipe(etl.map(Object)).promise().then(data => {
          resolve(String(Buffer.concat(data)));
        });
      });
    }

    t.same(res[0].filename, 'fileB.txt');
    t.same(res[1].filename, 'folderA/fileA.txt');
    t.same( await getBuffer(res[0].body), 'This is file B');
    t.same( await getBuffer(res[1].body), 'This is file A');
  });

  t.test('files buffer', async () => {
    const res = await cli({ _:[], source_type: 'files', source_dir: path('support/testfiles'), target_type: 'test', silent: 'true'});
    t.same(res[0].filename, 'fileB.txt');
    t.same(res[1].filename, 'folderA/fileA.txt');
    t.same( String(await res[0].buffer()), 'This is file B');
    t.same( String(await res[1].buffer()), 'This is file A');
  });

  t.test('xml', async () => {
    const res = await cli({ _: [path('./support/test.xml')], target_type: 'test', silent: 'true'});
    t.same(res, [{
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