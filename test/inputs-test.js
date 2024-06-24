const tap = require('tap');
const { cli } = require('./util');
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