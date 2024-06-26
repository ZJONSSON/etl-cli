const tap = require('tap');
const { cli } = require('./util');
const waitPort = require('wait-port');


tap.before(async function() {
  await waitPort({ host: '127.0.0.1', port: 27017 });
  const client = require("mongodb").MongoClient;
  const connection = await client.connect('mongodb://127.0.0.1:27017');
  const db = connection.db('test_schema');
  const coll = db.collection('test_coll');
  await coll.deleteMany({});
  connection.close();
});

tap.test('mongodb', async t => {
  t.test('writing data', async t => {
    const cmd = `etl ${__dirname}/support/test.csv mongo/test_schema/test_coll --target_uri="mongodb://127.0.0.1:27017"`;
    const res = await cli(cmd);
    t.same(res, { 'Σ_in': 2, 'Σ_out': 2 });
  });

  t.test('reading data', async t => {
    const cmd = `etl mongo/test_schema/test_coll test --source_uri="mongodb://127.0.0.1:27017"`;
    let res = await cli(cmd);
    res = res.data.map(d => {
      delete d._id;
      return { ...d };
    });
    const data = require('./support/test_collect.json');
    t.same(res, data);
  });
});