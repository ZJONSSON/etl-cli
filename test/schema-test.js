const tap = require('tap');
const { cli } = require('./util');

const schema = require('./support/nested_schema.json');

function removeRequired(obj) {
  if (obj && typeof obj == 'object') {
    return Object.keys(obj).reduce( (p, key) => {
      if (key !== 'required') {
        p[key] = removeRequired(obj[key]);
      }
      return p;
    }, {});
  }
  return obj;
}

const schemaWithoutRequired = removeRequired(schema);

tap.test('schema', async t => {
  t.test('gets schema from json', async t => {
    const cmd = `etl ${__dirname}/support/nested.json test --export_schema=true`;
    const res = await cli(cmd);
    t.same(res.data[0], schema );
  });

  t.test('gets schema from csv', async t => {
    const cmd = `etl ${__dirname}/support/nested.csv test --export_schema=true`;
    const res = await cli(cmd);
    t.same(res.data[0], schema );
  });

  t.test('gets schema from parquet', async t => {
    const cmd = `etl ${__dirname}/support/nested.parquet test --export_schema=true`;
    const res = await cli(cmd);
    t.same(res.data[0].properties, schemaWithoutRequired.properties );
    t.same(res.data[0]['$comment'], 'extracted from parquet schema');
  });

  t.test('any transform drops the original schema and derives it from output data', async t => {
    const cmd = `etl ${__dirname}/support/nested.parquet test --transform="d => { d.z = 42; return d}" --export_schema=true`;
    const res = await cli(cmd);
    t.same(res.data[0].properties, { ...schema.properties, z: { type: 'integer' } });
    t.same(res.data[0]['$comment'], undefined);
  });
});
