const tap = require('tap');
const { cli } = require('./util');

const schema = require('./support/nested_schema.json');

function fixSchema(obj, addComment) {
  if (obj && typeof obj == 'object') {
    obj = { ...obj };
    if (addComment) {
      if (obj.type == 'integer') {
        obj.comment = 'INT_64';
      } else if (obj.type == 'number') {
        obj.comment = 'DOUBLE';
      }
    }
    return Object.keys(obj).reduce( (p, key) => {
      if (key !== 'required') {
        p[key] = fixSchema(obj[key], addComment);
      }
      return p;
    }, {});
  }
  return obj;
}

// the parquet test file has all fields as optional
const schemaWithComments = fixSchema(schema, true);


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
    //console.log(res.data[0].properties, schemaWithComments);process.exit()
    t.same(res.data[0].properties, schemaWithComments.properties );
    t.same(res.data[0]['$comment'], 'extracted from parquet schema');
  });

  t.test('any transform drops the original schema and derives it from output data', async t => {
    const cmd = `etl ${__dirname}/support/nested.parquet test --transform="d => { d.z = 42; return d}" --export_schema=true`;
    const res = await cli(cmd);
    t.same(res.data[0].properties, { ...schema.properties, z: { type: 'integer' } });
    t.same(res.data[0]['$comment'], undefined);
  });

  t.test('argv.collect returns a correct schema', async t => {
    const cmd = `etl ${__dirname}/support/nested.parquet test --collect=true --export_schema=true`;
    const res = await cli(cmd);
    t.same(res.data[0].items.properties, schemaWithComments.properties );
    t.same(res.data[0]['$comment'], 'extracted from parquet schema');
  });

});
