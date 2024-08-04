const tap = require('tap');
const { cli } = require('./util');

const schema = require('./support/nested_schema.json');
const parquetSchema = require('./support/nested_schema_parquet.json');
const glueSchema = require('./support/nested_schema_glue.json');

tap.test('schema', async t => {
  t.test('gets schema from json', async t => {
    const cmd = `etl ${__dirname}/support/nested.json test --export_schema=true`;
    const res = await cli(cmd);
    t.same(res.argv.schema_required, true);
    t.same(res.data[0], schema );
  });

  t.test('schema is not required by default', async t => {
    const cmd = `etl ${__dirname}/support/nested.json test`;
    const res = await cli(cmd);
    t.same(res.argv.schema_required, false);
  });

  t.test('gets schema from csv', async t => {
    const cmd = `etl ${__dirname}/support/nested.csv test --export_schema=true`;
    const res = await cli(cmd);
    t.same(res.argv.schema_required, true);
    t.same(res.data[0], schema );
  });

  t.test('gets schema from parquet', async t => {
    const cmd = `etl ${__dirname}/support/nested.parquet test --export_schema=true`;
    const res = await cli(cmd);
    t.same(res.data[0], parquetSchema );
    t.same(res.data[0]['$comment'], 'extracted from parquet schema');
  });

  t.test('gets glue schema from parquet', async t => {
    const cmd = `etl ${__dirname}/support/nested.parquet test --export_glue_schema=true`;
    const res = await cli(cmd);
    t.same(res.data[0], glueSchema );
  });

  t.test('any transform drops the original schema and derives it from output data', async t => {
    const cmd = `etl ${__dirname}/support/nested.parquet test --transform="d => { d.z = 42; return d}" --export_schema=true`;
    const res = await cli(cmd);
    t.same(res.data[0].properties, { ...schema.properties, z: { type: 'integer' } });
    t.same(res.data[0]['$comment'], undefined);
  });
});
