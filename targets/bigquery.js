const etl = require('etl');

module.exports = async (stream, argv, schema) => {
  const { BigQuery } = require('@google-cloud/bigquery');
  schema = schema && schema.bigquery;

  ['target_index', 'target_indextype'].forEach(key => { if(!argv[key]) throw `${key} missing`;});
  const projectId = argv.target_config && argv.target_config.projectId || argv.project_id;
  if (!projectId) throw 'Missing ProjectID';

  const bigquery = new BigQuery(argv.target_config || { projectId });
  const dataset = bigquery.dataset(argv.target_index);
  const table = dataset.table(argv.target_indextype);
  const exists = await table.exists();
  if (!exists[0]) {
    if (schema) {
      schema = await schema;
      await dataset.createTable(argv.target_indextype, { schema, friendlyName: argv.target_indextype });
    } else {
      throw 'table does really not exists';
    }
  } else {
    if (argv.replace_table) {
      if (schema) {
        schema = await schema;
      } else {
        const metadata = await table.getMetadata();
        schema = metadata[0].schema;
      }
      await table.delete();
      await dataset.createTable(argv.target_indextype, { schema, friendlyName: argv.target_indextype });
      console.log('replacing table');
    }
  }

  const options = Object.assign({}, argv, { concurrency: argv.target_concurrency || 10 });
  return stream.pipe(etl.bigquery.insert(table, options));
};