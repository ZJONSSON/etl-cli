const etl = require('etl');

module.exports = async (stream,argv) => {
  const {BigQuery} = require('@google-cloud/bigquery');

  ['target_index','target_indextype'].forEach(key => { if(!argv[key]) throw `${key} missing`;});
  const projectId = argv.target_config && argv.target_config.projectId || argv.project_id;
  if (!projectId) throw 'Missing ProjectID';

  const bigquery = new BigQuery(argv.target_config || {projectId});
  const dataset = bigquery.dataset(argv.target_index);
  const table = dataset.table(argv.target_indextype);
  const options = Object.assign({}, argv, {concurrency: argv.target_concurrency || 10});
  return stream.pipe(etl.bigquery.insert(table,options));
};