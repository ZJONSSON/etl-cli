const etl = require('etl');

module.exports = (stream,argv) => {
  const pg = require('pg');
  ['target_collection', 'target_indextype'].forEach(key => { if(!argv[key]) throw `${key} missing`;});

  let conf = argv.target_config;
  let pool = new pg.Pool(conf);

  let fn = argv.upsert ? etl.postgres.upsert : etl.postgres.insert;
  argv = Object.assign({}, argv, {concurrency: argv.target_concurrency || 5});

  return stream.pipe(fn(pool, argv.target_collection, argv.target_indextype, argv));
};