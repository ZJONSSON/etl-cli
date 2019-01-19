const etl = require('etl');

module.exports = (stream,argv) => {
  const pg = require('pg');
  ['target_collection', 'target_indextype'].forEach(key => { if(!argv[key]) throw `${key} missing`;});

  let conf = argv.target_config;
  let pool = new pg.Pool(conf);

  return stream.pipe(etl.postgres.upsert(pool, argv.target_collection, argv.target_indextype, argv));
};