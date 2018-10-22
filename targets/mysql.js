const etl = require('etl');

module.exports = (stream,argv) => {
  const mysql = require('mysql');
  ['target_collection', 'target_indextype'].forEach(key => { if(!argv[key]) throw `${key} missing`;});

  let conf = argv.target_config;
  let pool = mysql.createPool(conf);

  return stream.pipe(etl.mysql.upsert(pool, argv.target_collection, argv.target_indextype, argv));
};