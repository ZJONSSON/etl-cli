const etl = require('etl');
const { createConfig } = require('../util');

module.exports = (stream,argv) => {
  const mysql = require('mysql');
  const config = createConfig(argv.target_config, argv, 'target', ['host','connectionLimit', 'user','password'])
  let pool = mysql.createPool(config);

  const ret = stream.pipe(etl.mysql.upsert(pool, argv.target_collection, argv.target_indextype, argv));
  ret.promise().then(() => pool.end());
  return ret;
};