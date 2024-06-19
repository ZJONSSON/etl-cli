const etl = require('etl');
const { createConfig } = require('../util');

module.exports = (stream, argv) => {
  const pg = require('pg');
  const config = createConfig(argv.target_config, argv, 'target', ['host', 'port', 'database', 'user', 'password']); argv.target_config || {};
  const pool = new pg.Pool(config);

  const fn = argv.upsert ? etl.postgres.upsert : etl.postgres.insert;
  argv = Object.assign({}, argv, { concurrency: argv.target_concurrency || 5 });

  const ret = stream.pipe(fn(pool, argv.target_collection, argv.target_indextype, argv));
  ret.promise().then(() => pool.end());
  return ret;
};