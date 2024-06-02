const etl = require('etl');
const { createConfig } = require('../util');

// TODO: Add schema support for elastic, similar to mysql

module.exports = argv => {
  const pg = require('pg');
  const QueryStream = require('pg-query-stream');
  let query = argv.source_query;
  if (!query) {
    ['source_database', 'source_table'].forEach(key => { if(!argv[key]) throw `${key} missing`;});
    query = `select * from ${argv.source_database}.${argv.source_table}`;
  }
  const config = createConfig(argv.target_config, argv, 'target', ['host', 'port', 'database', 'user', 'password']);
  const pool = new pg.Pool(config);
  const p = new etl.postgres.postgres(pool);
  return {
    stream: () => {
      const pquery = new QueryStream(query);
      const stream = p.stream(pquery);
      stream.promise().then(() => pool.end());
      return stream;
    },
    recordCount: () => {
      return p.query(`select count(*) from (${query}) c`).then(d => d.rows[0].count);
    }
  };
};