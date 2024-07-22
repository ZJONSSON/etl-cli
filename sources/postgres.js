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
  const config = createConfig(argv.target_config, argv, 'source', ['host', 'port', 'user', 'password']);

  const pool = new pg.Pool(config);
  const p = new etl.postgres.postgres(pool);
  return {
    stream: () => {
      const pquery = new QueryStream(query);
      return p.stream(pquery)
        .on('end', () => pool.end());
    },
    recordCount: async () => {
      const res = await p.query(`select count(*) as count from (${query})`);
      return res.rows[0].count;
    }
  };
};