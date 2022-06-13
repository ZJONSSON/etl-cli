const etl = require('etl');

// TODO: Add schema support for elastic, similar to mysql

module.exports = argv => {
  const pg = require('pg');
  const QueryStream = require('pg-query-stream');
  let query = argv.source_query;
  if (!query) {
    ['source_database', 'source_table'].forEach(key => { if(!argv[key]) throw `${key} missing`;});
    query = `select * from ${argv.source_database}.${argv.source_table}`;
  }
  let conf = argv.source_config;
  let pool = new pg.Pool(conf);
  const p = new etl.postgres.postgres(pool);
  return {
    stream: () => {
      const pquery = new QueryStream(query);
      return p.stream(pquery);
    },
    recordCount: () => {
      return p.query(`select count(*) from (${query}) c`).then(d => d.rows[0].count);
    }
  };
};