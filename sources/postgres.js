const etl = require('etl');

// TODO: Add schema support for elastic, similar to mysql

module.exports = argv => {
  const pg = require('pg');
  const QueryStream = require('pg-query-stream');
  ['source_database', 'source_table'].forEach(key => { if(!argv[key]) throw `${key} missing`;});

  argv.source_table = argv.source_database+'.'+argv.source_table;

  let conf = argv.source_config;
  let pool = new pg.Pool(conf);
  const p = new etl.postgres.postgres(pool);
  return {
    stream: () => {
      const query = new QueryStream(`select * from ${argv.source_table}`);
      return p.stream(query);
    },
    recordCount: () => {
      return p.query(`select count(*) from ${argv.source_table}`).then(d => d.rows[0].count);
    }
  };
};
