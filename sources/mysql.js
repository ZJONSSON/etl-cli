const etl = require('etl');
const { createConfig } = require('../util');

module.exports = argv => {
  const mysql = require('mysql');
  if (!argv.source_query)
    ['source_database', 'source_table'].forEach(key => { if(!argv[key]) throw `${key} missing`;});

  const sourceSchemaTable = argv.source_database + '.' + argv.source_table;
  const config = createConfig(argv.source_config, argv, 'source', ['host', 'connectionLimit', 'user', 'password']);
  const pool = mysql.createPool(config);
  const p = etl.mysql.execute(pool);

  const query = argv.source_query || mysql.format('SELECT * FROM ??', [sourceSchemaTable]);

  return {
    recordCount : async () => {
      const res = await p.query(`with data as (${query}) SELECT COUNT(*) AS recordCount FROM data`);
      return res[0].recordCount;
    },
    stream : () => {
      return p.stream(query).
        on('end', () => pool.end());
    }
  };
};
