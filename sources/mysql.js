const etl = require('etl');

module.exports = argv => {
  const mysql = require('mysql');
  ['source_host','source_database', 'source_table'].forEach(key => { if(!argv[key]) throw `${key} missing`;});

  let conf = {
    host: argv.source_host,
    user: argv.source_user,
    password: argv.source_password,
    database: argv.source_database,
    connectionLimit: argv.source_connectionLimit || 10
  };

  let pool = mysql.createPool(conf);
  var p = etl.mysql.execute(pool);

  query = argv.source_query || mysql.format('SELECT * FROM ??',[argv.source_table]);

  return {
    recordCount : () => p.query(mysql.format('SELECT COUNT(*) AS recordCount FROM ??',[argv.source_table])).then(d => d[0].recordCount),
    stream : () => p.stream(query)
  }
};
