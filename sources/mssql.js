const etl = require('etl');

module.exports = argv => {
  const sql = require('mssql');
  let query = argv.source_query;
  if (!query) {
    ['source_database', 'source_table'].forEach(key => { if(!argv[key]) throw `${key} missing`;});
    query = `select * from ${argv.source_database}.${argv.source_table}`;
  }

  const connection = sql.connect(argv.source_url);

  return {
    stream: async () => {
      await connection;

      const out = etl.map(undefined, {highWaterMark: 100000});
      const request = new sql.Request();
      request.stream = true;
      request.query(query);
      request.on('row',d => {
        if (out.write(d) == false) request.pause();
      });
      request.on('error', e => out.emit('error',e));
      request.on('done', () => out.end());
      out.on('drain', () => request.resume());
      return out;
    },
    recordCount: async () => {
      await connection;
      const count = await sql.query(`select count(*) as cnt from (${query} ) c`);
      return count.recordset[0].cnt;
    }
  };
};