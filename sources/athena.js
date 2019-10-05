const AWS = require('aws-sdk');
const etl = require('etl');
const crypto = require('crypto');
const Promise = require('bluebird');

module.exports = argv => {
  const Database =  argv.database ||  argv.source_collection;
  const table  = argv.table ||  argv.source_indextype;
  if (!table && !argv.source_query) throw '--table or --query missing';
  const QueryString = argv.source_query || `select * from ${table}`;
  const OutputLocation = argv.outputLocation || argv.source_config.outputLocation;
  const ClientRequestToken = crypto.createHash('md5').update(QueryString+argv.version).digest('hex');  

  const athena = new AWS.Athena(argv.source_config);

  return () => etl.toStream(async function() {
    const params = {
      QueryString,
      ClientRequestToken,
      QueryExecutionContext: { Database },
      ResultConfiguration: { OutputLocation },
      WorkGroup: 'primary'
    };

    const res = await athena.startQueryExecution(params).promise();
    const QueryExecutionId = res.QueryExecutionId;

    await athena.getQueryExecution({QueryExecutionId}).promise();

    const fetch = async(NextToken) => {
      let d;
      try {
        d = await athena.getQueryResults({QueryExecutionId, NextToken}).promise();
      } catch(e) {
        if (/QUEUED|RUNNING/.test(e.message)) {
          process.stdout.write('@');
          await Promise.delay(1000);
          return fetch(NextToken);
        } else {
          throw e;
        }
      }
 
      const cols = d.ResultSet.ResultSetMetadata.ColumnInfo;

      d.ResultSet.Rows.forEach(row => {
        this.push(row.Data.reduce( (p,d,i) => {
          let value = d[Object.keys(d)[0]];
          let col = cols[i];
          if (col.Precision < 2147483647) value = +value;
          p[col.Name] = value;
          return p;
        },{}));
      });
      if (d.NextToken) return fetch(d.NextToken);
    };

    await fetch();
  }); 
};