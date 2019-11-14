const AWS = require('aws-sdk');
const etl = require('etl');
const crypto = require('crypto');
const Promise = require('bluebird');

module.exports = argv => {
  if (argv.version && argv.version.includes('rand')) argv.version = String(Math.random());
  const Database =  argv.database ||  argv.source_collection;
  const table  = argv.table ||  argv.source_indextype;
  if (!table && !argv.source_query) throw '--table or --query missing';
  const QueryString = argv.source_query || `select * from ${table}`;
  const OutputLocation = argv.outputLocation || argv.source_config.outputLocation;

  const athena = new AWS.Athena(argv.source_config);

  return () => etl.toStream(QueryString.split(/;\s*\n/g))
    .pipe(etl.map(async function(QueryString) {
      if (!QueryString.trim().length) return;

      let version = argv.version && argv.version.includes('rand') ? Math.random() : argv.version;
      const ClientRequestToken = crypto.createHash('md5').update(QueryString+version).digest('hex');

      if (argv.verbose) console.log(`Executing query ${JSON.stringify(QueryString.slice(0,70))}`)
      
      const params = {
        QueryString,
        ClientRequestToken,
        QueryExecutionContext: { Database },
        ResultConfiguration: { OutputLocation },
        WorkGroup: 'primary'
      };

      const res = await athena.startQueryExecution(params).promise();
      const QueryExecutionId = res.QueryExecutionId;

      const fetch = async(NextToken) => {
        let d;
        let execution = await athena.getQueryExecution({QueryExecutionId}).promise();
        execution = execution.QueryExecution;
        const state = execution.Status.State;
        if (state == 'RUNNING') return Promise.delay(250).then(() => fetch(NextToken));
        if (state == 'FAILED') throw execution.Status.StateChangeReason.message || execution.Status.StateChangeReason;
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
        if (!d.ResultSet) throw 'Athena internal error';
        if (argv.verbose && !NextToken) console.log(`Done query ${JSON.stringify(QueryString.slice(0,70))}`)
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
    })); 
};