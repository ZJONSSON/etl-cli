const AWS = require('aws-sdk');
const etl = require('etl');
const crypto = require('crypto');
const Promise = require('bluebird');
const athenaParser = require('./lib/athenaParser');

module.exports = argv => {
  if (argv.version && argv.version.includes('rand')) argv.version = String(Math.random());
  const Database =  argv.database ||  argv.source_collection;
  const table  = argv.table ||  argv.source_indextype;
  if (!table && !argv.source_query) throw '--table or --query missing';
  const QueryString = argv.source_query || `select * from ${table}`;
  const OutputLocation = argv.outputLocation || argv.source_config.outputLocation;

  const athena = new AWS.Athena(argv.source_config || argv);
  const s3 = new AWS.S3(argv.source_config || argv);

  return () => etl.toStream(QueryString.split(/;\s*\n/g))
    .pipe(etl.map(async function(QueryString) {
      if (!QueryString.trim().length) return;

      let version = argv.version && argv.version.includes('rand') ? Math.random() : argv.version;
      const ClientRequestToken = crypto.createHash('md5').update(QueryString+version).digest('hex');

      if (argv.verbose) console.log(`Executing query ${JSON.stringify(QueryString.slice(0,70))}`);
      
      const params = {
        QueryString,
        ClientRequestToken,
        QueryExecutionContext: { Database },
        ResultConfiguration: { OutputLocation },
        WorkGroup: 'primary'
      };

      const res = await athena.startQueryExecution(params).promise();
      const QueryExecutionId = res.QueryExecutionId;

      const fetch = async() => {
        let execution = await athena.getQueryExecution({QueryExecutionId}).promise();
        execution = execution.QueryExecution;
        const state = execution.Status.State;
        if (/QUEUED|RUNNING/.test(state))
          return Promise.delay(250).then(() => fetch());
        else if (state == 'FAILED')
          throw execution.Status.StateChangeReason.message || execution.Status.StateChangeReason;
        else if (state == 'SUCCEEDED') {
          const [, Bucket, Key] = /s3:\/\/([^/]+)\/(.*)/.exec(execution.ResultConfiguration.OutputLocation);
          return s3.getObject({Bucket, Key})
            .createReadStream()
            .pipe(etl.csv())
            // TODO nested fields need to be parsed into JSON
            .pipe(etl.map(d => {
              Object.keys(d).forEach(key => {
                // Try decoding structured output
                if (d[key][0] == '[' || d[key[0] == '{']) {
                  try {
                    d[key] = athenaParser.parse(d[key]);
                  } catch(e) {}
                }
              });
              this.push(d);
            }))
            .promise();
        }
      };

      await fetch();
    })); 
};