const { AthenaClient, StartQueryExecutionCommand, GetQueryExecutionCommand } = require('@aws-sdk/client-athena');
const { S3Client, GetObjectCommand } = require('@aws-sdk/client-s3');
const etl = require('etl');
const Bluebird = require('bluebird');
const athenaParser = require('./lib/athenaParser');
// eslint-disable-next-line no-redeclare
const crypto = require('crypto');
const { createConfig } = require('../util');

module.exports = argv => {
  if (argv.version && argv.version.includes('rand')) argv.version = String(Math.random());
  const Database = argv.database || argv.source_collection;
  const table = argv.table || argv.source_indextype;
  if (!table && !argv.source_query) throw '--table or --query missing';
  const QueryString = argv.source_query || `select * from ${table}`;
  const OutputLocation = argv.outputLocation || argv.source_config.outputLocation;
  const config = createConfig(argv.source_config, argv, 'source');
  const athenaClient = new AthenaClient(config);
  const s3Client = new S3Client(config);

  return () => {
    const out = etl.map(null, { keepAlive: true });

    etl.toStream(QueryString.split(/;[\s\n]*/g))
      .pipe(etl.map(async function(QueryString) {
        if (!QueryString.trim().length) return;

        const version = argv.version && argv.version.includes('rand') ? Math.random() : argv.version;
        //@ts-ignore
        const ClientRequestToken = crypto.createHash('md5').update(QueryString + version).digest('hex');

        if (argv.verbose) console.log(`Executing query ${JSON.stringify(QueryString.slice(0, 70))}`);

        const params = {
          QueryString,
          ClientRequestToken,
          QueryExecutionContext: { Database },
          ResultConfiguration: { OutputLocation },
          WorkGroup: 'primary'
        };

        const res = await athenaClient.send(new StartQueryExecutionCommand(params));
        const QueryExecutionId = res.QueryExecutionId;

        const fetch = async() => {
          const getExecution = await athenaClient.send(new GetQueryExecutionCommand({ QueryExecutionId }));
          const execution = getExecution.QueryExecution;
          const state = execution.Status.State;
          if (/QUEUED|RUNNING/.test(state))
            return Bluebird.delay(250).then(() => fetch());
          else if (state == 'FAILED')
            throw execution.Status.StateChangeReason;
          else if (state == 'SUCCEEDED') {
            const [, Bucket, Key] = /s3:\/\/([^/]+)\/(.*)/.exec(execution.ResultConfiguration.OutputLocation);
            const res = await s3Client.send(new GetObjectCommand({ Bucket, Key }));
            if (!res.Body) throw 'No body in response';
            const stream = res.Body
              //@ts-ignore
              .pipe(etl.csv())
              .pipe(etl.map(d => {
                Object.keys(d).forEach(key => {
                  // Try decoding structured output
                  if (d[key][0] == '[' || d[key][0] == '{') {
                    try {
                      d[key] = athenaParser.parse(d[key]);
                    } catch(_e) {
                      //silently ignore
                    }
                  }
                });
                return d;
              }));

            stream.pipe(out);
            return new Promise( (resolve, reject) => stream.on('finish', resolve).on('error', reject));
          }
        };

        await fetch();
      }))
      .on('finish', () => out.end())
      .on('error', e => out.emit('error', e));
    return out;
  };
};