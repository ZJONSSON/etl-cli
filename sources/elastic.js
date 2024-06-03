const etl = require('etl');
const httpAwsEs = require('http-aws-es');
const AWS = require('aws-sdk');

module.exports = argv => {
  if (! (typeof argv.source_config === 'object' || argv.source_host))
    throw 'source_config or source_host missing for elastic';

  if (!argv.source_index)
    throw 'source_index missing';

  const config = Object.assign({}, argv.source_host ? { host: argv.source_host } : argv.source_config );

  const awsConfig = config.awsConfig || config.amazonES;
  if (awsConfig) {
    config.connectionClass = httpAwsEs;
    config.awsConfig = new AWS.Config({
      accessKeyId: awsConfig.accessKeyId || awsConfig.accessKey,
      secretAccessKey: awsConfig.secretAccessKey || awsConfig.secretKey,
      region: awsConfig.region
    });
  }

  const elastic = require('@elastic/elasticsearch');
  const client = new elastic.Client(config);
  const payload = {
    index: argv.source_index,
    type: argv.source_indextype,
    size: argv.source_size || 1000,
    body: argv.source_query && argv.source_query.query ? argv.source_query : { query: argv.source_query },
    scroll: argv.source_scroll || argv.scroll || '60s'
  };

  return {
    elastic: {
      mapping: () => {
        return client.indices.getMapping({ index: argv.source_index })
          .then(d => {
            d = d[argv.source_index].mappings[argv.source_indextype];
            d._all = undefined;
            return d;
          });
      },
      settings: () => client.indices.getSettings({ index: argv.source_index }).then(d => d[argv.source_index].settings)
    },
    recordCount: () => client.search(payload).then(d => d.hits.total),
    stream: () => etl.elastic.scroll(client, payload)
      .pipe(etl.map(d => {
        d._source._id = d._id;
        return d._source;
      }))
  };
};
