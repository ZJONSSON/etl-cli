const etl = require('etl');
const httpAwsEs = require('http-aws-es');

module.exports = argv => {
  if (! (typeof argv.source_config === 'object' || argv.source_host))
    throw 'source_config or source_host missing for elastic';

  if (!argv.source_index)
    throw 'source_index missing';

  let config = Object.assign({},argv.source_host ? { host: argv.source_host } : argv.source_config );

  if (config.amazonES)
    config.connectionClass = httpAwsEs;

  const client = new require('elasticsearch').Client(config);
  const payload = {
      index: argv.source_index,
      type: argv.source_indextype,
      size: argv.source_size || 1000,
      body: {query: argv.source_query}
    };

  return {
    elastic: {
      mapping: () => {
        return client.indices.getMapping({index: argv.source_index, type: argv.source_indextype})
          .then(d => d[argv.source_index].mappings[argv.source_indextype]);
      },
      settings: () => client.indices.getSettings({index: argv.source_index}).then(d => d[argv.source_index].settings)
    },
    recordCount: () => client.search(payload).then(d => d.hits.total),
    stream: () => etl.elastic.scroll(client,payload)
  };
};
