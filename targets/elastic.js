const etl = require('etl');
const httpAwsEs = require('http-aws-es');
const Bluebird = require('bluebird');
const AWS = require('aws-sdk');

module.exports = (stream, argv, schema) => {
  schema = schema && schema.elastic || {};

  ['target_index', 'target_indextype']
    .forEach(key => { if(!argv[key]) throw `${key} missing`;});

  if (!argv.target_config && !argv.target_host)
    throw 'target_config or target_host missing';

  const target_indextype = argv.target_indextype !== 'custom' ? argv.target_indextype : undefined;

  const config = Object.assign({}, argv.target_host ? { host: argv.target_host } : argv.target_config );

  // Collect 100 records by default for bulk indexing
  const out = stream.pipe(etl.collect(argv.collect || 100));

  // If amazonES parameters are defined, we use the aws connection class
  const awsConfig = config.awsConfig || config.amazonES;
  if (awsConfig){
    config.connectionClass = httpAwsEs;
    config.awsConfig = new AWS.Config({
      accessKeyId: awsConfig.accessKeyId || awsConfig.accessKey,
      secretAccessKey: awsConfig.secretAccessKey || awsConfig.secretKey,
      region: awsConfig.region
    });
  }

  const mapping = Bluebird.resolve(schema.mapping && typeof schema.mapping === 'function' ? schema.mapping() : schema.mapping)
    .then(mapping => mapping && ({[target_indextype]: mapping || {}}));

  const settings = Bluebird.resolve(schema.settings && typeof schema.settings === 'function' ? schema.settings() : schema.settings);

  const client = new require('elasticsearch').Client(config);

  return etl.toStream(function() {
    const indexStr = `${argv.target_index}/${target_indextype}`;

    return Bluebird.try( ()=> {
      // Start by deleting the index if `delete_target` is defined
      if (argv.delete_target)
        return client.indices.delete({index: argv.target_index, type: target_indextype})
          .then(
            () => !argv.silent && console.log(`Delete Index ${indexStr} successful`),
            e => !argv.silent && console.log(`Delete Index ${indexStr} failed: ${e.message}`)
          );
    })
      .then(() => Bluebird.join(settings, mapping, (settings, mapping) => {
      // Try creating the index with settings and mappings (if defined)
        if (settings && settings.index)
          ['provided_name', 'creation_date', 'uuid', 'version'].forEach(f => delete settings.index[f]);

        return client.indices.create({
          index: argv.target_index,
          body: {
            settings: settings,
            mappings: mapping
          }
        })
          .then(
            () => !argv.silent && console.log(`Create Index ${indexStr} successful`),
            e => {
              if (e.message &&
            (e.message.indexOf('IndexAlreadyExistsException') === -1) &&
            (e.message.indexOf('index_already_exists_exception') === -1) &&
            (e.message.indexOf('resource_already_exists_exception') === -1)
              )
                throw e;

              // If index already exists we try to update mapping
              if (!argv.silent)
                console.log(`Warning: Index ${indexStr} already exists ${settings && '- settings not updated'}`);

              if (mapping)
                return client.indices.putMapping({index: argv.target_index, type: target_indextype, body: mapping})
                  .then( () => !argv.silent && console.log(`Put Mapping ${indexStr} successful`));
            }
          );
      }))
      .then( () => {
        const options = {
          pushErrors: !argv.hide_target_errors,
          concurrency: argv.target_concurrency || 5,
          maxRetries: argv.max_retries || 0,
        };

        if (argv.update)
          return out.pipe(etl.elastic.update(client, argv.target_index, target_indextype, options));
        else if (argv.upsert)
          return out.pipe(etl.elastic.upsert(client, argv.target_index, target_indextype, options));
        else
          return out.pipe(etl.elastic.index(client, argv.target_index, target_indextype, options));
      });
  });

};
