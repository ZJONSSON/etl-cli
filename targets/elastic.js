const etl = require('etl');
const httpAwsEs = require('http-aws-es');
const Promise = require('bluebird');

module.exports = (stream,argv,schema) => {
  schema = schema && schema.elastic;

  ['target_index','target_indextype']
    .forEach(key => { if(!argv[key]) throw `${key} missing`;});

  if (!argv.target_config && !argv.target_host) 
    throw 'target_config or target_host missing';

  let config = Object.assign({},argv.target_config || { host: argv.source_host });

  // Collect 100 records by default for bulk indexing
  const out = stream.pipe(etl.collect(argv.collect || 100));

  // If amazonES parameters are defined, we use the aws connection class
  if (config.amazonES)
    config.connectionClass = httpAwsEs;

  const mapping = Promise.resolve(schema.mapping && typeof schema.mapping === 'function' ? schema.mapping() : schema.mapping)
    .then(mapping => ({[argv.target_index]: mapping}));

  const settings = Promise.resolve(schema.settings && typeof schema.settings === 'function' ? schema.settings() : schema.settings);
  
  const client = new require('elasticsearch').Client(config);

  return etl.toStream(function() {
    const indexStr = `${argv.target_index}/${argv.target_indextype}`;

    return Promise.try( ()=> {
      // Start by deleting the index if `delete_target` is defined
      if (argv.delete_target)
        return client.indices.delete({index: argv.target_index, type: argv.target_indextype})
          .then(
            d => !argv.silent && console.log(`Delete Index ${indexStr} successful`),
            e => !argv.silent && console.log(`Delete Index ${indexStr} failed: ${e.message}`)
          );
    })
    .then(() => Promise.join(settings,mapping, (settings,mapping) => {
      // Try creating the index with settings and mappings (if defined)
      if (settings)
        ['provided_name','creation_date','uuid', 'version'].forEach(f => delete settings.index[f]);
      return client.indices.create({
        index: argv.target_index,
        type: argv.target_indextype,
        body: {
          settings: settings,
          mapping: mapping 
        }
      })
      .then(
        () => !argv.silent && console.log(`Create Index ${indexStr} successful`),
        e => {
          // If index already exists we try to update mapping
          if (!argv.silent)
            console.log(`Warning: Index ${indexStr} already exists ${settings && '- settings not updated'}`);
          if (mapping)
            return client.indices.putMapping({index: argv.target_index, type: argv.target_indextype, body: mapping})
              .then( () => !argv.silent && console.log(`Put Mapping ${indexStr} successful`));
        }
      );
    }))
    .then( () => {
      const options = {
        pushErrors: !argv.hide_target_errors,
        concurrency: argv.target_concurrency || 5
      };

      if (argv.update)
        return out.pipe(etl.elastic.update(client,argv.target_index,argv.target_indextype,options));
      else if (argv.upsert)
        return out.pipe(etl.elastic.upsert(client,argv.target_index,argv.target_indextype,options));
      else
        return out.pipe(etl.elastic.index(client,argv.target_index,argv.target_indextype,options));
    });
  });
  
};