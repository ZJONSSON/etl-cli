const etl = require('etl');
const httpAwsEs = require('http-aws-es');
const Promise = require('bluebird');

module.exports = (stream,argv,schema) => {
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
    .then( () => {
      // Ensure the index exists
      
      return client.indices.create({index: argv.target_index, type: argv.target_indextype})
        .then(
          () => !argv.silent && console.log(`Create Index ${indexStr} successful`),
          e  => !argv.silent && console.log(`Warning: Index ${indexStr} already exists`)
        );
    })
    .then( () => {
      if (!schema.elasticMapping) return;

      return Promise.resolve(typeof schema.elasticMapping === 'function' ? schema.elasticMapping() : schema.elasticMapping)
        .then(mapping =>client.indices.putMapping({
          index: argv.target_index,
          type: argv.target_indextype,
          body: mapping
        }))
        .then( () => !argv.silent && console.log(`Put mapping ${indexStr} successful`));
    })
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