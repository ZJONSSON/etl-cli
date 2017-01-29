const etl = require('etl');
const httpAwsEs = require('http-aws-es');

module.exports = (stream,argv) => {
  ['target_index','target_indextype'].forEach(key => { if(!argv[key]) throw `${key} missing`;});

  if (!argv.target_config && !argv.target_host) 
    throw 'target_config or target_host missing';


  let config = Object.assign({},argv.source_config || { host: argv.source_host });

  if (config.amazonES)
    config.connectionClass = httpAwsEs;
  
  const client = new require('elasticsearch').Client(config);
  const out = stream.pipe(etl.collect(argv.collect || 100));
   
  if (argv.update)
    return out.pipe(etl.elastic.update(client,argv.target_index,argv.target_indextype));
  else if (argv.upsert)
    return out.pipe(etl.elastic.update(client,argv.target_index,argv.target_indextype));
  else
    return out.pipe(etl.elastic.index(client,argv.target_index,argv.target_indextype));
};