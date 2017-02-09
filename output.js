const minimist = require('minimist');
const etl = require('etl');
const path = require('path');
const Promise = require('bluebird');
const nconf = require('nconf').env().file({file: process.env.ETL_CONFIG || '.etlconfig'});

module.exports = function(obj,argv) {
  argv = Object.assign({},argv || minimist(process.argv.slice(2)));  
  let dest = argv.target || argv._[0];

  // If a config file is specified we load it
  if (argv.config)
    nconf.load(path.resolve(argv.config));

  // If dest has '/'
  if (dest && dest.match('/')) {
    dest = dest.split('/');
    argv.target_index = argv.target_index || dest[1];
    argv.target_collection = argv.target_collection || dest[1];
    argv.target_indextype = argv.target_indextype || dest[2];
    dest = dest[0];
  }

  // Load custom config for the target_type or output
  let conf = nconf.get(dest) || {};
  for (let key in conf)
    argv['target_'+key] = argv['target_'+key] || conf[key];

  // Removal should be a regex
  if (argv.remove)
    argv.remove = new RegExp(argv.remove);

  // Filter should be an array
  if (argv.filter)
    argv.filter = argv.filter.split('=');

  //  If not silent, we write periodic updates to console
  if (obj.recordCount)
    Promise.try(obj.recordCount)
      .catch(console.log)
      .then(d => total = d)
      .then(() => console.log('total',total));


  let Σ = 0,last =0,counter,total;
  if (!argv.silent) {
    counter = setInterval(() => {
      let Δ = Σ - last;
      last = Σ;
      console.log(`Σ${Σ} Δ${Δ} ${total && (Math.floor(Σ/total*10000)/100)+'%' ||''}`);
    }, argv.report_interval || 1000);
  }

  const stream = obj.stream(argv).pipe(etl.map(function(d) {
    if (argv.setid)
      d._id = d[argv.setid];

    if (argv.remove)
      Object.keys(d).forEach(key => {
        if (argv.remove.exec(key))
          delete d[key];
      });

    Σ+=1;
    total = d.__total || total;

    if (argv.filter)
      if (d[argv.filter[0]] !== argv.filter[1])
        return;

    if (argv.limit && Σ > argv.limit)
      return;

    if (!argv.limit || Σ <= argv.limit)
      return d;
  }));


  argv.dest = dest;

  let m = /\.(json|csv)/.exec(dest);
  let type = argv.target_type ||  (m && m[1]) || (dest && dest.toLowerCase()) || 'screen';

  if (!argv.silent)
    console.log(`Target: ${dest} - type ${type}  ${ (!!argv.upsert && 'w/upsert') || (!!argv.update && 'w/update') || ''} `);
  try {
    type = require(path.resolve(__dirname,'targets',type+'.js'));
  } catch(e) {
    if (e.code === 'MODULE_NOT_FOUND')
      throw 'target_type '+type+' not available';
    else
      throw e;
  }


  return type(stream,argv,obj)
    .pipe(etl.map(d => { if (!argv.silent) console.log(JSON.stringify(d,null,2));}))
    .promise()
    .then(() => {
      clearInterval(counter);
      if (!argv.silent)
        console.log(`Completed ${Σ} records`);
    }, e => console.log('error',e))
    .then(() => setImmediate(() => process.exit()));
};