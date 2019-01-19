const minimist = require('minimist');
const etl = require('etl');
const path = require('path');
const Promise = require('bluebird');
const nconf = require('nconf');
const fs = require('fs');

module.exports = async function(obj,argv) {
  argv = Object.assign({},argv || minimist(process.argv.slice(2)));  
  let dest = argv.target || argv._[0];

  // If a config file is specified we load it
  if (argv.config)
    nconf.load(path.resolve(argv.config));

  // If dest has '/'
  if (dest && dest.match('/')) {
    dest = dest.split('/');

    let trialPath = dest.slice(0,dest.length-1).join('/');
    // If dest is not a path, we break it up into target_index and target_collection
    if (!fs.existsSync(trialPath)) {
      argv.target_index = argv.target_index || dest[1];
      argv.target_collection = argv.target_collection || dest[1];
      argv.target_indextype = argv.target_indextype || dest[2];

      dest = dest[0];
    } else {
      dest = dest.join('/');
    }
  }

  argv.target_gzip = dest && dest.match(/\.gz$/ig);

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
    Promise.try(() => obj.recordCount(argv))
      .catch(console.log)
      .then(d => total = d)
      .then(() => console.log('total',total));


  let Σ = 0,last =0,counter,total;
  if (!argv.silent) {
    counter = setInterval(() => {
      let Δ = Σ - last;
      last = Σ;
      const heap = Math.round(process.memoryUsage().heapUsed/1000000);
      console.log(`Σ${Σ} Δ${Δ} ${total && (Math.floor(Σ/total*10000)/100)+'%' ||''} - Heap: ${heap} Mb`);
    }, argv.report_interval || 1000);
  }

  let m = /\.(json|csv)/.exec(dest);
  argv.target_type = argv.target_type ||  (m && m[1]) || (dest && dest.toLowerCase()) || 'screen';
  let type = argv.target_type;

  if (/.parquet$/.test(dest)) {
    type = argv.target_type = 'raw';
  }

  if (!argv.silent) {
    console.log(`Target: ${dest} - type ${type}  ${ (!!argv.upsert && 'w/upsert') || (!!argv.update && 'w/update') || ''}`);
  }

  let stream = obj.stream(argv).on('error',e => {
    console.error('error',e);
    process.exit();
  });

  if (argv.transform) {
    let transform_concurrency = argv.transform_concurrency || argv.concurrency || 1;
    argv.transform.split(',').forEach(transform => {
      transform = require(path.resolve('.',transform));
      stream = stream.pipe(etl.map(transform_concurrency,function(d) {
        return transform.call(this,d,argv);
      },{
        catch: console.log,
        flush: transform.flush
      }));
    });
  }

  if (argv.chain) {
    let chain = require(path.resolve('.',argv.chain));
    stream = stream.pipe(etl.chain(incoming => chain(incoming,argv)));
  }

  if (obj[type] && typeof obj[type].transform === 'function')
    stream = stream.pipe(etl.map(obj[type].transform));

  stream = stream.pipe(etl.map(function(d) {
    if (argv.setid)
      d._id = d[argv.setid];

    if (argv.select)
      d = argv.select.split(',').reduce( (p,key) => {
        if (d[key] !== undefined)
          p[key] = d[key];
        return p;
      },{});

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
  },{highWaterMark: argv.highWaterMark || 100}));


  argv.dest = dest;


  try {
    type = require(path.resolve(__dirname,'targets',type+'.js'));
  } catch(e) {
    if (e.code === 'MODULE_NOT_FOUND')
      throw 'target_type '+type+' not available';
    else
      throw e;
  }

  if (argv.collect) stream = stream.pipe(etl.collect(argv.collect));

  return (await type(stream,argv,obj))
    .pipe(etl.map(d => { if (!argv.silent) console.log(JSON.stringify(d,null,2));}))
    .promise()
    .then(() => {
      clearInterval(counter);
      if (!argv.silent)
        console.log(`Completed ${Σ} records`);
    }, e => {
      if (e.errors) console.log('errors', JSON.stringify(e.errors,null,2));
      else console.error('error',e.errors || e)
    })
    .then(() => setImmediate(() => process.exit()));
};
