const minimist = require('minimist');
const etl = require('etl');
const fs = require('fs');
const path = require('path');
const nconf = require('nconf').env().file({file: process.env.ETL_CONFIG || '.etlconfig'});

module.exports = function(obj,argv) {
  argv = Object.assign({},argv || minimist(process.argv.slice(2)));  
  let dest = argv._[0];
  let conf = nconf.get(dest) || {};
  Object.assign(conf,argv);

  if (conf.remove)
    conf.remove = new RegExp(conf.remove);

  const stream = (obj.stream || obj)(argv).pipe(etl.map(d => {
    if (conf.setid)
      d._id = d[conf.setid];
    if (conf.remove)
      Object.keys(d).forEach(key => {
        if (conf.remove.exec(key))
          delete d[key];
      });
    return d;
  }));


  dest = conf.dest || dest;

  let type = /\.(json|csv)/.exec(dest);
  type = type && type[1] || dest && dest.toLowerCase() || 'screen';

  if (type === 'screen')
    dest = stream.pipe(etl.stringify(2,null,true)).pipe(etl.map(d => console.log(d)));
  else if (type === 'csv') {
    let headers;
    dest = stream.pipe(etl.map(function(d) {
      if (!headers) headers = function traverse(d) {


      };
    }));
  } else if (type === 'json') {
    dest = stream.pipe(etl.stringify(0,null,true)).pipe(etl.toFile(dest));
  } else if (type == 'mongo') {
    const mongo = require('mongodb');
    const db = mongo.connect(conf.mongo_uri)
      .then(db => db.collection(conf.collection));
    dest = stream.pipe(etl.collect(nconf.collect || 10));
    if(conf.id || conf.setid)
      dest = dest.pipe(etl.mongo.upsert(db,conf.id && conf.id.split('|') || '_id',conf));
    else
      dest = dest.pipe(etl.mongo.insert(db,conf));
  } else if (type === 'elastic') {
    const client = new require('elasticsearch').Client(conf);
    dest = stream.pipe(etl.collect(nconf.collect || 10))
      .pipe(dest.pipe(etl.elastic.index(client,conf.index,conf.type,conf)));
  }

   else {
    throw 'unknown format';
  }

  return dest.pipe(etl.map(console.log))
    .promise()
    .then(() => console.log('done'), e => console.log('error',e));
};

if (!module.parent) {
  require('./source')()
}