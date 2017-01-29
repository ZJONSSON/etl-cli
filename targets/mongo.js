const etl = require('etl');

module.exports = (stream,argv) => {
  ['target_uri','target_collection'].forEach(key => { if(!argv[key]) throw `${key} missing`;});
  const coll = require('mongodb').connect(argv.target_uri)
      .then(db => db.collection(argv.target_collection));

  let out = stream.pipe(etl.collect(argv.collect || 10));

  if (!argv.update && !argv.upsert)
    return out.pipe(etl.mongo.insert(coll));
  else {
    let ids = (argv.upsert || argv.update);
    if (typeof ids === 'string')
      ids = ids.split(',');
    else
      ids = '_id';
    return out.pipe(etl.mongo.update(coll,ids,{upsert: !!argv.upsert}));
  }
};