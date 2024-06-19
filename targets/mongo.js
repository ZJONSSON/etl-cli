const etl = require('etl');
const { EJSON } = require('bson');

module.exports = async (stream, argv) => {
  const useEjson = argv.ejson && /false/i.exec(argv.ejson) ? false : true;
  if (argv.target_params.length > 1) {
    argv.target_db_name = argv.target_params[0];
    argv.target_collection = argv.target_params[1];
  } else {
    argv.target_collection = argv.target_collection || argv.target_params[0];
  }

  ['target_uri', 'target_collection'].forEach(key => { if(!argv[key]) throw `${key} missing`;});

  const client = require("mongodb").MongoClient;
  const connection = await client.connect(argv.target_uri);
  const db = connection.db(argv.target_db_name);
  const coll = db.collection(argv.target_collection);

  if (useEjson) stream = stream.pipe(etl.map(d => {
    return EJSON.parse(JSON.stringify(d));
  }));

  let out = stream;

  if (!argv.update && !argv.upsert)
    out = out.pipe(etl.mongo.insert(coll));
  else {
    let ids = (argv.upsert || argv.update);
    if (typeof ids === 'string')
      ids = ids.split(',');
    else
      ids = '_id';
    out = out.pipe(etl.mongo.update(coll, ids, { upsert: !!argv.upsert }));
  }
  out.on('finish', () => connection.close());
  return out;
};