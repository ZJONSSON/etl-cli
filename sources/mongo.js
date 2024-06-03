const etl = require('etl');
const { EJSON } = require('bson');

module.exports = async argv => {
  const useEjson = argv.ejson && /false/i.exec(argv.ejson) ? false : true;
  if (argv.source_params.length > 1) {
    argv.source_db_name = argv.source_params[0];
    argv.source_collection = argv.source_params[1];
  } else {
    argv.source_collection = argv.source_collection || argv.source_params[0];
  }

  ['source_uri', 'source_collection'].forEach(key => { if(!argv[key]) throw `${key} missing`;});
  const client = require("mongodb").MongoClient;
  const connection = await client.connect(argv.source_uri);
  const db = connection.db(argv.source_db_name);
  const coll = db.collection(argv.source_collection);

  let query = argv.source_query;
  if (useEjson && query) query = EJSON.parse(JSON.stringify(argv.source_query));

  const fields = argv.fields ? argv.fields.split(',').reduce( (p, key) => { p[key] = 1; return p;}, {}) : undefined;
  return {
    recordCount : () => coll.countDocuments(query),
    stream: () => coll
      .find(query, fields)
      .stream()
      .pipe(etl.map(d => {
        if (useEjson) d = JSON.parse(EJSON.stringify(d));
        else d._id = String(d._id);
        return d;
      }))
      .on('finish', () => connection.close())

  };
};