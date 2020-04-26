const etl = require('etl');
const { EJSON } = require('bson');

module.exports = argv => {
  const mongodb = require('mongodb');
  const useEjson = argv.ejson && /false/i.exec(argv.ejson) ? false : true;
  
  ['source_uri','source_collection'].forEach(key => { if(!argv[key]) throw `${key} missing`;});

  const client = mongodb.connect(argv.source_uri, {useUnifiedTopology: true});
  let query = argv.source_query;
  if (useEjson && query) query = EJSON.parse(JSON.stringify(argv.source_query));

  const fields = argv.fields ? argv.fields.split(',').reduce( (p,key) => { p[key] = 1; return p;},{}) : undefined;

  return {
    recordCount : () => client.then(client => 
      client.db().collection(argv.source_collection).countDocuments(query)
    ),
    stream: () => etl.toStream(client.then(client =>
      client.db().collection(argv.source_collection)
        .find(query, fields)
        .pipe(etl.map(d => {
          if (useEjson) d = JSON.parse(EJSON.stringify(d));
          else  d._id = String(d._id);
          return d;
        }))
    ))
  };
};