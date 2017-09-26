const etl = require('etl');

module.exports = argv => {
  const mongodb = require('mongodb');
  let query = undefined;
  
  ['source_uri','source_collection'].forEach(key => { if(!argv[key]) throw `${key} missing`;});

  const db = mongodb.connect(argv.source_uri);

  return {
    recordCount : () => db.then(db => 
      db.collection(argv.source_collection).count(query)
    ),
    stream: () => etl.toStream(db.then(db =>
      db.collection(argv.source_collection)
        .find(query)
        .pipe(etl.map(d => {
          d._id = String(d._id);
          return d;
        }))
    ))
  };
};