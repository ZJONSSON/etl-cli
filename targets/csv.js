const csv = require('./lib/csv');
const etl = require('etl');
const csvWriter = require('csv-write-stream');

module.exports = function(stream,argv) {
  return csv(stream,argv).pipe(argv.source === 'screen' ? etl.map(d => console.log(d)) : etl.toFile(argv.dest));
};
