const etl = require('etl');

module.exports = function(stream,argv) {
  argv.test = [];
  return stream.pipe(etl.map(d => argv.test.push(d)))
};