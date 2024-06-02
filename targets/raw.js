const etl = require('etl');

module.exports = function(stream, argv) {
  return stream.pipe(etl.toFile(argv.dest));
};