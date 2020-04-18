const csvStream = require('./lib/csvStream');
const destination = require('./lib/destination')

module.exports = function(stream,argv) {
  stream = csvStream(stream, argv);
  return destination(stream, argv);
};
