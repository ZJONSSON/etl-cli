const jsonStream = require('./lib/jsonStream');
const destination = require('./lib/destination')

module.exports = function(stream,argv) {
  stream = jsonStream(stream, argv);
  return destination(stream, argv);
};
