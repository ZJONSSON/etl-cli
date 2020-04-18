const etl = require('etl');

module.exports = function(stream,argv) {
  return stream.pipe(etl.stringify(argv.json_indent || 0,null,true));
};