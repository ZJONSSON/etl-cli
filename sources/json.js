const etl = require('etl');
const getFile = require('./getFile');

module.exports = function(argv) {
  return () => getFile(argv.source)
    .pipe(etl.split())
    .pipe(etl.map(d => JSON.parse(d.text || d)));
};