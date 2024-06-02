const etl = require('etl');

module.exports = stream => stream
  .pipe(etl.stringify(2, null, true))
  .pipe(etl.map(d => console.log(d)));