const etl = require('etl');

module.exports = (stream, argv) => {
  return stream.pipe(argv.source === 'screen' ? etl.map(d => console.log(d)) : etl.toFile(argv.dest));
}