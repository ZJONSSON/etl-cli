const etl = require('etl');

module.exports = function(stream, argv) {
  if (argv.json_collect) {
    // TODO stream the collect by adding brackets on both sides and append commas
    stream = stream.pipe(etl.collect(Math.Inf));
  }
  return stream.pipe(etl.stringify(argv.json_indent || 0, null, true))
    .pipe(argv.source === 'screen' ? etl.map(d => console.log(d)) : etl.toFile(argv.dest));
};