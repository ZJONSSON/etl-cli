const etl = require('etl');
const raxl = require('raxl');
const unzipper = require('unzipper');
const request = require('request');

module.exports = function(argv) {
  let directory;
  const out = etl.map();
  if (argv.source.startsWith('http')) {
    directory = unzipper.Open.url(request, Object.assign({ url: argv.source }, argv));
  } else {
    directory = unzipper.Open.file(argv.source);
  }

  directory
    .then( () => raxl(directory, argv))
    .then(workbook => workbook.sheet1().pipe(out))
    .catch(e => out.emit('error', e));

  return { stream: () => out };

};