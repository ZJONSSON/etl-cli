const etl = require('etl');
const Promise = require('bluebird');
const recursive = require('recursive-readdir');
const recursiveAsync = Promise.promisify(recursive);
const jsonSource = require('./json');
const getFile = require('./getFile');

module.exports = function(argv) {
  const source_dir = argv.source_dir || argv.source_collection;
  if (!source_dir) throw 'Not source_dir';
  const reFilter = RegExp(argv['filter-files']);

  return {
    stream: () => etl.toStream(() => recursiveAsync(source_dir))
      .pipe(etl.map(filename => {
        if (reFilter.exec(filename)) return {
          filename,
          body: () => /.json$/.test(filename) ? jsonSource(filename) : getFile(filename)
        };
      }))
  };
};