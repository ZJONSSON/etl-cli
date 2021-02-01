const etl = require('etl');
const Promise = require('bluebird');
const recursive = require('recursive-readdir');
const jsonSource = require('./json');
const getFile = require('./getFile');

module.exports = function(argv) {
  const source_dir = argv.source_dir || argv.source_collection;
  if (!source_dir) throw 'Not source_dir';
  const reFilter = RegExp(argv['filter-files']);
  const reSourceDir = new RegExp(`^${source_dir}/`);

  return {
    stream: () => etl.toStream(() => recursive(source_dir))
      .pipe(etl.map(filename => {
        if (reFilter.exec(filename)) return {
          filename: filename.replace(reSourceDir,''),
          body: () => /.json$/.test(filename) ? jsonSource({source: filename}) : getFile(filename)
        };
      }))
  };
};