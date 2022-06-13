const etl = require('etl');
const Promise = require('bluebird');
const recursive = require('recursive-readdir');
const jsonSource = require('./json');
const csvSource = require('./csv');
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
          filename,
          body: () => {
            if (/.json$/.test(filename)) return jsonSource({source: filename});
            else if (/.csv$/.test(filename)) return csvSource({source: filename})
            else return getFile(filename)
          }
        }
      }))
  };
};