const etl = require('etl');
const recursive = require('recursive-readdir');
const jsonSource = require('./json');
const csvSource = require('./csv');
const getFile = require('./getFile');

module.exports = function(argv) {

  const source_dir = argv.source_dir || argv.source_collection;
  if (!source_dir) throw 'Not source_dir';
  const reFilter = RegExp(argv['filter-files']);

  return {
    stream: () => etl.toStream(() => recursive(source_dir))
      .pipe(etl.map(filename => {
        if (reFilter.exec(filename)) return {
          filename: filename.replace(source_dir + '/', ''),
          body: (raw) => {
            raw = raw || argv.source_raw;
            if (!raw && /.json$/.test(filename)) return jsonSource({ ...argv, source: filename, })();
            else if (!raw && /.csv$/.test(filename)) return csvSource({ ...argv, source: filename, })();
            else return getFile(filename);
          }
        };
      }))
  };
};