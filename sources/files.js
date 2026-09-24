const etl = require('etl');
const { promises: fs } = require('fs');
const path = require('path');
const { Readable } = require('stream');
const jsonSource = require('./json');
const csvSource = require('./csv');
const getFile = require('./getFile');

// A file can disappear, or be a dangling symlink, while a directory is being
// scanned. Ignore that entry, but still fail for a missing source directory
// and for all other filesystem errors.
async function* recursive(source_dir, root = true) {
  let filenames;
  try {
    filenames = await fs.readdir(source_dir);
  } catch (e) {
    if (!root && e.code === 'ENOENT') return;
    throw e;
  }

  for (const filename of filenames) {
    const file = path.join(source_dir, filename);
    let stats;

    try {
      stats = await fs.stat(file);
    } catch (e) {
      if (e.code === 'ENOENT') continue;
      throw e;
    }

    if (stats.isDirectory()) {
      yield* recursive(file, false);
    } else {
      yield file;
    }
  }
}

module.exports = function(argv) {

  const source_dir = argv.source_dir || argv.source_collection;
  if (!source_dir) throw 'Not source_dir';
  const reFilter = RegExp(argv['filter-files']);

  return {
    stream: () => Readable.from(recursive(source_dir))
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
