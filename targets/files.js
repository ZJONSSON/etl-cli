const etl = require('etl');
const { createWriteStream, rename, utimes, stat, ensureDir } = require('fs-extra');
const recursive = require('recursive-readdir');

const path = require('path');
const os = require('os');
const bodyStream = require('./lib/bodyStream');

module.exports = async function(stream, argv) {
  const filter_files = argv.filter_files && new RegExp(argv.filter_files);
  let target_dir = argv.target_dir;
  if (!target_dir) throw 'Not target_dir';
  if (target_dir.startsWith('~')) {
    target_dir = path.join(os.homedir(), target_dir.slice(1));
  }

  let files = new Set([]);
  argv.target_files_scanned = false;

  if (!argv.target_overwrite && !argv.target_skip_scan) {
    if (argv.target_await_scan) {
      files = new Set(await recursive(target_dir));
      argv.target_files_scanned = true;
    } else {
      recursive(target_dir).then(d => {
        files = new Set(d);
        argv.target_files_scanned = true;
      });
    }
  }

  return stream.pipe(etl.map(argv.concurrency || 1, async d => {
    if (!d.filename) return;
    const Key = path.join(target_dir, d.filename);
    let skip = files.has(Key);
    if (!skip && !argv.target_files_scanned && !argv.target_overwrite) {
      try {
        await stat(Key);
        skip = true;
      } catch(e) {
        if (e.code !== 'ENOENT') throw e;
      }
    }
    if (skip) {
      argv.Σ_skipped += 1;
      return { message: 'skipping', Key };
    }

    if (filter_files && !filter_files.test(Key)) return { message: 'ignoring', Key };

    const Body = await bodyStream(d, argv);

    await ensureDir(path.dirname(Key));
    const tmpKey = `${Key}.download`;
    await new Promise( (resolve, reject) => {
      Body
        .on('error', reject)
        .pipe(createWriteStream(tmpKey))
        .on('close', async () => {
          await rename(tmpKey, Key);
          if (d.timestamp) {
            const timestamp = new Date(+d.timestamp);
            if (!isNaN(+timestamp)) await utimes(Key, timestamp, timestamp);
          }
          resolve();
        })
        .on('error', e => reject(e));
    });
    return { Key, message: 'OK' };

  }, {
    catch: function(e) {
      if (argv.throw) {
        this.emit('error', e);
        return;
      }
      console.error(e);
    }
  }));
};