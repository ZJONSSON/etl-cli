const etl = require('etl');
const path = require('path');
const { createConfig } = require('../util');
const bodyStream = require('./lib/bodyStream');
const { connect, walk } = require('../sources/sftp');

async function ensureDir(sftp, dir, ensured) {
  if (ensured.has(dir) || dir === '.' || dir === '/' || dir === '') return;
  await ensureDir(sftp, path.posix.dirname(dir), ensured);
  await sftp.mkdir(dir).catch(() => {}); // ignore EEXIST
  ensured.add(dir);
}

module.exports = async function(stream, argv) {
  const config = createConfig(argv.target_config, argv, 'target');
  const rootPath = (argv.target_params && argv.target_params.join('/')) || config.path || '.';
  if (!config.host) throw 'SFTP host missing';

  const { conn, sftp, raw } = await connect(config);

  const files = new Set();
  argv.target_files_scanned = false;

  async function scan() {
    const queue = [rootPath];
    while (queue.length) {
      const dir = queue.shift();
      try {
        for await (const entry of walk(sftp, dir)) {
          const full = path.posix.join(dir, entry.filename);
          if (entry.attrs.isDirectory()) queue.push(full);
          else if (entry.attrs.isFile()) files.add(full);
        }
      } catch(e) { if (e.code !== 2) throw e; }
    }
    argv.target_files_scanned = true;
  }

  if (!config.overwrite && !config.skip_scan) {
    if (config.await_scan) await scan();
    else scan().catch(e => {
      argv.target_files_scanned = true;
      if (!argv.silent) console.warn('sftp scan failed', e.message);
    });
  }

  const ensured = new Set();

  return stream.pipe(etl.map(argv.concurrency || 1, async d => {
    if (!d.filename || !d.body) {
      argv.Σ_skipped += 1;
      argv.Σ_out -= 1;
      return;
    }
    const Key = path.posix.join(rootPath, d.filename);
    let skip = files.has(Key);
    if (!skip && !argv.target_files_scanned && !config.overwrite) {
      skip = await sftp.stat(Key).then(() => true, () => false);
    }
    if (skip) {
      argv.Σ_skipped += 1;
      return { message: 'skipping', Key };
    }

    const Body = await bodyStream(d, argv);
    await ensureDir(sftp, path.posix.dirname(Key), ensured);

    const tmpKey = `${Key}.download`;
    try {
      await new Promise((res, rej) => {
        const ws = raw.createWriteStream(tmpKey);
        ws.on('close', res).on('error', rej);
        Body.on('error', rej).pipe(ws);
      });
      await sftp.rename(tmpKey, Key);
    } catch(e) {
      await sftp.unlink(tmpKey).catch(() => {});
      throw e;
    }
    return { Key, message: 'OK' };
  }, {
    catch: function(e, d) {
      if (argv.throw) this.emit('error', e);
      else { console.error(e); this.write(d); }
    },
    flush: cb => { conn.end(); cb(); }
  }));
};
