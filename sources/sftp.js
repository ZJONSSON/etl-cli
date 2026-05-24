const etl = require('etl');
const fs = require('fs');
const path = require('path');
const { promisify } = require('util');
const { Client } = require('ssh2');
const { createConfig } = require('../util');

function asyncify(sftp) {
  return new Proxy(sftp, {
    get: (t, k) => typeof t[k] === 'function' ? promisify(t[k].bind(t)) : t[k]
  });
}

function connect(config) {
  const opts = { port: 22, ...config };
  if (opts.privateKeyFile) opts.privateKey = fs.readFileSync(opts.privateKeyFile);
  return new Promise((resolve, reject) => {
    const conn = new Client();
    conn.once('ready', () => conn.sftp((err, sftp) => err ? reject(err) : resolve({ conn, sftp: asyncify(sftp), raw: sftp })))
      .once('error', reject)
      .connect(opts);
  });
}

async function* walk(sftp, dir) {
  const handle = await sftp.opendir(dir);
  try {
    while (true) {
      try { yield* await sftp.readdir(handle); }
      catch(e) { if (e.code === 1) return; throw e; }
    }
  } finally {
    sftp.close(handle).catch(() => {});
  }
}

module.exports = function(argv) {
  const config = createConfig(argv.source_config, argv, 'source');
  const rootPath = (argv.source_params && argv.source_params.join('/')) || config.path || '.';
  const reFilter = RegExp(config.filter || '');
  if (!config.host) throw 'SFTP host missing';

  return {
    stream: () => etl.toStream(async function() {
      const { sftp, raw } = await connect(config);
      const queue = [rootPath];
      while (queue.length) {
        const dir = queue.shift();
        for await (const entry of walk(sftp, dir)) {
          const full = path.posix.join(dir, entry.filename);
          if (entry.attrs.isDirectory()) queue.push(full);
          else if (entry.attrs.isFile() && reFilter.exec(full)) {
            this.push({
              filename: path.posix.relative(rootPath, full),
              size: entry.attrs.size,
              timestamp: entry.attrs.mtime ? entry.attrs.mtime * 1000 : undefined,
              body: async () => raw.createReadStream(full)
            });
          }
        }
      }
    })
  };
};

module.exports.connect = connect;
module.exports.walk = walk;
