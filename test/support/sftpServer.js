const fs = require('fs');
const path = require('path');
const os = require('os');
const ssh2 = require('ssh2');
const { Server } = ssh2;
const { generateKeyPairSync } = ssh2.utils;
const { OPEN_MODE, STATUS_CODE, flagsToString } = ssh2.utils.sftp;

function attrsFromStat(st) {
  return {
    mode: st.mode,
    uid: 0,
    gid: 0,
    size: st.size,
    atime: Math.floor(st.atimeMs / 1000),
    mtime: Math.floor(st.mtimeMs / 1000)
  };
}

module.exports.start = async function({ username = 'testuser', password = 'testpass' } = {}) {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'sftp-'));
  const { private: hostKey } = generateKeyPairSync('ed25519');

  const resolve = p => {
    const norm = path.posix.normalize('/' + String(p).replace(/^\.\/?/, ''));
    return path.join(root, norm);
  };

  const clients = new Set();
  const server = new Server({ hostKeys: [hostKey] }, client => {
    clients.add(client);
    client.on('close', () => clients.delete(client));
    client.on('authentication', ctx => {
      if (ctx.method === 'password' && ctx.username === username && ctx.password === password) ctx.accept();
      else ctx.reject(['password']);
    }).on('ready', () => {
      client.on('session', accept => {
        const session = accept();
        session.on('sftp', accept => {
          const sftp = accept();
          const handles = new Map();
          let nextId = 0;
          const newHandle = obj => {
            const id = nextId++;
            handles.set(id, obj);
            const buf = Buffer.alloc(4);
            buf.writeUInt32BE(id, 0);
            return buf;
          };
          const idOf = h => (h && h.length === 4) ? h.readUInt32BE(0) : -1;
          const errStatus = e => {
            if (!e) return STATUS_CODE.FAILURE;
            if (e.code === 'ENOENT') return STATUS_CODE.NO_SUCH_FILE;
            if (e.code === 'EACCES' || e.code === 'EPERM') return STATUS_CODE.PERMISSION_DENIED;
            return STATUS_CODE.FAILURE;
          };

          sftp.on('OPEN', (reqid, filename, flags) => {
            try {
              const mode = flagsToString(flags) || 'r';
              const fd = fs.openSync(resolve(filename), mode);
              sftp.handle(reqid, newHandle({ type: 'file', fd, path: resolve(filename) }));
            } catch(e) {
              sftp.status(reqid, errStatus(e));
            }
          });

          sftp.on('READ', (reqid, handle, offset, length) => {
            const h = handles.get(idOf(handle));
            if (!h || h.type !== 'file') return sftp.status(reqid, STATUS_CODE.FAILURE);
            try {
              const buf = Buffer.alloc(length);
              const n = fs.readSync(h.fd, buf, 0, length, Number(offset));
              if (!n) sftp.status(reqid, STATUS_CODE.EOF);
              else sftp.data(reqid, buf.slice(0, n));
            } catch(e) {
              sftp.status(reqid, errStatus(e));
            }
          });

          sftp.on('WRITE', (reqid, handle, offset, data) => {
            const h = handles.get(idOf(handle));
            if (!h || h.type !== 'file') return sftp.status(reqid, STATUS_CODE.FAILURE);
            try {
              fs.writeSync(h.fd, data, 0, data.length, Number(offset));
              sftp.status(reqid, STATUS_CODE.OK);
            } catch(e) {
              sftp.status(reqid, errStatus(e));
            }
          });

          sftp.on('FSTAT', (reqid, handle) => {
            const h = handles.get(idOf(handle));
            if (!h) return sftp.status(reqid, STATUS_CODE.FAILURE);
            try {
              const st = h.type === 'file' ? fs.fstatSync(h.fd) : fs.statSync(h.path);
              sftp.attrs(reqid, attrsFromStat(st));
            } catch(e) {
              sftp.status(reqid, errStatus(e));
            }
          });

          const statHandler = (reqid, p, useLstat) => {
            try {
              const st = useLstat ? fs.lstatSync(resolve(p)) : fs.statSync(resolve(p));
              sftp.attrs(reqid, attrsFromStat(st));
            } catch(e) {
              sftp.status(reqid, errStatus(e));
            }
          };
          sftp.on('STAT', (reqid, p) => statHandler(reqid, p, false));
          sftp.on('LSTAT', (reqid, p) => statHandler(reqid, p, true));

          sftp.on('OPENDIR', (reqid, p) => {
            try {
              const fullPath = resolve(p);
              const entries = fs.readdirSync(fullPath);
              sftp.handle(reqid, newHandle({ type: 'dir', path: fullPath, entries, pos: 0 }));
            } catch(e) {
              sftp.status(reqid, errStatus(e));
            }
          });

          sftp.on('READDIR', (reqid, handle) => {
            const h = handles.get(idOf(handle));
            if (!h || h.type !== 'dir') return sftp.status(reqid, STATUS_CODE.FAILURE);
            if (h.pos >= h.entries.length) return sftp.status(reqid, STATUS_CODE.EOF);
            const slice = h.entries.slice(h.pos, h.pos + 50);
            const list = [];
            for (const name of slice) {
              try {
                const st = fs.lstatSync(path.join(h.path, name));
                list.push({
                  filename: name,
                  longname: `${st.isDirectory() ? 'd' : '-'}rw-r--r-- 1 user user ${st.size} Jan 1 00:00 ${name}`,
                  attrs: attrsFromStat(st)
                });
              } catch(_e) { /* skip vanished entry */ }
            }
            h.pos += slice.length;
            sftp.name(reqid, list);
          });

          sftp.on('CLOSE', (reqid, handle) => {
            const h = handles.get(idOf(handle));
            if (h && h.type === 'file') {
              try { fs.closeSync(h.fd); } catch(_e) { /* already closed */ }
            }
            handles.delete(idOf(handle));
            sftp.status(reqid, STATUS_CODE.OK);
          });

          sftp.on('REALPATH', (reqid, p) => {
            const norm = path.posix.normalize('/' + String(p).replace(/^\.\/?/, ''));
            sftp.name(reqid, [{ filename: norm, longname: norm, attrs: {} }]);
          });

          sftp.on('MKDIR', (reqid, p) => {
            try {
              fs.mkdirSync(resolve(p));
              sftp.status(reqid, STATUS_CODE.OK);
            } catch(e) {
              if (e.code === 'EEXIST') sftp.status(reqid, STATUS_CODE.FAILURE);
              else sftp.status(reqid, errStatus(e));
            }
          });

          sftp.on('RENAME', (reqid, from, to) => {
            try {
              fs.renameSync(resolve(from), resolve(to));
              sftp.status(reqid, STATUS_CODE.OK);
            } catch(e) {
              sftp.status(reqid, errStatus(e));
            }
          });

          sftp.on('REMOVE', (reqid, p) => {
            try {
              fs.unlinkSync(resolve(p));
              sftp.status(reqid, STATUS_CODE.OK);
            } catch(e) {
              sftp.status(reqid, errStatus(e));
            }
          });
        });
      });
    }).on('error', () => {});
  });

  await new Promise(r => server.listen(0, '127.0.0.1', r));

  return {
    port: server.address().port,
    root,
    stop: () => new Promise(r => {
      for (const c of clients) {
        try { c.end(); } catch(_e) { /* already gone */ }
      }
      server.close(() => r());
    })
  };
};

// Silence OPEN_MODE unused-warning if linter ever cares
void OPEN_MODE;
