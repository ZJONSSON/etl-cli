const tap = require('tap');
const path = require('path');
const os = require('os');
const fs = require('fs');
const { cli } = require('./util');
const sftpServer = require('./support/sftpServer');

let server;
let TARGET;
let SOURCE;
let downloadDir;

tap.before(async () => {
  server = await sftpServer.start();
  TARGET = `--target_host=127.0.0.1 --target_port=${server.port} --target_username=testuser --target_password=testpass`;
  SOURCE = TARGET.replace(/target_/g, 'source_');
  downloadDir = fs.mkdtempSync(path.join(os.tmpdir(), 'sftp-dl-'));
});

tap.teardown(async () => {
  if (server) {
    await server.stop();
    fs.rmSync(server.root, { recursive: true, force: true });
  }
  if (downloadDir) fs.rmSync(downloadDir, { recursive: true, force: true });
});

tap.test('sftp', async t => {

  t.test('uploading files to sftp', async t => {
    const cmd = `etl files/${__dirname}/support/testfiles sftp/upload/sub ${TARGET}`;
    const res = await cli(cmd);
    t.same(res, { Σ_in: 2, Σ_out: 2 });

    t.equal(fs.readFileSync(path.join(server.root, 'upload/sub/fileB.txt'), 'utf8'), 'This is file B');
    t.equal(fs.readFileSync(path.join(server.root, 'upload/sub/folderA/fileA.txt'), 'utf8'), 'This is file A');
    t.notOk(fs.existsSync(path.join(server.root, 'upload/sub/fileB.txt.download')), 'no leftover .download');
  });

  t.test('uploading files again to sftp', async t => {
    const cmd = `etl files/${__dirname}/support/testfiles sftp/upload/sub ${TARGET}`;
    const res = await cli(cmd);
    t.same(res, { Σ_in: 2, Σ_out: 2, Σ_skipped: 2 }, 'skips uploading files that already exist');
  });

  t.test('uploading files via params', async t => {
    const cmd = `etl files/${__dirname}/support/testfiles sftp ${TARGET} --target_path=upload/sub`;
    const res = await cli(cmd);
    t.same(res, { Σ_in: 2, Σ_out: 2, Σ_skipped: 2 }, 'skips uploading files that already exist');
  });

  t.test('uploading files skipping scan', async t => {
    const cmd = `etl files/${__dirname}/support/testfiles sftp/upload/sub ${TARGET} --target_skip_scan=true`;
    const res = await cli(cmd);
    t.same(res.argv.target_files_scanned, false);
    t.same(res, { Σ_in: 2, Σ_out: 2, Σ_skipped: 2 }, 'individual exists check still skips');
  });

  t.test('uploading files awaiting scan', async t => {
    const cmd = `etl files/${__dirname}/support/testfiles sftp/upload/sub ${TARGET} --target_await_scan=true`;
    const res = await cli(cmd);
    t.same(res.argv.target_files_scanned, true);
    t.same(res, { Σ_in: 2, Σ_out: 2, Σ_skipped: 2 });
  });

  t.test('uploading files with target_overwrite', async t => {
    const cmd = `etl files/${__dirname}/support/testfiles sftp/upload/sub ${TARGET} --target_overwrite=true`;
    const res = await cli(cmd);
    t.same(res, { Σ_in: 2, Σ_out: 2 }, 'overwrites files that already exist');
  });

  t.test('downloading files from sftp', async t => {
    const dest = path.join(downloadDir, 'roundtrip');
    fs.mkdirSync(dest, { recursive: true });
    const cmd = `etl sftp/upload/sub files/${dest} ${SOURCE}`;
    const res = await cli(cmd);
    t.same(res, { Σ_in: 2, Σ_out: 2 });
    t.equal(fs.readFileSync(path.join(dest, 'fileB.txt'), 'utf8'), 'This is file B');
    t.equal(fs.readFileSync(path.join(dest, 'folderA/fileA.txt'), 'utf8'), 'This is file A');
  });

  t.test('downloading files via params', async t => {
    const dest = path.join(downloadDir, 'roundtrip-params');
    fs.mkdirSync(dest, { recursive: true });
    const cmd = `etl sftp files/${dest} ${SOURCE} --source_path=upload/sub`;
    const res = await cli(cmd);
    t.same(res, { Σ_in: 2, Σ_out: 2 });
  });
});
