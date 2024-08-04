const tap = require('tap');
const { cli } = require('./util');
const etl = require('etl');
const path = require('path');
const tmpDir = path.resolve(require('os').tmpdir(), String(Math.random()).replace('.', ''));
const fs = require('fs');

tap.before(async () => {
  await fs.promises.mkdir(tmpDir);
  await fs.promises.mkdir(path.resolve(tmpDir, 'testfiles'));
});

tap.after(async () => {
  await fs.promises.rm(tmpDir, { recursive: true });
});

tap.test('files', async t => {

  t.test('files body', async () => {
    const cmd = `etl files/${__dirname}/support/testfiles test --silent`;
    const res = await cli(cmd);
    const data = res.data;

    function getBuffer(body) {
      return new Promise((resolve) => {
        body(true).pipe(etl.map(Object)).promise().then(data => {
          resolve(String(Buffer.concat(data)));
        });
      });
    }
    t.same(data[0].filename, 'fileB.txt');
    t.same(data[1].filename, 'folderA/fileA.txt');
    t.same( await getBuffer(data[0].body), 'This is file B');
    t.same( await getBuffer(data[1].body), 'This is file A');
  });

  t.test('files buffer', async () => {
    const cmd = `etl files/${__dirname}/support/testfiles test --silent`;
    const res = await cli(cmd);
    const data = res.data;
    t.same(data[0].filename, 'fileB.txt');
    t.same(data[1].filename, 'folderA/fileA.txt');
    t.same( String(await data[0].buffer()), 'This is file B');
    t.same( String(await data[1].buffer()), 'This is file A');
  });

  t.test('writing files', async () => {
    const cmd = `etl files/${__dirname}/support/testfiles files/${tmpDir}/testfiles/ --silent`;
    const res = await cli(cmd);
    t.same(res, { 'Σ_in': 2, 'Σ_out': 2 });
  });

  t.test('writing files again', async () => {
    const cmd = `etl files/${__dirname}/support/testfiles files/${tmpDir}/testfiles/ --silent`;
    const res = await cli(cmd);
    t.same(res, { 'Σ_in': 2, 'Σ_out': 2, Σ_skipped: 2 });
  });

  t.test('writing files again via params', async () => {
    const cmd = `etl files --source_dir=${__dirname}/support/testfiles files --target_dir=${tmpDir}/testfiles/ --silent`;
    const res = await cli(cmd);
    t.same(res, { 'Σ_in': 2, 'Σ_out': 2, Σ_skipped: 2 });
  });

  t.test('writing files skipping scan', async () => {
    const cmd = `etl files/${__dirname}/support/testfiles files/${tmpDir}/testfiles/ --silent --target_skip_scan=true`;
    const res = await cli(cmd);
    t.same(res.argv.target_files_scanned, false);
    t.same(res, { 'Σ_in': 2, 'Σ_out': 2, Σ_skipped: 2 });
  });

  t.test('writing files await scan', async () => {
    const cmd = `etl files/${__dirname}/support/testfiles files/${tmpDir}/testfiles/ --silent --target_await_scan=true`;
    const res = await cli(cmd);
    t.same(res.argv.target_files_scanned, true);
    t.same(res, { 'Σ_in': 2, 'Σ_out': 2, Σ_skipped: 2 });
  });

  t.test('writing files with overwrite', async () => {
    const cmd = `etl files/${__dirname}/support/testfiles files/${tmpDir}/testfiles/ --silent --target_overwrite=true`;
    const res = await cli(cmd);
    t.same(res.argv.target_files_scanned, false);
    t.same(res, { 'Σ_in': 2, 'Σ_out': 2 });
  });

  t.test('reading them back', async () => {
    const cmd = `etl files/${tmpDir}/testfiles test --silent`;
    const res = await cli(cmd);
    const data = res.data;
    t.same(data[0].filename, 'fileB.txt');
    t.same(data[1].filename, 'folderA/fileA.txt');
    t.same( String(await data[0].buffer()), 'This is file B');
    t.same( String(await data[1].buffer()), 'This is file A');
  });

  t.test('writing files with target_gzip', async () => {
    const cmd = `etl files/${__dirname}/support/testfiles files/${tmpDir}/testfiles/ --silent --target_gzip=true`;
    const res = await cli(cmd);
    t.same(res, { 'Σ_in': 2, 'Σ_out': 2 });

    // check one of the files to ensure it is gzipped
    const buffer = fs.readFileSync(path.resolve(tmpDir, 'testfiles', 'fileB.txt.gz'));
    const text = require('zlib').gunzipSync(buffer).toString();
    t.same(text, 'This is file B');
  });
});