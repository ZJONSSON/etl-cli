const tap = require('tap');
const { cli } = require('./util');
const path = require('path');
const tmpDir = path.resolve(require('os').tmpdir(), String(Math.random()).replace('.', ''));
const fs = require('fs');

tap.before(async () => {
  await fs.promises.mkdir(tmpDir);
  await fs.promises.mkdir(path.resolve(tmpDir, 'testfiles'));
  await fs.promises.mkdir(path.resolve(tmpDir, 'jsonfiles'));
  await fs.promises.writeFile(path.resolve(tmpDir, 'jsonfiles', 'data.json'), JSON.stringify({ a: 1, b: 2 }));
});

tap.after(async () => {
  await fs.promises.rm(tmpDir, { recursive: true });
});

tap.test('files', async t => {

  t.test('files body', async () => {
    const cmd = `etl files/${__dirname}/support/testfiles test`;
    const res = await cli(cmd);
    const data = res.data;

    const fileB = data.find(d => d.filename === 'fileB.txt');
    const fileA = data.find(d => d.filename === 'folderA/fileA.txt');
    t.same(fileB.filename, 'fileB.txt');
    t.same(fileA.filename, 'folderA/fileA.txt');
    t.same( String(await fileB.buffer()), 'This is file B');
    t.same( String(await fileA.buffer()), 'This is file A');
  });

  t.test('files buffer', async () => {
    const cmd = `etl files/${__dirname}/support/testfiles test`;
    const res = await cli(cmd);
    const data = res.data;
    const fileB = data.find(d => d.filename === 'fileB.txt');
    const fileA = data.find(d => d.filename === 'folderA/fileA.txt');
    t.same(fileB.filename, 'fileB.txt');
    t.same(fileA.filename, 'folderA/fileA.txt');
    t.same( String(await fileB.buffer()), 'This is file B');
    t.same( String(await fileA.buffer()), 'This is file A');
  });

  t.test('files json', async () => {
    const cmd = `etl files/${tmpDir}/jsonfiles test`;
    const res = await cli(cmd);
    const data = res.data;
    const jsonFile = data.find(d => d.filename === 'data.json');
    t.same(await jsonFile.json(), { a: 1, b: 2 });
  });

  t.test('writing files', async () => {
    const cmd = `etl files/${__dirname}/support/testfiles files/${tmpDir}/testfiles/`;
    const res = await cli(cmd);
    t.same(res, { 'Σ_in': 2, 'Σ_out': 2 });
  });

  t.test('writing files again', async () => {
    const cmd = `etl files/${__dirname}/support/testfiles files/${tmpDir}/testfiles/`;
    const res = await cli(cmd);
    t.same(res, { 'Σ_in': 2, 'Σ_out': 2, Σ_skipped: 2 });
  });

  t.test('writing files again via params', async () => {
    const cmd = `etl files --source_dir=${__dirname}/support/testfiles files --target_dir=${tmpDir}/testfiles/`;
    const res = await cli(cmd);
    t.same(res, { 'Σ_in': 2, 'Σ_out': 2, Σ_skipped: 2 });
  });

  t.test('writing files skipping scan', async () => {
    const cmd = `etl files/${__dirname}/support/testfiles files/${tmpDir}/testfiles/ --target_skip_scan=true`;
    const res = await cli(cmd);
    t.same(res.argv.target_files_scanned, false);
    t.same(res, { 'Σ_in': 2, 'Σ_out': 2, Σ_skipped: 2 });
  });

  t.test('writing files await scan', async () => {
    const cmd = `etl files/${__dirname}/support/testfiles files/${tmpDir}/testfiles/ --target_await_scan=true`;
    const res = await cli(cmd);
    t.same(res.argv.target_files_scanned, true);
    t.same(res, { 'Σ_in': 2, 'Σ_out': 2, Σ_skipped: 2 });
  });

  t.test('writing files with overwrite', async () => {
    const cmd = `etl files/${__dirname}/support/testfiles files/${tmpDir}/testfiles/ --target_overwrite=true`;
    const res = await cli(cmd);
    t.same(res.argv.target_files_scanned, false);
    t.same(res, { 'Σ_in': 2, 'Σ_out': 2 });
  });

  t.test('reading them back', async () => {
    const cmd = `etl files/${tmpDir}/testfiles test`;
    const res = await cli(cmd);
    const data = res.data;
    const fileB = data.find(d => d.filename === 'fileB.txt');
    const fileA = data.find(d => d.filename === 'folderA/fileA.txt');
    t.same(fileB.filename, 'fileB.txt');
    t.same(fileA.filename, 'folderA/fileA.txt');
    t.same( String(await fileB.buffer()), 'This is file B');
    t.same( String(await fileA.buffer()), 'This is file A');
  });

  t.test('writing files with target_gzip', async () => {
    const cmd = `etl files/${__dirname}/support/testfiles files/${tmpDir}/testfiles/ --target_gzip=true`;
    const res = await cli(cmd);
    t.same(res, { 'Σ_in': 2, 'Σ_out': 2 });

    // check one of the files to ensure it is gzipped
    const buffer = fs.readFileSync(path.resolve(tmpDir, 'testfiles', 'fileB.txt.gz'));
    const text = require('zlib').gunzipSync(buffer).toString();
    t.same(text, 'This is file B');
  });

  t.test('writing bodies that are either string, readable, async string or async readable', async () => {
    const cmd = `etl ${__dirname}/support/bodies.js files/${tmpDir}/testfiles/`;
    const res = await cli(cmd);
    t.same(res, { 'Σ_in': 10, 'Σ_out': 8, 'Σ_skipped': 2 });
    console.log(tmpDir);
  });

  t.test('writing gzip bodies that are either string, readable, async string or async readable', async () => {
    const cmd = `etl ${__dirname}/support/bodies.js files/${tmpDir}/testfiles/ --target_gzip=true`;
    const res = await cli(cmd);
    t.same(res, { 'Σ_in': 10, 'Σ_out': 8, 'Σ_skipped': 2 });
  });

  t.test('reading .gz file, transforming, and writing as .gz', async () => {
    const zlib = require('zlib');

    // Create a gzipped JSON input file
    const inputData = { original: true, value: 100, items: ['a', 'b', 'c'] };
    const gzipInputDir = path.resolve(tmpDir, 'gzip-input');
    await fs.promises.mkdir(gzipInputDir, { recursive: true });
    const inputFile = path.resolve(gzipInputDir, 'data.json.gz');
    await fs.promises.writeFile(inputFile, zlib.gzipSync(JSON.stringify(inputData)));

    // Create transform that adds random data
    const transformFile = path.resolve(tmpDir, 'add-random.js');
    await fs.promises.writeFile(transformFile, `
module.exports = async function(d) {
  const data = await d.json();
  data.transformed = true;
  data.random = Math.random();
  data.timestamp = Date.now();
  return {
    filename: d.filename,
    body: JSON.stringify(data)
  };
};
`);

    // Run ETL: .gz input → transform → .gz output
    const gzipOutputDir = path.resolve(tmpDir, 'gzip-output');
    await fs.promises.mkdir(gzipOutputDir, { recursive: true });
    const cmd = `etl files/${gzipInputDir} files/${gzipOutputDir} --transform=${transformFile} --target_gzip=true`;
    const res = await cli(cmd);

    t.same(res, { 'Σ_in': 1, 'Σ_out': 1 });

    // Verify output file exists and is gzipped
    const outputFile = path.resolve(gzipOutputDir, 'data.json.gz');
    t.ok(fs.existsSync(outputFile), 'output file exists');

    const outputBuffer = fs.readFileSync(outputFile);

    // Check gzip magic bytes
    t.same(outputBuffer[0], 0x1f, 'first byte is gzip magic number');
    t.same(outputBuffer[1], 0x8b, 'second byte is gzip magic number');

    // Decompress and verify transformation
    const decompressed = zlib.gunzipSync(outputBuffer).toString();
    const outputData = JSON.parse(decompressed);

    t.same(outputData.original, true, 'original field preserved');
    t.same(outputData.value, 100, 'value field preserved');
    t.same(outputData.transformed, true, 'transformed flag added');
    t.ok(typeof outputData.random === 'number', 'random field is a number');
    t.ok(typeof outputData.timestamp === 'number', 'timestamp field is a number');
    t.same(outputData.items, ['a', 'b', 'c'], 'items array preserved');
  });


});