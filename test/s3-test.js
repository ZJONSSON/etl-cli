const tap = require('tap');
const { cli } = require('./util');
const { S3Client, CreateBucketCommand } = require("@aws-sdk/client-s3");

const Bucket = `testbucket${String(Math.random()).replace('.', '')}`;

process.env.AWS_ACCESS_KEY_ID = 'xxxxxxxxxxxx';
process.env.AWS_SECRET_ACCESS_KEY = 'xxxxxxxxxxxx';
process.env.AWS_REGION = 'us-east-1';

const client = new S3Client({
  endpoint: 'http://localhost:9090',
  forcePathStyle: true,
});

tap.before(async() => {
  let bucketCreated;
  while (!bucketCreated) {
    try {
      bucketCreated = await client.send(new CreateBucketCommand({ Bucket }));
    } catch(e) {
      // waitport does not work here as s3mock opens port 9090
      // before it's actually ready to accept s3 commands
      await new Promise(resolve => setTimeout(resolve, 1000));
    }
  }

});


tap.test('s3files', async t => {

  t.test('uploading files to s3', async t => {
    const cmd = `etl files/${__dirname}/support/testfiles s3files/${Bucket}/test/subdirectory --target_endpoint=http://localhost:9090 --target_forcePathStyle=true`;
    const res = await cli(cmd);
    t.same(res, { Σ_in: 2, Σ_out: 2 });
  });

  t.test('uploading files again to s3', async t => {
    const cmd = `etl files/${__dirname}/support/testfiles s3files/${Bucket}/test/subdirectory --target_endpoint=http://localhost:9090 --target_forcePathStyle=true`;
    const res = await cli(cmd);
    t.same(res, { Σ_in: 2, Σ_out: 2, Σ_skipped: 2 }, 'skips uploading files that already exist');
  });


  t.test('uploading files again to s3 via params', async t => {
    const cmd = `etl files/${__dirname}/support/testfiles s3files --target_bucket=${Bucket} --target_prefix="test/subdirectory" --target_endpoint=http://localhost:9090 --target_forcePathStyle=true`;
    const res = await cli(cmd);
    t.same(res, { Σ_in: 2, Σ_out: 2, Σ_skipped: 2 }, 'skips uploading files that already exist');
  });

  t.test('uploading files again to s3 skipping scan', async t => {
    const cmd = `etl files/${__dirname}/support/testfiles s3files/${Bucket}/test/subdirectory --target_endpoint=http://localhost:9090 --target_forcePathStyle=true --target_skip_scan=true`;
    const res = await cli(cmd);
    t.same(res.argv.target_files_scanned, false);
    t.same(res, { Σ_in: 2, Σ_out: 2, Σ_skipped: 2 }, 'skips uploading files that already exist');
  });

  t.test('uploading files again to s3 awaiting scan', async t => {
    const cmd = `etl files/${__dirname}/support/testfiles s3files/${Bucket}/test/subdirectory --target_endpoint=http://localhost:9090 --target_forcePathStyle=true --target_await_scan=true`;
    const res = await cli(cmd);
    t.same(res.argv.target_files_scanned, true);
    t.same(res, { Σ_in: 2, Σ_out: 2, Σ_skipped: 2 }, 'skips uploading files that already exist');
  });

  t.test('uploading files again to s3 with target_overwrite', async t => {
    const cmd = `etl files/${__dirname}/support/testfiles s3files/${Bucket}/test/subdirectory --target_endpoint=http://localhost:9090 --target_forcePathStyle=true --target_overwrite=true`;
    const res = await cli(cmd);
    t.same(res, { Σ_in: 2, Σ_out: 2 }, 'overwrite files that already exist');
  });

  t.test('downloading files from s3', async t => {
    const cmd = `etl s3://${Bucket}/test test --silent --source_endpoint=http://localhost:9090 --source_forcePathStyle=true`;
    const res = await cli(cmd);
    const data = res.data;
    t.same(data[0].filename, 'subdirectory/fileB.txt');
    t.same(data[1].filename, 'subdirectory/folderA/fileA.txt');
    t.same( String(await data[0].buffer()), 'This is file B');
    t.same( String(await data[1].buffer()), 'This is file A');
  });

  t.test('uploading files again to s3 with target_gzip', async t => {
    const cmd = `etl files/${__dirname}/support/testfiles s3files/${Bucket}/test/gzip --target_endpoint=http://localhost:9090 --target_forcePathStyle=true --target_gzip=true`;
    const res = await cli(cmd);
    t.same(res, { Σ_in: 2, Σ_out: 2 }, 'overwrite files that already exist');
  });

  t.test('reading gzipped files from s3', async t => {
    const cmd = `etl s3://${Bucket}/test/gzip test --silent --source_endpoint=http://localhost:9090 --source_forcePathStyle=true`;
    const res = await cli(cmd);
    const buffer = await res.data[0].buffer();
    const text = require('zlib').gunzipSync(buffer).toString();
    t.same( res.data[0].filename, 'fileB.txt.gz');
    t.same( text, 'This is file B');
  });

  t.test('writing bodies that are either string, readable, async string or async readable', async t => {
    const cmd = `etl ${__dirname}/support/bodies.js s3files/${Bucket}/test/bodies --target_endpoint=http://localhost:9090 --target_forcePathStyle=true --target_gzip=true`;
    const res = await cli(cmd);
    t.same(res, { 'Σ_in': 10, 'Σ_out': 9, 'Σ_skipped': 1 });
  });
});