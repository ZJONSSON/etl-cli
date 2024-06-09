const tap = require('tap');
const { cli } = require('./util');
const { S3Client, CreateBucketCommand } = require("@aws-sdk/client-s3");

const Bucket = `testbucket${String(Math.random()).replace('.', '')}`;


const client = new S3Client({ endpoint: 'http://localhost:9090', forcePathStyle: true });
tap.before( () => client.send(new CreateBucketCommand({ Bucket })));


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

  t.test('downloading files from s3', async t => {
    const cmd = `etl s3files/${Bucket}/test test --silent --source_endpoint=http://localhost:9090 --source_forcePathStyle=true`;
    const res = await cli(cmd);
    const data = res.data;
    t.same(data[0].filename, 'subdirectory/fileB.txt');
    t.same(data[1].filename, 'subdirectory/folderA/fileA.txt');
    t.same( String(await data[0].buffer()), 'This is file B');
    t.same( String(await data[1].buffer()), 'This is file A');
  });
});