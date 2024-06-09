const { createConfig } = require('../util');
const etl = require('etl');
const { Upload } = require("@aws-sdk/lib-storage");
const { S3Client } = require('@aws-sdk/client-s3');

module.exports = async function(stream, argv) {
  const config = createConfig(argv.target_config, argv, 'target');
  const client = new S3Client(config);
  const Bucket = argv.target_params[0] || config.bucket;
  const Key = config.target_key || argv.target_params.slice(1).join('/');
  if (!Bucket) throw 'S3 Bucket missing';
  const Body = stream
    .pipe(etl.map(function(d) {
      if (d instanceof Object && !(d instanceof Buffer)) {
        return JSON.stringify(d) + '\n';
      } else {
        return d;
      }
    }));
  const upload = new Upload({ client, params: { Bucket, Key, Body } });
  await upload.done();
};