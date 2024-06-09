const etl = require('etl');
const { createConfig } = require('../util');
const { S3Client, GetObjectCommand } = require('@aws-sdk/client-s3');
const { Readable } = require('stream');

module.exports = async argv => {
  const config = createConfig(argv.source_config, argv, 'source');
  const client = new S3Client(config);
  const Bucket = argv.source_params[0] || config.bucket;
  const Key = config.source_key || argv.source_params.slice(1).join('/');

  const cmd = new GetObjectCommand({ Bucket, Key });
  const item = await client.send(cmd);

  if (!(item.Body instanceof Readable)) {
    throw 'The response body is not a stream';
  }
  return item.Body
    .pipe(etl.chain(inbound => {
      if (argv.source_format === 'raw') {
        return inbound;
      } else {
        return inbound
          .pipe(etl.split())
          .pipe(etl.map(d => JSON.parse(d.text)));
      }
    }));
};