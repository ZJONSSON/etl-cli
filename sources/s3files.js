const etl = require('etl');
const { paginateListObjectsV2,
  S3Client,
  GetObjectCommand
} = require('@aws-sdk/client-s3');
const { createConfig } = require('../util');

module.exports = function(argv) {
  const config = createConfig(argv.source_config, argv, 'source');
  const client = new S3Client(config);
  const Bucket = argv.source_params[0] || config.bucket;
  const Prefix = argv.source_params.slice(1).join('/') || config.prefix || '';
  const reFilter = RegExp(config.filter);
  if (!Bucket) throw 'S3 Bucket missing';

  return {
    stream: () => etl.toStream(async function() {
      const query = { Bucket, Prefix };
      for await (const res of paginateListObjectsV2({ client }, query)) {
        res?.Contents?.forEach(d => {
          if (reFilter.exec(d.Key)) this.push({
            bucket: Bucket,
            filename: d.Key.replace(Prefix, '').replace(/^([/]+)/, ''),
            etag: d.ETag.replace(/"/g, ''),
            lastModified: d.LastModified,
            size: d.Size,
            getClient: () => client,
            body: async () => {
              const cmd = new GetObjectCommand({ Bucket, Key: d.Key });
              const item = await client.send(cmd);
              return item.Body;
            }
          });
        });
      }
    })
  };
};