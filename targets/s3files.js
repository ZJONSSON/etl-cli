const etl = require('etl');
const path = require('path');
const { Upload } = require("@aws-sdk/lib-storage");
const { paginateListObjectsV2,
  S3Client
} = require('@aws-sdk/client-s3');
const { createConfig } = require('../util');


module.exports = async function(stream, argv) {
  const config = createConfig(argv.target_config, argv, 'target');
  const client = new S3Client(config);
  const Bucket = argv.target_params[0] || config.bucket;
  const Prefix = argv.target_params.slice(1).join('/') || config.prefix || '';
  //const reFilter = RegExp(config.filter);
  if (!Bucket) throw 'S3 Bucket missing';


  const files = new Set([]);

  const query = { Bucket, Prefix };

  if (!config.overwrite) {
    for await (const res of paginateListObjectsV2({ client }, query)) {
      res?.Contents?.forEach(d => {
        files.add(d.Key);
      });
    }
  }

  return stream.pipe(etl.map(argv.concurrency || 1, async d => {
    const Key = path.join(Prefix, d.filename);
    if (files.has(Key)) {
      argv.Σ_skipped += 1;
      return { message: 'skipping', Key };
    }
    const Body = typeof d.body === 'function' ? await d.body() : d.body;
    const upload = new Upload({ client, params: { Bucket, Key, Body } });
    await upload.done();
    return { Key, message: 'OK' };
  }, {
    catch: function(e, d) {
      console.error(e);
      this.write(d);
    }
  }));
};