const etl = require('etl');
const path = require('path');
const { Upload } = require("@aws-sdk/lib-storage");
const { paginateListObjectsV2,
  S3Client,
  HeadObjectCommand
} = require('@aws-sdk/client-s3');
const { createConfig } = require('../util');

module.exports = async function(stream, argv) {
  const config = createConfig(argv.target_config, argv, 'target');
  const client = new S3Client(config);
  const Bucket = argv.target_params[0] || config.bucket;
  const Prefix = argv.target_params.slice(1).join('/') || config.prefix || '';

  if (!Bucket) throw 'S3 Bucket missing';


  const files = new Set([]);

  const query = { Bucket, Prefix };

  argv.target_files_scanned = false;

  if (!config.overwrite && !config.skip_scan) {
    // We scan in the background
    async function scan() {
      for await (const res of paginateListObjectsV2({ client }, query)) {
        res?.Contents?.forEach(d => {
          files.add(d.Key);
        });
      }
      argv.target_files_scanned = true;
    };
    if (config.await_scan) {
      await scan();
    } else {
      scan();
    }
  }

  return stream.pipe(etl.map(argv.concurrency || 1, async d => {
    const Key = path.join(Prefix, d.filename);

    let skip = files.has(Key);

    // If we haven't scanned all the files yet we check if the file
    // exists with HeadObjectCommand
    if (!skip && !argv.target_files_scanned && !config.overwrite) {
      const command = new HeadObjectCommand({ Bucket, Key });
      try {
        await client.send(command);
        skip = true;
      } catch(e) {
        if (e.name !== 'NotFound') {
          throw e;
        }
      }
    }

    if (skip) {
      argv.Σ_skipped += 1;
      return { message: 'skipping', Key };
    }
    const Body = await d.body();
    if (!Body) return { Key, message: 'No body' };
    const upload = new Upload({ client, params: { Bucket, Key, Body } });
    await upload.done();
    return { Key, message: 'OK' };
  }, {
    catch: function(e, d) {
      if (argv.throw) {
        this.emit('error', e);
      } else {
        console.error(e);
        this.write(d);
      }
    }
  }));
};