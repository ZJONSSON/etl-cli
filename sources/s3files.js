const etl = require('etl');
const AWS = require('aws-sdk');
const s3Source = require('./s3');

module.exports = function(argv) {
  argv = Object.assign({},argv);
  
  if (argv.target_accessKeyId) argv.accessKeyId = argv.target_accessKeyId;
  if (argv.target_secretAccessKey) argv.secretAccessKey = argv.target_secretAccessKey;

  const s3 = new AWS.S3(argv.source_config || argv);
  const Bucket = argv.source_bucket ||  argv.source_collection;
  const Prefix = argv.source_key ||  argv.source_indextype;
  if (!Bucket) throw 'S3 Bucket missing';
  if (!Prefix) throw 'S3 Prefix missing';

  const reFilter = RegExp(argv['filter-files']);

  return {
    stream: () => etl.toStream(async function() {
      let truncated = true;
      let query = {Bucket, Prefix};

      while (!argv.no_skip && truncated) {
        const res = await s3.listObjects(query).promise();

        res.Contents.forEach(d => {
          let params = {Bucket, Key: d.Key};
          if (reFilter.exec(d.Key)) this.push({
            bucket: Bucket,
            filename: d.Key,
            etag: d.ETag.replace(/"/g,''),
            getClient: () => s3,
            body: () => s3Source(params)
          });
        });

        truncated = res.IsTruncated;
        if (truncated) query.Marker = res.Contents.slice(-1)[0].Key;
      }
    })
  };
};