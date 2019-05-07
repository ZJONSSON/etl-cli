const etl = require('etl');
const AWS = require('aws-sdk');

module.exports = function(argv) {
  argv = Object.assign({},argv);
  if (argv.target_accessKeyId) argv.accessKeyId = argv.target_accessKeyId;
  if (argv.target_secretAccessKey) argv.secretAccessKey = argv.target_secretAccessKey;

  const s3 = new AWS.S3(argv);
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
          if (reFilter.exec(d.Key)) this.push(d.Key);
        });

        truncated = res.IsTruncated;
        if (truncated) query.Marker = res.Contents.slice(-1)[0].Key;
      }
    })
    .pipe(etl.map(async filename => ({
      filename,
      body: () => s3.getObject({Bucket, Key: filename}).createReadStream()
    })))
  };
};