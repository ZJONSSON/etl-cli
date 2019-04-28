const etl = require('etl');
const AWS = require('aws-sdk');

module.exports = function(stream,argv) {
  argv = Object.assign({},argv);
  if (argv.target_accessKeyId) argv.accessKeyId = argv.target_accessKeyId;
  if (argv.target_secretAccessKey) argv.secretAccessKey = argv.target_secretAccessKey;
  const s3 = new AWS.S3(argv);

  const Bucket = argv.target_bucket ||  argv.target_collection;
  const Prefix = argv.target_key ||  argv.target_indextype;
  if (!Bucket) throw 'S3 Bucket missing';
  if (!Prefix) throw 'S3 Prefix missing';

  let files = new Set([]);

  return etl.toStream(async () => {
    let truncated = true;
    let query = {Bucket, Prefix};

    while (!argv.no_skip && truncated) {
      const res = await s3.listObjects(query).promise();
      res.Contents.forEach(d => files.add(d.Key));
      truncated = res.IsTruncated;
      if (truncated) query.marker = res.Contents.slice(-1)[0].Key;
    }

    return stream;
  })
  .pipe(etl.map(argv.concurrency || 1, async d => {
    const Key = `${Prefix}/${d.filename}`;
    if (files.has(Key)) return {message: 'skipping', Key};

    const Body = typeof d.body === 'function' ? await d.body() : d.body;
    if (!Body) return {Key, message: 'No body'};
    await s3.upload({ Bucket, Key, Body }).promise();
    return {Key, message: 'OK'};

  },{
    catch: function(e,d) {
      console.error(e);
      this.write(d);
    }
  }));
    
};