const AWS = require('aws-sdk');
const etl = require('etl');

module.exports = argv => {
  argv = Object.assign({},argv);
  argv.accessKeyId = argv.source_accessKeyId;
  argv.secretAccessKey = argv.source_secretAccessKey;

  argv.source_bucket = argv.source_bucket ||  argv.source_collection;
  argv.source_key = argv.source_key ||  argv.source_indextype;

  const s3 = new AWS.S3(argv.source_config || argv);

  return () => s3.getObject({
    Bucket : argv.source_bucket,
    Key: argv.source_key
  })
  .createReadStream()
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