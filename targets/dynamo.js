const etl = require('etl');
const { createConfig } = require('../util');

module.exports = async (stream, argv) => {
  const { DynamoDBClient, PutItemCommand, BatchWriteItemCommand } = require('@aws-sdk/client-dynamodb');
  const { marshall } = require('@aws-sdk/util-dynamodb');

  const table = argv.target_collection || argv.target_table;
  if (!table) throw 'target table missing (use dynamo/<table>)';

  const config = createConfig(argv.target_config, argv, 'target', ['region', 'endpoint']);
  const client = new DynamoDBClient(config);
  const concurrency = argv.target_concurrency || 5;

  // When --collect is set, output.js already batches records into arrays before calling us
  if (argv.collect) {
    return stream.pipe(etl.map(concurrency, async items => {
      const batch = [].concat(items);
      for (let i = 0; i < batch.length; i += 25) {
        await client.send(new BatchWriteItemCommand({
          RequestItems: {
            [table]: batch.slice(i, i + 25).map(item => ({
              PutRequest: { Item: marshall(item, { removeUndefinedValues: true }) }
            }))
          }
        }));
      }
    }));
  }

  return stream.pipe(etl.map(concurrency, async item => {
    await client.send(new PutItemCommand({
      TableName: table,
      Item: marshall(item, { removeUndefinedValues: true })
    }));
  }));
};
