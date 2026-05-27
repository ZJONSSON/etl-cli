const tap = require('tap');
const { cli } = require('./util');
const { DynamoDBClient, CreateTableCommand, DeleteTableCommand, waitUntilTableExists, waitUntilTableNotExists } = require('@aws-sdk/client-dynamodb');

const ENDPOINT = 'http://localhost:8000';

process.env.AWS_ACCESS_KEY_ID = 'local';
process.env.AWS_SECRET_ACCESS_KEY = 'local';
process.env.AWS_REGION = 'us-east-1';

const client = new DynamoDBClient({ endpoint: ENDPOINT, region: 'us-east-1' });

tap.before(async () => {
  while (true) {
    try {
      await client.send(new DeleteTableCommand({ TableName: 'test_table' }));
      await waitUntilTableNotExists({ client, maxWaitTime: 30 }, { TableName: 'test_table' });
    } catch(e) {
      if (e.name !== 'ResourceNotFoundException') {
        await new Promise(r => setTimeout(r, 1000));
        continue;
      }
    }
    try {
      await client.send(new CreateTableCommand({
        TableName: 'test_table',
        AttributeDefinitions: [{ AttributeName: 'a', AttributeType: 'S' }],
        KeySchema: [{ AttributeName: 'a', KeyType: 'HASH' }],
        BillingMode: 'PAY_PER_REQUEST',
      }));
      await waitUntilTableExists({ client, maxWaitTime: 30 }, { TableName: 'test_table' });
      break;
    } catch(e) {
      await new Promise(r => setTimeout(r, 1000));
    }
  }
});

tap.test('dynamodb', async t => {
  t.test('writing data', async t => {
    const cmd = `etl ${__dirname}/support/test.csv dynamo/test_table --target_endpoint=${ENDPOINT} --target_region=us-east-1`;
    const res = await cli(cmd);
    t.same(res, { Σ_in: 2, Σ_out: 2 });
  });

  t.test('reading data', async t => {
    const cmd = `etl dynamo/test_table test --source_endpoint=${ENDPOINT} --source_region=us-east-1`;
    const res = await cli(cmd);
    const data = res.data.sort((a, b) => a.a > b.a ? 1 : -1);
    const expected = require('./support/test_collect.json');
    t.same(data, expected);
  });

  t.test('writing data with --collect', async t => {
    const cmd = `etl ${__dirname}/support/test.csv dynamo/test_table --target_endpoint=${ENDPOINT} --target_region=us-east-1 --collect=25`;
    const res = await cli(cmd);
    t.same(res, { Σ_in: 2, Σ_out: 2 });
  });
});
