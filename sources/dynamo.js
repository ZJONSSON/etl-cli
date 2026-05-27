const etl = require('etl');
const { createConfig } = require('../util');

module.exports = argv => {
  const { DynamoDBClient, ScanCommand, DescribeTableCommand } = require('@aws-sdk/client-dynamodb');
  const { unmarshall } = require('@aws-sdk/util-dynamodb');

  const table = argv.source_collection || argv.source_table;
  if (!table) throw 'source table missing (use dynamo/<table>)';

  const config = createConfig(argv.source_config, argv, 'source', ['region', 'endpoint']);
  const client = new DynamoDBClient(config);

  // source_indextype is the third path segment: dynamo/<table>/<gsi-name>
  const scanParams = { TableName: table };
  if (argv.source_indextype) scanParams.IndexName = argv.source_indextype;
  if (argv.source_query) Object.assign(scanParams, argv.source_query);

  return {
    recordCount: async () => {
      const res = await client.send(new DescribeTableCommand({ TableName: table }));
      return res.Table.ItemCount;
    },
    stream: () => {
      const out = etl.map(null, { keepAlive: true });

      const scan = async (ExclusiveStartKey) => {
        const res = await client.send(new ScanCommand({ ...scanParams, ExclusiveStartKey }));
        res.Items.forEach(item => out.push(unmarshall(item)));
        if (res.LastEvaluatedKey) return scan(res.LastEvaluatedKey);
        out.end();
      };

      scan().catch(e => { console.error('dynamo scan error:', e.message || e); out.end(); });
      return out;
    }
  };
};
