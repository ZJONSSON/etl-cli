const express = require('express');
const tap = require('tap');

const app = express();
const { cli } = require('./util');

app.use(express.raw({ type: '*/*', limit: '10mb' }));
app.use((req, res) => {
  const body = typeof req.body == 'object' ? req.body : JSON.parse(req.body);
  const target = req.headers['x-amz-target'];

  if (target === 'AmazonAthena.StartQueryExecution') {


    res.json({
      QueryExecutionId: body.ClientRequestToken
    });
  } else if (target === 'AmazonAthena.GetQueryExecution') {
    res.json({
      QueryExecution: {
        QueryExecutionId: body.queryExecutionId,
        ResultConfiguration: {
          OutputLocation: 's3://test.results/results'
        },
        Status: {
          State: 'SUCCEEDED',
        }
      }
    });
  } else if (req.path === '/test.results/results') {
    res.end('a,b,c\n1,2,"[{d=3,e=4,f=[{e=5},{f=6}]}]"');
  }

});

const server = app.listen(3000);

tap.test('athena', async t => {
  const cmd = `etl athena/test/test test --source_endpoint="http://localhost:3000" --source_region="us-east-1" --source_forcePathStyle=true`;
  const res = await cli(cmd);
  console.log(JSON.stringify(res));
  t.same(res, { "Σ_in":1, "Σ_out":1, "data":[{ "a":"1", "b":"2", "c":[{ "d":3, "e":4, "f":[{ "e":5 }, { "f":6 }] }] }] });
  server.close();
});
