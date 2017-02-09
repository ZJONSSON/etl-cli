Command line tool for etl pipelines.

Install globally to have `etl` available on the command line:
```
npm install etl-cli -g
```

Heavily under development - see source code for advanced usage 

Examples:

Display results of a csv file as json:

```
etl https://data.consumerfinance.gov/api/views/s6ew-h6mp/rows.csv --silent
```

Save results of a csv file as json:
```
etl https://data.consumerfinance.gov/api/views/s6ew-h6mp/rows.csv sample.json
```

Convert json to csv:
```
etl sample.json sample.csv
```

Pipe a csv file to mongo
```
etl https://data.consumerfinance.gov/api/views/s6ew-h6mp/rows.csv mongo 
     --target_uri=mongodb://localhost:27017/test --target_collection=test
```

Pipe from mongo to elasticsearch
```
etl mongo elastic --source_uri=mongodb://localhost:27017/test --source_collection=test 
    --target_host=localhost:9200 --target_index=test --target_indextype=test
``` 

Pipe a stream from a node scraper to mongo.   The scraper should either exports a function that returns a stream, or export an object with a function named `stream`.

```
etl scraper.js elastic/index/type --target_host=localhost:9200
```


Pipe from one elastic index to another (the mapping and settings will be copied as well)
```
etl elastic/test/records elastic/test2/records --target_host=localhost:9200 --source_host=foreignhost.com:9200
```

Reindexing with a different mapping:  
```
etl elastic/test/records elastic/test2/records --schema=schema.json --target_host=localhost:9200 --source_host=localhost:9200
```
Where schema.json has a property `elastic` containing  `settings` and `mapping` (each one optional)

Pipe from elastic into S3 (newline delimited json)
```
etl elastic/test/records s3/testbucket/records.json --source_host=localhost:9200 --target_accessKeyId=XXXXX --target_secretAccessKey=XXXX
```
Pipe from S3 into elastic
```
etl s3/testbucket/records.json elastic/test2/records --target_host=localhost:9200 --source_accessKeyId=XXXXX --source_secretAccessKey=XXXX
```
