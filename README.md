Command line tool for etl pipelines.

Install globally to have `etl` available on the command line:
```
npm install etl-cli -g
```

Generically you use etl-cli as follows:

```
etl [source] [target]
```

source can be any of the following:
* .js  - javascript program exporting a stream
* .json - file with json objects separated by newline
* .csv - csv file
* .xlsx - excel file, first sheet
* http(s) or s3 link to a .csv, .json or .xlsx file
* stdin (you have to specificy )
* database/collection/table (elastic, mysql, mssql, postgres, mongo, athena)

target can be any of the following:
* .json
* .csv
* s3 link to either .json or .csv file
* database/collection/table (elastic, mysql, postgres, mongo)

If the target is `.csv` then any nested fieldnames will be flattened to a path using ᐅ (or optional --sepearator=x) as a separator .   Structure will be determined by prescanning 100 lines by default (can be modified by --prescan=x)

### Command line arguments (optional)

* --silent: surpress process notifications
* --collect=x: collect x number of records for bulk insert/upsert
* --separator: separator for flattening nested objects into csv
* --transform=x: javascript transform between source and target
* --tranform_concurrency=x: concurrency for transforms
* --chain=x: javascript chain between source and target
* --proxy=x: proxy (for use with argv.getProxy())
* --source_query: query to be applied to the source
* --source_query_file: use query from either JSON or .js file that exports a function
* --schema=file : optional file with schemas for elasticsearch or bigquery
* --replace_table: bigquery - will delete the table and recreate with same schema before inserting


### Javascript source

A typical source is a javascript file that fetches something from web, ftp site or other remote location. A javascript source file needs to export an object that contains a function called `stream`.  This function will receive `argv` as first argument, which gives access to command line arguments and config.  The function needs to return a valid node stream in objectMode.

Optionally the object can also contain `recordCount` function that should return the recordCount of the source stream (if available).  This allows the runner to report % completed as the stream is running.

The javascript file can also just return a function that returns a stream.

Here is an example of javascript source:

```
const etl = require('etl');

module.exports = argv => {
  const arr = new Array(argv.count || 1000);
  return etl.toStream([...arr].map( (d,i) => ({i})));
}
```


### source/target from config

If source or target is


### argv

The argument `argv` that is passed to javascript is a combination of
* the command line arguments
* nconf:  access to `.etlconfig.json`
* inject_?: any injected datasets

### source/target specific properties

Many of the sources/targets require specific properties defined to function.   If they are defined on the command line they have to be prefixed by `source_` or `target_`.    If they are loaded through n




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

Stream first worksheet of an .xlsx file from the web:
```
etl https://www.hud.gov/sites/documents/RM-A_07-31-2014.xlsx
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

### files / s3files
If the records being streamed contains `filename` and a `body` which is either a stream or a function that returns a stream, the individual `bodys` can be saved to disk (using `files` target) or to s3 (using `s3files` target).  

By default both `files` and `s3files` will only save the file if the filename does not exist.  Both methods start scanning all the files in the target directory to see if they exist or not.  This scan is done in the background (unless you specifiy `--target_scan_await=true`).  You can also skip the scan with `--target_skip_scan=true`.  While the scan is being performed (or if it's skipped), we use fs.stats or getHeadCommand on indvidual files to check if they exist.  

Overwrite can be enforced by specifying `--target_overwrite=true`

Files can be optionally gzipped by specifying `--target_gzip=true`.  A `.gz` extension will be added to the filename and the content will be gzipped.
