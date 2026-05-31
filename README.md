[![Coverage](https://ZJONSSON.github.io/etl-cli/badge.svg)](https://ZJONSSON.github.io/etl-cli/)

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
* .js / .ts  - javascript or typescript program exporting a stream (loaded via `tsx`)
* .json - file with json objects separated by newline (or a single json document with `--json_multiline`)
* .csv - csv file
* .xlsx - excel file, first sheet
* .parquet - parquet file (row groups streamed, projection via `--columns=a,b,c`)
* .xml - xml file (use `--selector` to choose the element)
* http(s) or s3 link to a .csv, .json, .xlsx, .parquet or .xml file
* stdin (you have to specify `--source_type=json|csv|xlsx|parquet|xml` since there is no extension to sniff)
* directory of files (`files/<dir>`, emits `{ filename, body }` records, body is lazy)
* sftp directory (`sftp/<path>`, walks remote directory and emits `{ filename, body }` records)
* s3 prefix (`s3files/<bucket>/<prefix>` or `s3://bucket/prefix`, emits `{ filename, body }` records)
* database/collection/table (elastic, mysql, mssql, postgres, mongo, athena, dynamo)

target can be any of the following:
* .json
* .csv
* .parquet (requires `--schema=schemafile.json` with a `parquet` property)
* directory of files (`files/<dir>`, writes each `{ filename, body }` record to disk)
* sftp directory (`sftp/<path>`, uploads each `{ filename, body }` record over sftp)
* s3 link to either .json or .csv file (single object), or `s3files/<bucket>/<prefix>` for multiple objects
* database/collection/table (elastic, mysql, postgres, mongo, bigquery, dynamo)
* `screen` (default if no target specified - pretty-prints to stdout)
* `test` (collects records into the result for programmatic / test usage)

If the target is `.csv` then any nested fieldnames will be flattened to a path using `>` (or optional --separator=x) as a separator.   Structure will be determined by prescanning 100 lines by default (can be modified by --prescan=x)

Both source and target accept a path-like syntax that gets parsed into `source_*` / `target_*` properties.  For example `mongo/mydb/mycoll` becomes `source_db_name=mydb` and `source_collection=mycoll`, `elastic/myindex/mytype` becomes `source_index=myindex` and `source_indextype=mytype`, and so on.   The segments after the first are also exposed as `source_params` / `target_params` (an array) for sources/targets that interpret them directly (s3, sftp, files).

### Command line arguments (optional)

* --silent: suppress process notifications
* --collect=x: collect x number of records for bulk insert/upsert
* --concurrency=x: default concurrency for files/s3files/sftp targets and transforms
* --target_concurrency=x: concurrency for db targets that support it (elastic default 5, bigquery default 10)
* --separator: separator for flattening nested objects into csv (default `>`)
* --prescan=x: number of rows to prescan for csv header discovery (default 100)
* --transform=x: javascript transform between source and target (inline arrow function, or comma-separated module paths)
* --transform_concurrency=x: concurrency for transforms
* --chain=x: javascript chain between source and target (operates on the upstream stream rather than per record); may be an async generator `async function* (incoming, argv)` receiving the full upstream stream
* --filter=x: inline filter function, e.g. `--filter="d => d.country == 'US'"`
* --select=a,b,c: keep only these top-level keys
* --remove=a,b: drop these top-level keys
* --setid=key: copy `d[key]` into `d._id` (handy before mongo/elastic targets)
* --limit=n: stop emitting after n output records
* --count: short-circuit the pipeline and emit `[{ recordCount }]` from the source's recordCount function
* --proxy=x: proxy (for use with argv.getProxy()).  String can contain `{{random}}` which gets substituted per call.  If `x` matches a key in `.etlconfig.json` it is replaced with that key's value.
* --source_query: query to be applied to the source
* --source_query_file: use query from either JSON or .js/.ts file that exports a function (function gets `argv` and may return a string or json)
* --schema=file : optional file with schemas for elasticsearch, bigquery or parquet
* --prescan_size=x: number of records used to infer a schema when target needs one (default 1000)
* --export_schema=true: short-circuit the pipeline and emit the inferred (or supplied) JSON schema as a single record
* --export_glue_schema=true: emit AWS Glue / Athena column definitions derived from the schema
* --replace_table: bigquery - will delete the table and recreate with same schema before inserting
* --config=path: load an extra `nconf`-compatible json config file at runtime
* --report_interval=ms: how often progress is logged (default 1000)
* --highWaterMark=n: backpressure setting for the post-transform stream
* --throw: throw transform/target errors instead of swallowing them


### Javascript source

A typical source is a javascript file that fetches something from a web API, FTP site, database, or other remote location.  The file must export a `stream` function that receives `argv` (command line arguments and config) and returns either a node stream in objectMode or an async generator that yields records.

The file can export in one of three equivalent forms:

* `module.exports = argv => stream` — shorthand: the file itself is the `stream` function
* `module.exports.stream = (argv) => stream` — named export; allows adding `recordCount` alongside it
* `module.exports.stream = async function* (argv) { yield ...; }` — async generator; no stream boilerplate needed

The named-export forms can also include a `recordCount` function that returns the total number of records, which lets the runner report % completed as the stream runs.

```js
// classic stream form
const etl = require('etl');
module.exports = argv => {
  const arr = new Array(argv.count || 1000);
  return etl.toStream([...arr].map((d, i) => ({ i })));
};
```

```js
// async generator form — no stream boilerplate needed
module.exports.stream = async function* (argv) {
  for (let i = 0; i < (argv.count || 1000); i++) {
    yield { i };
  }
};
```


### source/target from config

If source or target is the name of a key in `.etlconfig.json` (loaded from `~/.etlconfig.json` or whatever path `ETL_CONFIG` points at), all the properties under that key get merged into `argv` as `source_*` / `target_*`.  This lets you keep credentials and host info out of your shell history.   The `source` (or `target`) property inside a config block can also redirect to a different module name, e.g.:

```json
{
  "myftp": {
    "source": "sftp",
    "host": "files.example.com",
    "username": "etl",
    "privateKeyFile": "/Users/me/.ssh/id_rsa",
    "path": "incoming"
  },
  "elastic": {
    "host": "https://prod.es.example.com:9200",
    "auth": { "username": "etl", "password": "secret" }
  }
}
```

With this config `etl myftp files/./downloads` will run an `sftp` source against `files.example.com`.

### argv

The argument `argv` that is passed to javascript is a combination of
* the command line arguments (parsed by `minimist`)
* nconf:  access to `.etlconfig.json` via `argv.nconf`
* `argv.etl`: the `etl` library, ready to use
* `argv.fetch(url, opts)`: a `node-fetch` wrapped to honor `--proxy`.  Supports `opts.jar` for cookie-aware fetches via `fetch-cookie`
* `argv.request` / `argv.requestAsync`: `request` wrappers that auto-apply the proxy
* `argv.getProxy()`: returns the active proxy string with `{{random}}` substituted
* `argv.userAgent`: current user agent (overridable with `--userAgent` or via config)
* inject_?: any injected datasets - any flag that begins with `inject_` is itself resolved as a source and made available on `argv` as a stream.   For example `--inject_seed=seed.json` exposes `argv.inject_seed` as a stream of seed records

### source/target specific properties

Many of the sources/targets require specific properties defined to function.   If they are defined on the command line they have to be prefixed by `source_` or `target_`.    If they are loaded through nconf they get nested under the source/target key (without the prefix) and merged in automatically.   The most common ones:

* elastic: `host`, `index`, `indextype`, `size`, `scroll`, `auth`
* mongo: `uri`, `db_name`, `collection`
* mysql / postgres: `host`, `port`, `user`, `password`, `database`, `table`
* mssql: `url`
* bigquery: `projectId`, `target_index` (dataset), `target_indextype` (table)
* s3 / s3files: `accessKeyId`, `secretAccessKey`, `region`, `bucket`, `prefix`, `filter` (regex on key)
* sftp: `host`, `port` (default 22), `username`, `password`, `privateKeyFile`, `path`, `filter`
* files: `dir`, `filter-files` (regex)
* athena: `database`, `table`, `outputLocation`
* dynamo: `region`, `endpoint` (for local/custom endpoint), `collection` (table name, from path `dynamo/<table>`), `indextype` (GSI/LSI name for source, from path `dynamo/<table>/<gsi>`), `query` (JSON FilterExpression object for source)




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

Mirror an sftp directory to local disk (skipping files that already exist):
```
etl sftp/incoming files/./downloads --source_host=files.example.com --source_username=etl --source_privateKeyFile=~/.ssh/id_rsa
```

Upload a directory tree to sftp, gzipping each file:
```
etl files/./out sftp/upload/sub --target_host=files.example.com --target_username=etl --target_password=$FTP_PASSWORD --target_gzip=true
```

Mirror an s3 prefix to local disk:
```
etl s3files/mybucket/data files/./mirror --source_accessKeyId=XXXX --source_secretAccessKey=XXXX
```

Run an Athena query and dump results to a parquet file (using a schema):
```
etl athena results.parquet --database=my_db --source_query="select * from my_table where dt='2024-01-01'" --outputLocation=s3://my-athena-output/queries/ --schema=schema.json
```

Scan a DynamoDB table to JSON:
```
etl dynamo/my-table output.json --source_region=us-east-1
```

Copy a DynamoDB table to another region:
```
etl dynamo/src-table dynamo/dst-table --source_region=us-east-1 --target_region=eu-west-1
```

Load a CSV into DynamoDB (batch mode — strongly recommended for write throughput):
```
etl data.csv dynamo/my-table --target_region=us-east-1 --collect=25
```
Without `--collect`, each record is written individually (`PutItem`). With `--collect=25`, records are batched into `BatchWriteItem` requests of up to 25 items — DynamoDB's maximum — which is significantly faster for bulk loads.

Filter, project and limit:
```
etl source.csv target.json --filter="d => d.country == 'US'" --select=id,country,amount --limit=1000
```

Export the inferred JSON schema for a CSV file:
```
etl input.csv test --export_schema=true
```

Export AWS Glue column definitions from a parquet file:
```
etl data.parquet test --export_glue_schema=true
```

Use a transform module (works for both .js and .ts):
```
etl source.csv target.json --transform=./transforms/normalize.ts
```

Inject another source into a custom scraper:
```
etl scraper.js out.json --inject_lookup=lookup.json
```

Programmatic use:
```js
const etl = require('etl-cli');

const result = await etl({
  source: 'input.csv',
  target: 'test',
  silent: true,
  filter: "d => +d.amount > 100",
});

console.log(result.Σ_in, result.Σ_out, result.data);
```

### files / s3files / sftp
If the records being streamed contain `filename` and a `body`, the individual bodies can be saved to disk (using `files` target), to s3 (using `s3files` target) or to a remote sftp directory (using `sftp` target).   Records with a `body` also get two convenience methods added automatically:
* `await d.buffer()` — reads the body and returns a `Buffer`
* `await d.json()` — reads the body and returns the parsed JSON (`JSON.parse(await d.buffer())`)

`body` can be:
* a string or Buffer
* a node Readable stream
* a function that returns any of the above (or a Promise of any of the above)
* a JSON value (serialized when written)

By default `files`, `s3files` and `sftp` will only save the file if the filename does not exist.  All three start scanning all the files in the target directory to see if they exist or not.  This scan is done in the background (unless you specify `--target_await_scan=true`).  You can also skip the scan with `--target_skip_scan=true`.  While the scan is being performed (or if it's skipped), we use `fs.stat`, `HeadObjectCommand` or `sftp.stat` on individual files to check if they exist.

Overwrite can be enforced by specifying `--target_overwrite=true`

Files can be optionally gzipped by specifying `--target_gzip=true`.  A `.gz` extension will be added to the filename and the content will be gzipped.

`files` and `sftp` write to a `.download` suffix first and rename atomically on success - so a partial write will not be picked up by the next run's existence check.  `s3files` doesn't need this because s3 multipart uploads only commit the object on completion - a failed upload simply never materializes.   For `files` the source `mtime` is preserved when present.

Instead of specifying s3files source or target you can also use a full s3 url, i.e. `s3://bucket/path/to/file`

### transforms

Transforms run after the source and before the target.  They can be inline arrow functions (evaluated in a `vm`):
```
etl input.csv output.json --transform="d => ({ ...d, total: d.a + d.b })"
```

Or a module path (resolved relative to the working directory, supports both .js and .ts via `tsx`):
```
etl input.csv output.json --transform=./transforms/normalize.js
```

The module may export:
* a function `(d, argv) => d` - applied with `etl.map` at `--transform_concurrency`
* `{ transform, catch, flush, finalize }` - a customized `etl.map`; see `finalize` below
* `{ chain }` (or a function with `.chain = true`) - applied with `etl.chain` instead of `etl.map`; if `chain` is an async generator `async function* (incoming, argv)`, the entire upstream stream is passed as `incoming` and the result is consumed via `Readable.from`
* an async generator `async function* (d, argv) { yield ...; }` - applied with `stream.flatMap` at `--transform_concurrency`.   Each upstream record can yield zero or many downstream records, e.g. fanning out a parent record into its children.  Note: with `--transform_concurrency > 1` output order across records is not guaranteed.

Example async-generator transform (fan-out):

```js
module.exports = async function* (row, argv) {
  if (row.skip) return;
  yield { id: row.id, type: 'main' };
  yield { id: row.id, type: argv.extra_type || 'extra' };
};
```

#### finalize

Any transform module (function or async generator) can export a `finalize` function that runs once after all upstream records have been processed.  It is useful for flushing accumulators or appending summary records.

`finalize` can emit records in two ways — both are supported and both are emitted downstream:
* call `this.push(record)` for multiple records
* return a value (or `yield` values if `finalize` is itself an async generator)

```js
// regular function transform with a finalize that appends a summary record
let count = 0;

module.exports = function(d) {
  count++;
  return d;
};

module.exports.finalize = async function* () {
  yield { summary: 'total', count };
};
```

`finalize` also works when the transform is an async generator:

```js
let seen = 0;

module.exports = async function* (d) {
  seen++;
  yield d;
};

module.exports.finalize = async function* () {
  yield { finalized: seen };
};
```

Multiple transforms can be chained with comma separation: `--transform=./step1.js,./step2.js,./step3.js`

`--chain=...` is similar but operates on the upstream readable stream rather than per record, which is useful for batching or async flow control.

### schema

Some targets need a schema (parquet, bigquery, csv flattening, optionally elastic).   The runner handles this in three layered ways:

1. If `--schema=file.json` is set, the relevant subkey (`parquet`, `bigquery`, `elastic`) is passed to the target.
2. If the source provides a `schema()` function (e.g. parquet), that schema is used.
3. Otherwise, when a target requires a schema, the runner prescans `--prescan_size` records (default 1000) and infers a JSON schema using `@jsonhero/schema-infer`.  Any `anyOf` types are coerced to `string`.

Two convenience modes short-circuit the pipeline and emit a schema instead of data:
* `--export_schema=true` - emits the inferred (or supplied) JSON schema as a single record
* `--export_glue_schema=true` - emits the schema as an array of `{ Name, Type }` objects compatible with AWS Glue / Athena column definitions

Any `--transform` drops the source-provided schema and falls back to inference from output data (since the shape may have changed).

For elasticsearch reindexing, the source's mapping and settings are copied to the target index automatically (see examples).  For bigquery, `--replace_table` will drop and recreate the table with the same schema (or one supplied via `--schema`) before inserting.

### counters and exit value

Every pipeline returns (and logs unless `--silent`):
* `Σ_in` - records read from the source
* `Σ_out` - records emitted to the target after filter / select / `--limit`
* `Σ_skipped` - records skipped (existing files, records without a usable body, etc.)
* `data` - only present with the `test` target (collected records)
* `argv` - non-enumerable, attached for inspection in tests
