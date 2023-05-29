const etl = require('etl');
const fs = require('fs');
const Promise = require('bluebird');
const recursive = require('recursive-readdir');
const recursiveAsync = Promise.promisify(recursive);
const renameAsync = Promise.promisify(fs.rename);
const utimesAsync = Promise.promisify(fs.utimes);
const fstream = require('fstream');
const convert = require('./lib/convert');

module.exports = function(stream,argv) {
  const filter_files = argv.filter_files && new RegExp(argv.filter_files);
  const target_dir = argv.target_dir || argv.target_collection;
  if (!target_dir) throw 'Not target_dir';
  let files = new Set([]);


  return etl.toStream(async () => {
    if (!argv.no_skip) files = new Set(await recursiveAsync(target_dir));
    return stream;
  })
  .pipe(etl.map(argv.concurrency || 1, async d => {
    const Key = `${target_dir}/${d.filename}`;
    if (files.has(Key)) return {message: 'skipping', Key};
    if (filter_files && !filter_files.test(Key)) return {message: 'ignoring', Key};

    let Body = typeof d.body === 'function' ? await d.body() : d.body;
    if (typeof Body == 'function') Body = Body();
    if (!Body) return {Key, message: 'No body'};
    Body = convert(Body, d.filename, argv);

    const tmpKey = `${Key}.download`;
    await new Promise( (resolve, reject) => {
      Body
        .on('error',reject)
        .pipe(fstream.Writer(tmpKey))
        .on('close',async () => {
          await renameAsync(tmpKey, Key);
          if (d.timestamp) {
            const timestamp = new Date(+d.timestamp);
            if (!isNaN(timestamp)) await utimesAsync(Key, timestamp, timestamp);
          }
          resolve();
        })
        .on('error', e => reject(e));
    });
    return {Key, message: 'OK'};

  },{
    catch: function(e) {
      console.error(e);
    }
  }));
    
};