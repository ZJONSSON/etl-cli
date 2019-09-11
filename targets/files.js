const etl = require('etl');
const fs = require('fs');
const Promise = require('bluebird');
const recursive = require('recursive-readdir');
const recursiveAsync = Promise.promisify(recursive);
const renameAsync = Promise.promisify(fs.rename);
const fstream = require('fstream');

module.exports = function(stream,argv) {
  const target_dir = argv.target_dir || argv.target_collection;
  if (!target_dir) throw 'Not target_dir';
  let files = new Set([]);


  return etl.toStream(async () => {
    if (!argv.no_skip) files = new Set(await recursiveAsync(target_dir));
    return stream;
  })
  .pipe(etl.map(argv.concurrency || 1, async d => {
    const Key = `${target_dir}/${d.filename}`;
    if (files.has(Key)) {
      return {message: 'skipping', Key};
    }

    const Body = typeof d.body === 'function' ? await d.body() : d.body;
    if (!Body) return {Key, message: 'No body'};
    const tmpKey = `${Key}.download`;
    await new Promise( (resolve, reject) => {
      Body
        .on('error',reject)
        .pipe(fstream.Writer(tmpKey))
        .on('close',async () => {
          await renameAsync(tmpKey, Key);
          resolve();
        })
        .on('error', e => reject(e));
    });
    return {Key, message: 'OK'};

  },{
    catch: function(e,d) {
      console.error(e);
      this.write(d);
    }
  }));
    
};