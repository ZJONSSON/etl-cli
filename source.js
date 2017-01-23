const fs = require('fs');
const path = require('path');
const minimist = require('minimist');
const etl = require('etl');

function getReadStream(source) {
  if (/https/.exec(source))
    return require('request').get(source);
  else
    return fs.createReadStream(path.resolve(__dirname,source));
}

module.exports = function(argv) {
  argv = argv || minimist(process.argv.slice(2));
  const source = argv._[0];
  argv._ = argv._.slice(1);

  if (!source)
    return console.log('Source missing.  etl [source] [dest] ');

  let obj = argv.schema && require(path.resolve(__dirname,argv.schema)) || {};
  obj = Object.create(obj);


  if (/\.js$/.exec(source))
    obj.stream = require(source);
  else if (/\.json[$|?]/.exec(source))
    obj.stream = () => getReadStream(source).pipe(etl.split()).pipe(etl.map(function(d) {
      return JSON.parse(d.text);
    }));
  else if (/\.csv[$|?]/.exec(source))
    obj.stream = () => getReadStream(source).pipe(etl.csv());
  else
    return console.log('File name could not be determined');

  return require('./index')(obj,argv)
    .then( () => process.exit())
};

