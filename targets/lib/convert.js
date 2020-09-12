const csv = require('./csv');
const json = require('./json');

module.exports = (stream, filename, argv) => {
  if (filename.endsWith('.csv')) stream = csv(stream, argv);
  else if (!argv.target_raw) stream = json(stream, argv);
  return stream
}