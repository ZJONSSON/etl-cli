const csvStream = require('./csvStream');
const jsonStream = require('./jsonStream');

module.exports = async (d, argv) => {
  let Body = typeof d.body === 'function' ? await d.body() : d.body;
  if (d.filename.endsWith('.json')) {
    Body = jsonStream(Body, argv);
  } else if (d.filename.endsWith('.csv')) {
    Body = csvStream(Body, argv);
  }
  return Body;
}
