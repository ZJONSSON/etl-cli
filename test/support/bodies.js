const { Readable } = require("stream");

module.exports = () => {
  const body = 'file contents';
  return Readable.from([
    { filename: null, body }, // ignored
    { filename: 'nobody.txt' }, // empty
    { filename: 'string.txt', body },
    { filename: 'buffer.txt', body },
    { filename: 'stream.txt', body },
    { filename: 'fnString.txt', body: async () => body },
    { filename: 'fnBuffer.txt', body: async () => body },
    { filename: 'fnStream.txt', body: async () => Readable.from([body]) },
    { filename: 'json.txt', body: { body } },
    { filename: 'fnJson.txt', body: async () => ({ body }) }
  ]);
};