const { Readable } = require("stream");

module.exports = () => {
  const body = 'file contents';
  return Readable.from([
    { filename: 'string.txt', body },
    { filename: 'buffer.txt', body },
    { filename: 'stream.txt', body },
    { filename: 'fnString.txt', body: async () => body },
    { filename: 'fnBuffer.txt', body: async () => body },
  ]);
};