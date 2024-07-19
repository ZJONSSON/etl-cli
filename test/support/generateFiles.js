const etl = require('etl');
module.exports = () => {
  return etl.toStream(() => [
    { filename: 'test1.json', body: () => etl.toStream('This is file test1.json') },
    { filename: 'a/b/c/test2.json', body: () => etl.toStream('This is file test2.json') },
    { filename: 'a/b/c/test3.json', body: () => etl.toStream('This is file test3.json') },
  ]);
};
