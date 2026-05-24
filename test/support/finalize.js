let count = 0;

module.exports = function(d) {
  count++;
  return d;
};

/** @this {import('stream').Transform} */
module.exports.finalize = async function() {
  await new Promise(resolve => setTimeout(resolve, 5));
  this.push({ finalized: count });
  return { returned: count };
};
