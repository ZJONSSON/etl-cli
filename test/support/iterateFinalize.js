let count = 0;

module.exports = async function* (d) {
  count++;
  yield { parent: d.a };
};

module.exports.finalize = async function* () {
  await new Promise(resolve => setTimeout(resolve, 5));
  yield { finalized: count };
  await new Promise(resolve => setTimeout(resolve, 5));
  yield { done: true };
};
