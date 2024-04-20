import data from './ts-dep';
import * as etl from 'etl';

export function chain(inbound, argv) {
  return inbound.pipe(etl.map(d => d + 10));
};