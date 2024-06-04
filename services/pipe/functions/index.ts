import { ArrayHandler } from './array';
import { BitwiseHandler } from './bitwise';
import { ConditionalHandler } from './conditional';
import { DateHandler } from './date';
import { JsonHandler } from './json';
import { LogicalHandler } from './logical';
import { MathHandler } from './math';
import { NumberHandler } from './number';
import { ObjectHandler } from './object';
import { StringHandler } from './string';
import { SymbolHandler } from './symbol';
import { UnaryHandler } from './unary';

export default {
  array: new ArrayHandler(),
  bitwise: new BitwiseHandler(),
  conditional: new ConditionalHandler(),
  date: new DateHandler(),
  json: new JsonHandler(),
  logical: new LogicalHandler(),
  math: new MathHandler(),
  number: new NumberHandler(),
  object: new ObjectHandler(),
  string: new StringHandler(),
  symbol: new SymbolHandler(),
  unary: new UnaryHandler(),
};
