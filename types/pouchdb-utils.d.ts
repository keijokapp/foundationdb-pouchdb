import type { Doc, PartialBy, RevId } from '../lib/types.ts';
import type { PouchError } from './pouchdb-errors';

export declare function clone<T>(t: T): T;
export declare function filterChange(opts: unknown): (change: Change) => boolean | PouchError;
export declare function invalidIdError(id: Id): asserts id is Id;
export declare function rev(doc: PartialBy<Doc, '_rev'>, deterministicRevs: boolean): RevId;
export declare function uuid(): string;
