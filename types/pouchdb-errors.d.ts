export declare class PouchError extends Error {
	error: true
}

export declare const BAD_ARG: PouchError;
export declare const DOC_VALIDATION: PouchError;
export declare const INVALID_REV: PouchError;
export declare const MISSING_DOC: PouchError;
export declare const MISSING_STUB: PouchError;
export declare const NOT_OPEN: PouchError;
export declare const REV_CONFLICT: PouchError;
export declare function createError(error: PouchError, reason?: string): PouchError;
