import crypto from 'node:crypto';

/**
 * @overload
 * @returns {(reason: any) => void}
 */
/**
 * @template T
 * @overload
 * @param {T} value
 * @returns {(reason: any) => T}
 */
/**
 * @param {any} [value]
 * @returns {(reason: any) => any}
 */
export function handleNotFound(value) {
	return function (reason) {
		if (reason.status !== 404) {
			throw reason;
		}

		return value;
	};
}

/**
 * @param {any} input
 * @returns {string}
 */
function stringify(input) {
	if (!input) {
		return 'undefined'; // backwards compat for empty reduce
	}

	// for backwards compat with mapreduce, functions/strings are stringified
	// as-is. everything else is JSON-stringified.
	switch (typeof input) {
	case 'function':
		// e.g. a mapreduce map
		return input.toString();
	case 'string':
		// e.g. a mapreduce built-in _reduce function
		return input.toString();
	default:
		// e.g. a JSON object in the case of mango queries
		return JSON.stringify(input);
	}
}

/**
 * @param {Function} mapFun
 * @param {Function} [reduceFun]
 * @returns {string}
 */
export function createViewSignature(mapFun, reduceFun) {
	return crypto
		.createHash('md5')
		.update(stringify(mapFun) + stringify(reduceFun), 'binary')
		.digest('hex');
}
