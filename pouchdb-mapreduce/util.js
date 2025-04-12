/**
 * @template T
 * @param {T} [value]
 * @returns {(err: any) => T}
 */
export function handleNotFound(value) {
	return function (reason) {
		if (reason.status === 404) {
			return value;
		}

		throw reason;
	};
}

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

export function createViewSignature(mapFun, reduceFun) {
	return stringify(mapFun) + stringify(reduceFun);
}

export function stringMd5(string) {
	return crypto.createHash('md5').update(string, 'binary').digest('hex');
}
