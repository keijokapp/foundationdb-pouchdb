import vm from 'vm';

/**
 * @param {Function | string} func
 * @returns {(key: Buffer, value: Buffer) => [Buffer, Buffer][]}
 */
export function mapper(func) {
	const functionString = func.toString().replace(/;\s*$/, '');

	return doc => {
		const code = '(function() {"use strict";'
      + 'var __emitteds__ = [];'
      + 'var emit = function (key, value) {__emitteds__.push([key, value]);};'
      + `(${functionString})(${JSON.stringify(doc)});`
      + 'return __emitteds__;'
      + '})()';

		return vm.runInNewContext(code);
	};
}

/**
 * @param {Function | string} func
 * @returns {(keys: unknown[], values: unknown[], rereduce: boolean) => unknown}
 */
export function reducer(func) {
	const functionString = func.toString().replace(/;\s*$/, '');

	return (arg1, arg2, arg3) => {
		const code = '(function() {"use strict";'
      + `return (${functionString})(${JSON.stringify(arg1)},${JSON.stringify(arg2)},${JSON.stringify(arg3)});`
      + '})()';

		return vm.runInNewContext(code);
	};
}
