import vm from 'vm';

/**
 * @param {Function | string} func
 * @returns {(doc: import('../types.ts').Doc) => [unknown, unknown][]}
 */
export function mapper(func) {
	// for temp_views one can use emit(doc, emit), see #38
	if (typeof func === 'function' && func.length === 2) {
		/** @type {[unknown, unknown][]} */
		let emitted;

		/**
		 * @param {unknown} key
		 * @param {unknown} value
		 */
		function emit(key, value) {
			emitted.push([key, value]);
		}

		return doc => {
			emitted = [];

			func(doc, emit);

			return emitted;
		};
	}

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
