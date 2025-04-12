import vm from 'vm';

// All of this vm hullaballoo is to be able to run arbitrary code in a sandbox
// for security reasons.
export default function evalFunctionInVm(func, emit) {
	return function (arg1, arg2, arg3) {
		const code = '(function() {"use strict";'
      + 'var __emitteds__ = [];'
      + 'var emit = function (key, value) {__emitteds__.push([key, value]);};'
      + `var __result__ = (${func.replace(/;\s*$/, '')})(${JSON.stringify(arg1)},${JSON.stringify(arg2)},${JSON.stringify(arg3)});`
      + 'return {result: __result__, emitteds: __emitteds__};'
      + '})()';

		const output = vm.runInNewContext(code);

		output.emitteds.forEach(emitted => {
			emit(emitted[0], emitted[1]);
		});

		return output.result;
	};
}
