import { EventEmitter } from 'events';
import Codec from 'level-codec';
import ltgt from 'ltgt';

class NotFoundError extends Error {
	constructor() {
		super();
		this.name = 'NotFoundError';
	}
}

const NOT_FOUND_ERROR = new NotFoundError();

const precodec = {
	encode(decodedKey) {
		return `\xff${decodedKey[0]}\xff${decodedKey[1]}`;
	},
	decode(encodedKeyAsBuffer) {
		const str = encodedKeyAsBuffer.toString();
		const idx = str.indexOf('\xff', 1);

		return [str.substring(1, idx), str.substring(idx + 1)];
	},
	lowerBound: '\x00',
	upperBound: '\xff'
};

const codec = new Codec();

export default function sublevelPouch(db) {
	return shell(nut(db), [], db.options);
}

function nut(db) {
	function encodePrefix(prefix, key, opts1, opts2) {
		return precodec.encode([prefix, codec.encodeKey(key, opts1, opts2)]);
	}

	function addEncodings(op, prefix) {
		if (prefix && prefix.options) {
			op.keyEncoding = op.keyEncoding || prefix.options.keyEncoding;
			op.valueEncoding = op.valueEncoding || prefix.options.valueEncoding;
		}

		return op;
	}

	db.open(() => { /* no-op */ });

	return {
		apply(ops, opts = {}) {
			const batch = [];
			let i = -1;
			const len = ops.length;

			while (++i < len) {
				const op = ops[i];
				addEncodings(op, op.prefix);

				op.prefix = typeof op.prefix.prefix === 'function'
					? op.prefix.prefix()
					: op.prefix.prefix;

				batch.push({
					key: encodePrefix(op.prefix, op.key, opts, op),
					value: op.type !== 'del' && codec.encodeValue(op.value, opts, op),
					type: op.type
				});
			}

			return new Promise((resolve, reject) => {
				db.db.batch(batch, opts, (e, v) => {
					if (e != null) {
						reject(e);
					} else {
						resolve(v);
					}
				});
			});
		},
		get(key, prefix, opts) {
			opts.asBuffer = codec.valueAsBuffer(opts);

			return new Promise((resolve, reject) => {
				db.db.get(
					encodePrefix(prefix, key, opts),
					opts,
					(e, v) => {
						if (e != null) {
							reject(e);
						} else {
							resolve(codec.decodeValue(v, opts));
						}
					}
				);
			});
		},
		createDecoder(opts) {
			return function (key, value) {
				return {
					key: codec.decodeKey(precodec.decode(key)[1], opts),
					value: codec.decodeValue(value, opts)
				};
			};
		},
		isClosed() {
			return db.isClosed();
		},
		close() {
			return new Promise((resolve, reject) => {
				db.close((e, v) => {
					if (e != null) {
						reject(e);
					} else {
						resolve(v);
					}
				});
			});
		},
		async* iterator(_opts = {}) {
			const opts = { ..._opts };
			const prefix = _opts.prefix || [];

			function encodeKey(key) {
				return encodePrefix(prefix, key, opts, {});
			}

			ltgt.toLtgt(_opts, opts, encodeKey, precodec.lowerBound, precodec.upperBound);

			// if these legacy values are in the options, remove them

			opts.prefix = null;

			//* ***********************************************
			// hard coded defaults, for now...
			// TODO: pull defaults and encoding out of levelup.
			opts.keyAsBuffer = false;
			opts.valueAsBuffer = false;
			//* ***********************************************

			opts.limit ??= -1;
			opts.keyAsBuffer = precodec.buffer;
			opts.valueAsBuffer = codec.valueAsBuffer(opts);

			const iterator = db.db.iterator(opts);

			try {
				for (;;) {
					const entry = await iterator.next();

					if (entry == null) {
						break;
					}

					yield entry;
				}
			} catch (e) {
				await iterator.end();

				throw e;
			}
		}
	};
}

function shell(nut, prefix, options) {
	const emitter = new EventEmitter();
	emitter.sublevels = {};
	emitter.options = options;
	emitter.version = '6.5.4';
	emitter.methods = {};

	prefix = prefix ?? [];

	function mergeOpts(opts) {
		const o = {};
		let k;
		if (options) {
			for (k in options) {
				if (typeof options[k] !== 'undefined') {
					o[k] = options[k];
				}
			}
		}
		if (opts) {
			for (k in opts) {
				if (typeof opts[k] !== 'undefined') {
					o[k] = opts[k];
				}
			}
		}

		return o;
	}

	emitter.put = async function (key, value, opts) {
		await nut.apply(
			[{
				key,
				value,
				prefix: prefix.slice(),
				type: 'put'
			}],
			mergeOpts(opts)
		);

		emitter.emit('put', key, value);
	};

	emitter.prefix = function () {
		return prefix.slice();
	};

	emitter.batch = async function (ops, opts) {
		ops = ops.map(op => ({
			key: op.key,
			value: op.value,
			prefix: op.prefix || prefix,
			keyEncoding: op.keyEncoding, // *
			valueEncoding: op.valueEncoding, // * (TODO: encodings on sublevel)
			type: op.type
		}));

		await nut.apply(ops, mergeOpts(opts));

		emitter.emit('batch', ops);
	};

	emitter.get = async function (key, opts) {
		try {
			return await nut.get(key, prefix, mergeOpts(opts));
		} catch (e) {
			throw NOT_FOUND_ERROR;
		}
	};

	emitter.sublevel = function (name, opts) {
		emitter.sublevels[name] ??= shell(nut, prefix.concat(name), mergeOpts(opts));

		return emitter.sublevels[name];
	};

	emitter.iterator = async function* iterator(opts) {
		opts = mergeOpts(opts);
		opts.prefix = prefix;

		const it = nut.iterator(opts);
		const makeData = nut.createDecoder(opts);

		for await (const [key, value] of it) {
			yield makeData(key, value);
		}
	};

	emitter.close = () => nut.close();

	emitter.isOpen = nut.isOpen;
	emitter.isClosed = nut.isClosed;

	return emitter;
}
