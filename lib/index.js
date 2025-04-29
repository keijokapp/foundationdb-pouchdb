import assert from 'assert';
import Deque from 'double-ended-queue';
import { clone, filterChange } from 'pouchdb-utils';
import * as fdb from '@arbendium/foundationdb';
import { getWinningRev, nonNullPromise } from './utils.js';

const ks = fdb.keySelector;

const DOC_STORE = 'document-store';
const BY_SEQ_STORE = 'by-sequence';

// store the value of update_seq in the by-sequence store the key name will
// never conflict, since the keys in the by-sequence store are integers
const UPDATE_SEQ_KEY = '_local_last_update_seq';

const emptyBuffer = Buffer.allocUnsafe(0);

/**
 * @typedef {Buffer | number} Seq
 * @typedef {Buffer} FinalizedSeq
 * @typedef {{ seq: Buffer | number }} Metadata
 * @typedef {{ seq: Buffer }} FinalizedMetadata
 */

/**
 * @type {import('@arbendium/foundationdb').Transformer<Metadata, FinalizedMetadata>}
 */
const metadataEncoder = {
	pack(metadata) {
		if (!Buffer.isBuffer(metadata.seq) || metadata.seq.length !== 12) {
			throw new Error('Invalid seq');
		}

		return metadata.seq;
	},
	unpack(buffer) {
		if (!Buffer.isBuffer(buffer) || buffer.length !== 12) {
			throw new Error('Invalid seq');
		}

		return { seq: buffer };
	},
	packUnboundVersionstamp(seq) {
		if (typeof seq !== 'number' || !Number.isInteger(seq) || seq < 0 || seq > 0xffff) {
			throw new Error('Invalid seq');
		}

		const buffer = Buffer.alloc(12);
		buffer.writeUInt16BE(seq, 10);

		return {
			data: buffer,
			stampPos: 0
		};
	},
	bakeVersionstamp(metadata, versionstamp, code) {
		if (code != null || versionstamp.length !== 10) {
			throw new Error('Invalid version stamp');
		}

		if (typeof metadata.seq !== 'number' || !Number.isInteger(metadata.seq) || metadata.seq < 0 || metadata.seq > 0xffff) {
			throw new Error('Invalid seq');
		}

		const buffer = Buffer.allocUnsafe(12);
		versionstamp.copy(buffer);
		buffer.writeUInt16BE(metadata.seq, 10);

		metadata.seq = buffer;
	}
};

const minSeq = /** @type {import('./types.js').SeqString} */('000000000000000000000000');

/**
 * @param {string | bigint} seq
 * @returns {import('./types.js').SeqString}
 */
function toSeqString(seq) {
	if (typeof seq === 'bigint') {
		if (seq < 0 || seq > 0xffffffffffffffffffffffffn) {
			throw new Error('Invalid seq');
		}
	} else {
		try {
			BigInt(`0x${seq}`);
		} catch {
			throw new Error('Invalid seq');
		}
	}

	const n = typeof seq === 'bigint' ? seq.toString(16) : seq;

	return /** @type {import('./types.js').SeqString} */(n.padStart(24, '0'));
}

/** @type {import('@arbendium/foundationdb').Transformer<Buffer | number, Buffer>} */
const seqEncoder = {
	pack(seq) {
		if (!Buffer.isBuffer(seq) || seq.length !== 12) {
			throw new Error('Invalid seq');
		}

		return seq;
	},
	unpack(buffer) {
		if (!Buffer.isBuffer(buffer) || buffer.length !== 12) {
			throw new Error('Invalid seq');
		}

		return buffer;
	},
	packUnboundVersionstamp(seq) {
		if (typeof seq !== 'number' || !Number.isInteger(seq) || seq < 0 || seq > 0xffff) {
			throw new Error('Invalid seq');
		}

		const buffer = Buffer.alloc(12);
		buffer.writeUInt16BE(seq, 10);

		return {
			data: buffer,
			stampPos: 0
		};
	}
};

/**
 * @param {fdb.Transaction} db
 * @param {TransactionState} state
 */
async function executeQueue(db, state) {
	const { queue } = state;

	while (queue.length > 0) {
		const task = /** @type {Task} */(queue.peekFront());

		if (!task.write) {
			const readTasks = [task];

			for (let i = 1; i < queue.length; i++) {
				const nextTask = /** @type {Task} */(queue.get(i));

				if (nextTask.write) {
					break;
				}

				readTasks.push(nextTask);
			}

			await Promise.all(readTasks.map(
				({ fn, resolve, reject }) => fn(db).then(resolve, reject)
			));

			readTasks.forEach(() => {
				queue.shift();
			});
		} else {
			const { fn, resolve, reject } = task;

			await fn(db).then(resolve, reject);

			queue.shift();
		}
	}
}

/**
 * @typedef {(
 *   | fdb.Database
 *   | fdb.Transaction
 * )} Actionable
 * @typedef {{
 *   entryStore: Map<import('./types.js').Id, Metadata>
 *   nextVersionPrefix?: string
 *   queue: Deque<Task>
 *   recordStore: Map<Seq, Buffer>
 *   seqOffset: number
 * }} TransactionState
 * @typedef {{
 *   fn: (tn: fdb.Transaction) => Promise<unknown>
 *   resolve: (value: unknown) => void
 *   reject: (reason?: any) => void
 *   write: boolean
 * }} Task
 */

/**
 * A quasi-global state needed for locks for when concurrent PouchDB operations are made with the
 * same transaction. The key is the hidden context object of the transaction.
 * @type {WeakMap<object, Record<string, TransactionState>>}
 */
const transactionState = new WeakMap();

/**
 * @param {fdb.Transaction} tn
 * @param {string} name
 * @returns {TransactionState}
 */
function getTransactionState(tn, name) {
	let state = transactionState.get(tn.context);

	if (state == null) {
		state = {};

		transactionState.set(tn.context, state);
	}

	if (!(name in state)) {
		state[name] = {
			entryStore: new Map(),
			queue: new Deque(),
			recordStore: new Map(),
			seqOffset: 0
		};
	}

	return state[name];
}

/**
 * @param {any} api
 * @param {Actionable} db
 */
export default function FoundationdbAdapter(api, db) {
	/**
	 * @template T
	 * @param {(tn: fdb.Transaction) => Promise<T>} fn
	 * @returns {Promise<T>}
	 */
	function doReadTn(fn) {
		return doTn(fn, false);
	}

	/**
	 * @template T
	 * @param {(tn: fdb.Transaction) => Promise<T>} fn
	 * @returns {Promise<T>}
	 */
	function doWriteTn(fn) {
		return doTn(fn, true);
	}

	/**
	 * @template T
	 * @param {(tn: fdb.Transaction) => Promise<T>} fn
	 * @param {boolean} write
	 * @returns {Promise<T>}
	 */
	function doTn(fn, write) {
		if ('doTn' in db) {
			return db.doTn(fn);
		}

		const state = getTransactionState(db, name);

		const promise = new Promise((resolve, reject) => {
			state.queue.push({
				fn,
				write,
				resolve,
				reject
			});
		});

		if (state.queue.length === 1) {
			executeQueue(db, state);
		}

		return promise;
	}

	/**
	 * @param {import('./types.js').Id} key
	 * @param {fdb.Transaction} tn
	 * @returns {Promise<Metadata | undefined>}
	 */
	function getDoc(key, tn) {
		const state = getTransactionState(tn, name);

		if (state.entryStore.has(key)) {
			return Promise.resolve(state.entryStore.get(key));
		}

		return tn.at(stores.entryStore).get(key);
	}

	/**
	 * @param {Seq} seq
	 * @param {fdb.Transaction} tn
	 * @returns {Promise<Buffer | undefined>}
	 */
	function getBySeq(seq, tn) {
		if (typeof seq === 'string') {
			return tn.at(stores.recordStore).get(seq);
		}

		const state = getTransactionState(tn, name);

		assert(state.recordStore.has(seq));

		return Promise.resolve(state.recordStore.get(seq));
	}

	const subspace = new fdb.Subspace(db.subspace.prefix);
	const name = subspace.prefix.toString('base64url');

	const stores = {
		/* eslint-disable stylistic/max-len */
		entryStore: /** @type {fdb.Subspace<import('./types.js').Id, import('./types.js').Id, Metadata, FinalizedMetadata>} */(
			subspace.at(fdb.tuple.pack(DOC_STORE), fdb.encoders.string, metadataEncoder)
		),
		recordStore: /** @type {fdb.Subspace<Seq, FinalizedSeq, Buffer, Buffer>} */(
			subspace.at(fdb.tuple.pack(BY_SEQ_STORE), seqEncoder, fdb.encoders.buf)
		),
		updateSeqStore: subspace.at(fdb.tuple.pack(UPDATE_SEQ_KEY), fdb.encoders.buf, seqEncoder)
		/* eslint-enable stylistic/max-len */
	};

	/**
	 * @param {import('./types.js').Id} key
	 * @param {Buffer} value
	 */
	api.set = (key, value) => doWriteTn(async tn => {
		const state = getTransactionState(tn, name);

		/** @type {Metadata} */
		const metadata = { seq: ++state.seqOffset };

		tn.at(stores.entryStore).setVersionstampedValue(key, metadata);
		state.entryStore.set(key, metadata);
		tn.at(stores.recordStore).setVersionstampedKey(metadata.seq, value);
		state.recordStore.set(metadata.seq, value);

		tn.at(stores.updateSeqStore).setVersionstampedValue(
			emptyBuffer,
			state.seqOffset,
			false
		);
	});

	/**
	 * @param {import('./types.js').Id} key
	 * @returns {Promise<Buffer | undefined>}
	 */
	api.get = key => doReadTn(async tn => {
		const metadata = await getDoc(key, tn);

		if (metadata != null) {
			return nonNullPromise(getBySeq(metadata.seq, tn));
		}
	});

	/**
	 * @param {{
	 *   endkey?: import('./types.js').Id,
	 *   include_docs?: boolean,
	 *   inclusive_end?: boolean,
	 *   reverse?: boolean,
	 *   startkey?: import('./types.js').Id,
	 * }} opts
	 * @returns {Promise<Array<[import('./types.js').Id, Buffer | undefined]>>}
	 */
	api.getRange = opts => doReadTn(async tn => {
		const {
			endkey,
			include_docs: includeDocs = false,
			inclusive_end: inclusiveEnd = false,
			reverse = false,
			startkey
		} = opts;

		// eslint-disable-next-line no-nested-ternary
		const start = startkey !== undefined
			? (!inclusiveEnd && reverse ? ks.firstGreaterThan(startkey) : startkey)
			: undefined;
		// eslint-disable-next-line no-nested-ternary
		const end = endkey !== undefined
			? (!inclusiveEnd && !reverse ? endkey : ks.firstGreaterThan(endkey))
			: undefined;

		const state = getTransactionState(tn, name);
		const pendingDocs = [...state.entryStore.entries()]
			.filter(
				// eslint-disable-next-line stylistic/max-len
				([id]) => (startkey === undefined || (!inclusiveEnd && reverse ? id > startkey : id >= startkey))
						&& (endkey === undefined || (!inclusiveEnd && !reverse ? id < endkey : id <= endkey))
			)
			.toSorted(
				// eslint-disable-next-line no-nested-ternary
				([id1], [id2]) => id1 < id2 ? -1 : id1 > id2 ? 1 : 0
			);

		if (reverse) {
			pendingDocs.reverse();
		}

		let pendingDocIndex = 0;

		/** @type {Array<Promise<[import('./types.js').Id, Buffer | undefined]>>} */
		const results = [];

		let iterationStart = start;
		let iterationEnd = end;

		for (;;) {
			if (reverse) {
				iterationStart = pendingDocIndex < pendingDocs.length
					? ks.firstGreaterThan(pendingDocs[pendingDocIndex][0])
					: start;
			} else {
				iterationEnd = pendingDocIndex < pendingDocs.length
					? pendingDocs[pendingDocIndex][0]
					: end;
			}

			const it = tn.at(stores.entryStore).getRange(iterationStart, iterationEnd, { reverse });

			for await (const [key, metadata] of it) {
				if (reverse) {
					iterationEnd = key;
				} else {
					iterationStart = ks.firstGreaterThan(key);
				}

				results.push(processRow(key, metadata, tn, includeDocs));
			}

			if (pendingDocIndex < pendingDocs.length) {
				const [key, metadata] = pendingDocs[pendingDocIndex];

				if (reverse) {
					iterationEnd = key;
				} else {
					iterationStart = ks.firstGreaterThan(key);
				}

				results.push(processRow(key, metadata, tn, includeDocs));

				pendingDocIndex++;
			} else {
				break;
			}
		}

		return Promise.all(results);

		/**
	   * @param {import('./types.js').Id} key
	   * @param {Metadata} metadata
	   * @param {fdb.Transaction} tn
	   * @param {boolean} includeDocs
	   * @returns {Promise<[import('./types.js').Id, Buffer | undefined]>}
	   */
		async function processRow(key, metadata, tn, includeDocs) {
			return [
				key,
				includeDocs ? await nonNullPromise(getBySeq(metadata.seq, tn)) : undefined
			];
		}
	});

	/**
	 * @param {{
	 *   attachments?: boolean,
	 *   binary?: boolean,
	 *   complete: (e?: null | Error, result?: import('./types.js').ChangesResult<Seq>) => void
	 *   continuous?: boolean,
	 *   descending?: boolean,
	 *   doc_ids?: import('./types.js').Id[],
	 *   limit?: number,
	 *   onChange: (change: import('./types.js').Change<Seq>) => void,
	 *   processChange: (
	 *     doc: import('./types.js').Doc,
	 *     metadata: import('./types.js').Metadata,
	 *     opts: unknown
	 *   ) => import('./types.js').Change<Seq>,
	 *   return_docs?: boolean,
	 *   since: Seq,
	 * }} opts
	 * @returns {{ cancel: () => void }}
	 */
	api._changes = function (opts) {
		opts = clone(opts);

		let cancelled = false;
		const it = changesIterator(opts);

		async function iterate() {
			let r;

			for (r = await it.next(); !r.done && !cancelled; r = await it.next()) {
				opts.onChange(r.value);
			}

			return r.value;
		}

		iterate().then(
			v => {
				if (!cancelled) {
					opts.complete(null, /** @type {import('./types.js').ChangesResult<Seq>} */(v));
				}
			},
			e => {
				if (!cancelled) {
					opts.complete(e);
				}
			}
		);

		return {
			cancel() {
				cancelled = true;
				it.return(undefined);
			}
		};
	};

	/**
	 * @param {{
	 *   attachments?: boolean,
	 *   binary?: boolean,
	 *   complete: (e?: null | Error, result?: import('./types.js').ChangesResult<Seq>) => void
	 *   continuous?: boolean,
	 *   descending?: boolean,
	 *   doc_ids?: import('./types.js').Id[],
	 *   limit?: number,
	 *   onChange: (change: import('./types.js').Change<Seq>) => void,
	 *   processChange: (
	 *     doc: import('./types.js').Doc,
	 *     metadata: import('./types.js').Metadata,
	 *     opts: unknown
	 *   ) => import('./types.js').Change<Seq>,
	 *   return_docs?: boolean,
	 *   since: Seq,
	 * }} opts
	 * @returns {AsyncGenerator<
	 *   import('./types.js').Change<Seq>,
	 *   undefined | import('./types.js').ChangesResult<Seq>
	 * >}
	 */
	async function* changesIterator(opts) {
		const { continuous } = opts;
		const reverse = !continuous && !!opts.descending;
		const limit = opts.limit != null && opts.limit > 0
			? opts.limit
			: undefined;
		const docIds = opts.doc_ids && new Set(opts.doc_ids);
		const filter = filterChange(opts);

		let lastSeq = !opts.descending && opts.since != null
			? toSeqString(opts.since)
			: minSeq;

		const results = [];
		let called = 0;

		for (;;) {
			// eslint-disable-next-line no-loop-func
			const { it, watch } = await doReadTn(async tn => {
				const it = await tn.at(stores.recordStore).getRangeAll(
					lastSeq,
					undefined,
					{ reverse }
				);

				return {
					it,
					...continuous ? { watch: tn.at(stores.updateSeqStore).watch(emptyBuffer) } : {}
				};
			});

			/** @type {Map<import('./types.js').Id, import('./types.js').Metadata>} */
			const docIdsToMetadata = new Map();

			for (const [seq, doc] of it) {
				if (limit && called >= limit) {
					break;
				}

				if (!reverse && seq <= lastSeq) {
					continue;
				}

				if (docIds && !docIds.has(doc._id)) {
					continue;
				}

				let metadata = docIdsToMetadata.get(doc._id);

				if (!metadata) {
					metadata = /** @type {import('./types.js').Metadata} */(
						await doReadTn(tn => getDoc(doc._id, tn))
					);

					docIdsToMetadata.set(doc._id, metadata);
				}

				const winningRev = getWinningRev(metadata);

				if (metadata.seq !== seq) {
					// some other seq is later
					continue;
				}

				lastSeq = seq;

				const winningDoc = winningRev === doc._rev
					? doc
					: await doReadTn(tn => tn.at(stores.recordStore).get(metadata.rev_map[winningRev]));

				assert(winningDoc);

				/** @type {import('./types.js').Change<Seq>} */
				const change = opts.processChange(winningDoc, metadata, opts);
				change.seq = metadata.seq;

				const filtered = filter(change);

				if (typeof filtered === 'object') {
					throw filtered;
				}

				if (filtered) {
					called++;

					let returned = true;

					try {
						yield change;
						returned = false;
					} finally {
						if (returned) {
							watch?.cancel();
						}
					}

					if (opts.return_docs) {
						results.push(change);
					}
				}
			}

			if (watch == null) {
				return {
					results,
					last_seq: lastSeq
				};
			}

			const changed = await watch.promise;

			if (!changed) {
				break;
			}
		}
	}
}
