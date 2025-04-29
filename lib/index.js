import assert from 'assert';
import Deque from 'double-ended-queue';
import { clone, filterChange } from 'pouchdb-utils';
import {
	collectConflicts,
	compactTree,
	latest as getLatest,
	traverseRevTree
} from 'pouchdb-merge';
import {
	MISSING_DOC,
	createError
} from 'pouchdb-errors';
import * as fdb from '@arbendium/foundationdb';
import parseDoc from './parseDoc.js';
import { handleNewDoc, handleUpdatedDoc } from './processDocs.js';
import {
	getIsDeleted,
	getWinningRev
} from './utils.js';

const ks = fdb.keySelector;

const DOC_STORE = 'document-store';
const BY_SEQ_STORE = 'by-sequence';

// store the value of update_seq in the by-sequence store the key name will
// never conflict, since the keys in the by-sequence store are integers
const UPDATE_SEQ_KEY = '_local_last_update_seq';
const DOC_COUNT_KEY = '_local_doc_count';

const emptyBuffer = Buffer.allocUnsafe(0);

/**
 * @type {import('@arbendium/foundationdb').Transformer<
 *   import('./types.js').FinalizedMetadata,
 *   import('./types.js').FinalizedMetadata
 * >}
 */
const metadataEncoder = {
	pack(metadata) {
		const { seq } = metadata;

		if (seq.length !== 24) {
			throw new Error('Invalid seq');
		}

		// /** @type {import('./types.js').PartialBy<import('./types.js').Metadata, 'seq'>} */
		const savedMetadata = { ...metadata };

		savedMetadata.seq = /** @type {any} */(seq.slice(-4));

		const buffer = Buffer.from(JSON.stringify(savedMetadata));
		const seqBuffer = Buffer.from(seq.slice(0, 20), 'hex');

		return Buffer.concat([seqBuffer, buffer]);
	},
	unpack(buffer) {
		const seq = buffer.subarray(0, 10).toString('hex');
		buffer = buffer.subarray(10);

		/** @type {import('./types.js').FinalizedMetadata} */
		const metadata = JSON.parse(/** @type {any} */(buffer));

		metadata.seq = /** @type {any} */(seq + metadata.seq);

		for (const key in metadata.rev_map) {
			const rev = /** @type {import('./types.js').Rev} */(key);
			const revSeq = /** @type {any} */(metadata.rev_map[rev]);

			if (revSeq.length === 4) {
				metadata.rev_map[rev] = /** @type {any} */(seq + revSeq);
			}
		}

		return metadata;
	},
	packUnboundVersionstamp(metadata) {
		/** @type {import('./types.js').Metadata} */
		const nonFinalizedMetadata = {
			...metadata,
			rev_map: { ...metadata.rev_map }
		};

		const revSeq = nonFinalizedMetadata.seq;

		if (typeof revSeq !== 'bigint' || revSeq < 0 || revSeq > 0xffff) {
			throw new Error('Invalid metadata');
		}

		nonFinalizedMetadata.seq = /** @type {any} */(revSeq.toString(16).padStart(4, '0'));

		for (const key in nonFinalizedMetadata.rev_map) {
			const rev = /** @type {import('./types.js').Rev} */(key);
			const revSeq = /** @type {any} */(nonFinalizedMetadata.rev_map[rev]);

			if (typeof revSeq === 'bigint') {
				if (revSeq < 0 || revSeq > 0xffff) {
					throw new Error('Invalid seq');
				}

				nonFinalizedMetadata.rev_map[rev] = /** @type {any} */(revSeq.toString(16).padStart(4, '0'));
			}
		}

		const seqBuffer = Buffer.alloc(10);
		const buffer = Buffer.from(JSON.stringify(nonFinalizedMetadata));

		return {
			data: Buffer.concat([seqBuffer, buffer]),
			stampPos: 0
		};
	},
	bakeVersionstamp(metadata, versionstamp, code) {
		if (code != null || versionstamp.length !== 10) {
			throw new Error('Invalid version stamp');
		}

		const seq = versionstamp.toString('hex');
		const revSeq = /** @type {any} */(metadata.seq);

		if (typeof revSeq !== 'bigint' || revSeq < 0 || revSeq > 0xffff) {
			throw new Error('Invalid metadata');
		}

		metadata.seq = /** @type {import('./types.js').SeqString} */(seq + revSeq.toString(16).padStart(4, '0'));

		for (const key in metadata.rev_map) {
			const rev = /** @type {import('./types.js').Rev} */(key);
			const revSeq = /** @type {any} */(metadata.rev_map[rev]);

			if (typeof revSeq === 'bigint') {
				if (revSeq < 0 || revSeq > 0xffff) {
					throw new Error('Invalid seq');
				}

				metadata.rev_map[rev] = /** @type {import('./types.js').SeqString} */(seq + revSeq.toString(16).padStart(4, '0'));
			}
		}
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

/**
 * @type {import('@arbendium/foundationdb').Transformer<
 *   import('./types.js').SeqString | bigint,
 *   import('./types.js').SeqString
 * >}
 */
const seqEncoder = {
	pack(seq) {
		return Buffer.from(toSeqString(seq), 'hex');
	},
	unpack(seq) {
		if (seq.length !== 12) {
			throw new Error('Invalid seq');
		}

		return /** @type {import('./types.js').SeqString} */(seq.toString('hex'));
	},
	packUnboundVersionstamp(seq) {
		const buffer = Buffer.from(toSeqString(seq), 'hex');

		return {
			data: buffer,
			stampPos: 0
		};
	}
};

/**
 * @template {unknown[]} A
 * @template T
 * @param {T | ((...args: A) => Promise<T>)} fn
 * @returns {(...args: [...A, (e: unknown, v?: T) => void]) => void}
 */
function callbackify(fn) {
	const value = typeof fn === 'function'
		? /** @type {((...args: A) => Promise<T>)} */(fn)
		: Promise.resolve(fn);

	return (...args) => {
		/** @type {(e: unknown, v?: T) => void} */
		const callback = /** @type {any} */(args[args.length - 1]);

		/** @type {Promise<T>} */
		const promise = typeof value === 'function'
			? value(.../** @type {A} */(args.slice(0, -1)))
			: value;

		promise.then(
			v => callback(undefined, v),
			callback
		);
	};
}

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
 *   queue: Deque<Task>
 *   sparse?: {
 *     bySeqStore: Map<import('./types.js').SeqString | bigint, import('./types.js').Doc>
 *     docStore: Map<import('./types.js').Id, import('./types.js').Metadata>
 *     nextVersionPrefix?: string
 *     seqOffset: bigint
 *   }
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
			queue: new Deque()
		};
	}

	return state[name];
}

/**
 * @typedef {import('./types.js').SeqString} Seq
 */

/**
 * @param {any} api
 * @param {{ db: Actionable, name: string, revs_limit?: number }} opts
 */
export default function FoundationdbAdapter(api, { db, name, revs_limit: revLimit }) {
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
	 * @param {fdb.Transaction} tn
	 */
	function assertCompatibleTransaction(tn) {
		if (!('context' in tn)) {
			throw new Error('The original \'foundationdb\' library is not supported. Migrate to \'@arbendium/foundationdb\'.');
		}
	}

	/**
	 * @template T
	 * @param {(tn: fdb.Transaction) => Promise<T>} fn
	 * @param {boolean} write
	 * @returns {Promise<T>}
	 */
	function doTn(fn, write) {
		if ('doTn' in db) {
			return db.doTn(tn => {
				assertCompatibleTransaction(tn);

				return fn(tn);
			});
		}

		assertCompatibleTransaction(db);

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
	 * @param {import('./types.js').Id} id
	 * @param {fdb.Transaction} tn
	 * @returns {Promise<import('./types.js').Metadata | undefined>}
	 */
	function getDoc(id, tn) {
		const state = getTransactionState(tn, name);

		if (state.sparse != null && state.sparse.docStore.has(id)) {
			return Promise.resolve(state.sparse.docStore.get(id));
		}

		return tn.at(stores.docStore).get(id);
	}

	/**
	 * @param {import('./types.js').Id} id
	 * @param {import('./types.js').Metadata} metadata
	 * @param {fdb.Transaction} tn
	 */
	function setDoc(id, metadata, tn) {
		const state = getTransactionState(tn, name);

		if (typeof metadata.seq === 'string') {
			tn.at(stores.docStore).set(id, metadata);

			if (state.sparse != null) {
				state.sparse.docStore.delete(id);
			}
		} else {
			assert(state.sparse != null);

			tn.at(stores.docStore).setVersionstampedValue(id, metadata, false);
			state.sparse.docStore.set(id, metadata);
		}
	}

	/**
	 * @param {import('./types.js').SeqString | bigint} seq
	 * @param {fdb.Transaction} tn
	 * @returns {Promise<import('./types.js').Doc | undefined>}
	 */
	function getBySeq(seq, tn) {
		if (typeof seq === 'string') {
			return tn.at(stores.bySeqStore).get(seq);
		}

		const state = getTransactionState(tn, name);

		assert(state.sparse != null && state.sparse.bySeqStore.has(seq));

		return Promise.resolve(state.sparse.bySeqStore.get(seq));
	}

	/**
	 * @param {fdb.Transaction} tn
	 * @returns {Promise<import('./types.js').SeqString>}
	 */
	async function getSeq(tn) {
		const state = getTransactionState(tn, name);

		if (state.sparse != null) {
			state.sparse.nextVersionPrefix ??= `${fdb.util.strInc(await tn.getReadVersion()).toString('hex')}0000`;

			return /** @type {import('./types.js').SeqString} */(
				`${state.sparse.nextVersionPrefix}${state.sparse.seqOffset.toString(16).padStart(4, '0')}`
			);
		}

		return await tn.at(stores.updateSeqStore).get(emptyBuffer) ?? minSeq;
	}

	const subspace = new fdb.Subspace(db.subspace.prefix).at(fdb.tuple.pack(name));

	const stores = {
		/* eslint-disable stylistic/max-len */
		docStore: /** @type {fdb.Subspace<import('./types.js').Id, import('./types.js').Id, import('./types.js').Metadata, import('./types.js').FinalizedMetadata>} */(
			subspace.at(fdb.tuple.pack(DOC_STORE), fdb.encoders.string, metadataEncoder)
		),
		bySeqStore: /** @type {fdb.Subspace<number | import('./types.js').SeqString | bigint, import('./types.js').SeqString, import('./types.js').Doc, import('./types.js').Doc>} */(
			subspace.at(fdb.tuple.pack(BY_SEQ_STORE), seqEncoder, fdb.encoders.json)
		),
		docCountStore: subspace.at(fdb.tuple.pack(DOC_COUNT_KEY), fdb.encoders.buf, fdb.encoders.uint32LE),
		updateSeqStore: subspace.at(fdb.tuple.pack(UPDATE_SEQ_KEY), fdb.encoders.buf, seqEncoder)
		/* eslint-enable stylistic/max-len */
	};

	api._get = callbackify(
		/**
		 * @param {import('./types.js').Id} id
		 * @param {{ rev?: import('./types.js').Rev, latest?: boolean }} param
		 * @returns {Promise<{
		 *   doc: import('./types.js').Doc,
		 *   metadata: import('./types.js').Metadata
		 * }>}
		 */
		(id, { rev, latest } = {}) => doReadTn(async tn => {
			const metadata = await getDoc(id, tn);

			if (metadata == null) {
				throw createError(MISSING_DOC, 'missing');
			}

			if (!rev) {
				rev = getWinningRev(metadata);
				const deleted = getIsDeleted(metadata, rev);

				if (deleted) {
					throw createError(MISSING_DOC, 'deleted');
				}
			} else if (latest) {
				rev = getLatest(rev, metadata);
			}

			// This check does not exist in the upstream, resulting in a questionably serialization
			if (metadata.rev_map[rev] == null) {
				throw createError(MISSING_DOC);
			}

			const doc = await getBySeq(metadata.rev_map[rev], tn);

			if (doc == null) {
				throw createError(MISSING_DOC);
			}

			if (doc._id !== metadata.id) {
				// this failing implies something very wrong
				throw new Error('wrong doc returned');
			}

			if (doc._rev !== rev) {
				// this failing implies something very wrong
				throw new Error('wrong doc returned');
			}

			return {
				doc,
				// don't care about converting non-finalized seqs since they are not exposed to userland
				metadata
			};
		})
	);

	api._bulkDocs = callbackify(
		/**
		 * @param {{
		 *   docs: Array<import('./types.js').InputDoc>
		 * }} request
		 * @param {{ new_edits: boolean, was_delete?: boolean }} opts
		 * @returns {Promise<import('./types.js').BulkDocsResultRow[]>}
		 */
		({ docs: userDocs }, opts) => doWriteTn(async tn => {
			const state = getTransactionState(tn, name);

			const docInfos = userDocs.map(doc => {
				const newDoc = parseDoc(doc, opts.new_edits, api.__opts);

				if ('error' in newDoc) {
					throw newDoc;
				}

				if (newDoc.metadata.rev_map == null) {
					newDoc.metadata.rev_map = {};
				}

				return newDoc;
			});

			if (!docInfos.length) {
				return [];
			}

			/** @type {Map<import('./types.js').Id, import('./types.js').Metadata>} */
			const fetchedDocs = new Map();
			/** @type {Map<import('./types.js').Id, import('./types.js').Rev[]>} */
			const stemmedRevs = new Map();

			let docCountDelta = 0;

			/**
			 * @param {import('./types.js').DocInfo<unknown>} doc
			 */
			async function fetchDoc(doc) {
				const info = await getDoc(doc.metadata.id, tn);

				if (info != null) {
					fetchedDocs.set(doc.metadata.id, info);
				}
			}

			await Promise.all(docInfos.map(doc => fetchDoc(doc)));

			if (state.sparse == null) {
				state.sparse = {
					docStore: new Map(),
					bySeqStore: new Map(),
					seqOffset: 0n
				};
			}

			let updateSeq = state.sparse == null
				? BigInt(`0x${await tn.at(stores.updateSeqStore).get(emptyBuffer) ?? 0}`)
				// not used
				: 0n;

			const results = await Promise.allSettled(docInfos.map(async docInfo => {
				const { id } = docInfo.metadata;

				const existingDoc = fetchedDocs.get(id);

				const result = existingDoc
					? handleUpdatedDoc(existingDoc, docInfo, revLimit ?? 1000, opts)
					: handleNewDoc(docInfo, revLimit ?? 1000, opts);

				if (result == null) {
					return {};
				}

				docInfo = result.docInfo;

				fetchedDocs.set(id, docInfo.metadata);
				docCountDelta += result.delta;

				if (docInfo.stemmedRevs?.length) {
					stemmedRevs.set(docInfo.metadata.id, docInfo.stemmedRevs);
				}

				const { rev } = docInfo.metadata;

				if (state.sparse == null) {
					state.sparse = {
						docStore: new Map(),
						bySeqStore: new Map(),
						seqOffset: 0n
					};
				}

				if (state.sparse != null) {
					const seq = ++state.sparse.seqOffset;

					docInfo.metadata.seq = seq;
					docInfo.metadata.rev_map[rev] = seq;

					tn.at(stores.docStore).setVersionstampedValue(id, docInfo.metadata);
					state.sparse.docStore.set(id, docInfo.metadata);
					tn.at(stores.bySeqStore).setVersionstampedKey(seq, docInfo.data);
					state.sparse.bySeqStore.set(seq, docInfo.data);
				} else {
					const seq = toSeqString(++updateSeq);

					docInfo.metadata.seq = toSeqString(seq);
					docInfo.metadata.rev_map[rev] = toSeqString(seq);

					tn.at(stores.docStore).set(id, docInfo.metadata);
					tn.at(stores.bySeqStore).set(seq, docInfo.data);
				}

				return { ok: true, id: docInfo.metadata.id, rev: docInfo.metadata.rev };
			}));

			for (const [docId, revs] of stemmedRevs) {
				await doCompaction(docId, revs, tn);
			}

			if (api.auto_compaction) {
				for (const [docId, metadata] of fetchedDocs) {
					const revs = compactTree(metadata);

					await doCompaction(docId, revs, tn);
				}
			}

			if (docCountDelta) {
				tn.at(stores.docCountStore).add(emptyBuffer, docCountDelta);
			}

			if (state.sparse == null) {
				tn.at(stores.updateSeqStore).set(emptyBuffer, updateSeq);
			} else {
				tn.at(stores.updateSeqStore).setVersionstampedValue(
					emptyBuffer,
					state.sparse.seqOffset,
					false
				);
			}

			return results.map(
				result => result.status === 'rejected'
					? result.reason
					: result.value
			);
		})
	);

	api._allDocs = callbackify(
		/**
		 * @param {{
		 *   attachments?: boolean,
		 *   binary?: boolean,
		 *   conflicts?: boolean,
		 *   deleted?: 'ok',
		 *   descending?: boolean,
		 *   endkey?: import('./types.js').Id,
		 *   include_docs?: boolean,
		 *   inclusive_end?: boolean,
		 *   key?: import('./types.js').Id,
		 *   keys?: import('./types.js').Id[],
		 *   limit?: number,
		 *   skip?: number,
		 *   startkey?: import('./types.js').Id,
		 *   update_seq?: boolean
		 * }} opts
		 * @returns {Promise<import('./types.js').AllDocsResult<Seq>>}
		 */
		opts => doReadTn(async tn => {
			const [rows, docCount = 0, updateSeq = minSeq] = await Promise.all([
				getAllDocsRows(opts, tn),
				tn.at(stores.docCountStore).get(emptyBuffer),
				opts.update_seq ? getSeq(tn) : undefined
			]);

			/** @type {import('./types.js').AllDocsResult<Seq>} */
			const returnVal = {
				total_rows: docCount,
				offset: opts.skip,
				rows
			};

			if (opts.update_seq) {
				returnVal.update_seq = updateSeq;
			}

			return returnVal;
		})
	);

	/**
	 * @param {{
	 *   attachments?: boolean,
	 *   binary?: boolean,
	 *   conflicts?: boolean,
	 *   deleted?: 'ok',
	 *   descending?: boolean,
	 *   endkey?: import('./types.js').Id,
	 *   include_docs?: boolean,
	 *   inclusive_end?: boolean,
	 *   key?: import('./types.js').Id,
	 *   keys?: import('./types.js').Id[],
	 *   limit?: number,
	 *   skip?: number,
	 *   startkey?: import('./types.js').Id,
	 * }} opts
	 * @param {fdb.Transaction} tn
	 * @returns {Promise<import('./types.js').AllDocsResultRow[]>}
	 */
	function getAllDocsRows(opts, tn) {
		switch (true) {
		case 'keys' in opts:
			return Promise.all(opts.keys.map(async key => {
				const metadata = await getDoc(key, tn);

				if (metadata) {
					const winningRev = getWinningRev(metadata);
					const deleted = getIsDeleted(metadata, winningRev);

					return processAllDocsRow(metadata, winningRev, deleted, tn, opts);
				}

				return { key, error: 'not_found' };
			}));
		case 'key' in opts:
			return getDoc(opts.key, tn).then(async metadata => {
				if (metadata) {
					const winningRev = getWinningRev(metadata);
					const deleted = getIsDeleted(metadata, winningRev);

					if (!deleted || opts.deleted === 'ok') {
						return [await processAllDocsRow(metadata, winningRev, deleted, tn, opts)];
					}
				}

				return [];
			});
		default:
			return processAllDocs(opts, tn);
		}
	}

	/**
	 * @param {{
	 *   attachments?: boolean,
	 *   binary?: boolean,
	 *   conflicts?: boolean,
	 *   deleted?: 'ok',
	 *   descending?: boolean,
	 *   endkey?: import('./types.js').Id,
	 *   include_docs?: boolean,
	 *   inclusive_end?: boolean,
	 *   limit?: number,
	 *   skip?: number,
	 *   startkey?: import('./types.js').Id,
	 * }} opts
	 * @param {fdb.Transaction} tn
	 * @returns {Promise<import('./types.js').AllDocsResultRow[]>}
	 */
	async function processAllDocs(opts, tn) {
		const reverse = opts.descending;

		let [gte, lte] = [opts.startkey, opts.endkey];

		if (reverse) {
			[lte, gte] = [gte, lte];
		}

		let limit = opts.limit ?? Infinity;

		if (opts.limit === 0 || (gte != null && lte != null && gte > lte)) {
			return [];
		}

		const includeDeleted = opts.deleted === 'ok';
		const inclusiveEnd = opts.inclusive_end ?? true;
		let skip = opts.skip ?? 0;

		// eslint-disable-next-line no-nested-ternary
		const start = gte !== undefined
			? (!inclusiveEnd && reverse ? ks.firstGreaterThan(gte) : gte)
			: undefined;
		// eslint-disable-next-line no-nested-ternary
		const end = lte !== undefined
			? (!inclusiveEnd && !reverse ? lte : ks.firstGreaterThan(lte))
			: undefined;

		const state = getTransactionState(tn, name);
		const pendingDocs = state.sparse != null
			? [...state.sparse.docStore.values()]
				.filter(
					({ id }) => (gte === undefined || (!inclusiveEnd && reverse ? id > gte : id >= gte))
						&& (lte === undefined || (!inclusiveEnd && !reverse ? id < lte : id <= lte))
				)
				.toSorted(
					// eslint-disable-next-line no-nested-ternary
					({ id: id1 }, { id: id2 }) => id1 < id2 ? -1 : id1 > id2 ? 1 : 0
				)
			: [];
		let pendingDocIndex = reverse ? pendingDocs.length - 1 : 0;

		/** @type {Promise<import('./types.js').AllDocsResultRow>[]} */
		const results = [];

		let iterationStart = start;
		let iterationEnd = end;

		/**
		 * @param {import('./types.js').Metadata} metadata
		 * @returns {boolean}
		 */
		function processDoc(metadata) {
			if (reverse) {
				iterationEnd = metadata.id;
			} else {
				iterationStart = ks.firstGreaterThan(metadata.id);
			}

			const winningRev = getWinningRev(metadata);
			const deleted = getIsDeleted(metadata, winningRev);

			if (!deleted) {
				if (skip-- > 0) {
					return true;
				}

				if (limit-- <= 0) {
					return false;
				}
			} else if (!includeDeleted) {
				return true;
			}

			results.push(processAllDocsRow(metadata, winningRev, deleted, tn, opts));

			return true;
		}

		// eslint-disable-next-line no-labels
		out: for (;;) {
			if (reverse) {
				iterationStart = pendingDocIndex >= 0
					? ks.firstGreaterThan(pendingDocs[pendingDocIndex].id)
					: start;
			} else {
				iterationEnd = pendingDocIndex < pendingDocs.length
					? pendingDocs[pendingDocIndex].id
					: end;
			}

			const it = tn.at(stores.docStore).getRange(
				iterationStart,
				iterationEnd,
				reverse ? { reverse: true } : undefined
			);

			for await (const [, metadata] of it) {
				if (!processDoc(metadata)) {
					// eslint-disable-next-line no-labels
					break out;
				}
			}

			if (reverse ? pendingDocIndex >= 0 : pendingDocIndex < pendingDocs.length) {
				if (!processDoc(pendingDocs[pendingDocIndex])) {
					break;
				}

				if (reverse) {
					pendingDocIndex--;
				} else {
					pendingDocIndex++;
				}
			} else {
				break;
			}
		}

		return Promise.all(results);
	}

	/**
	 * @param {import('./types.js').Metadata} metadata
	 * @param {import('./types.js').Rev} winningRev
	 * @param {boolean} deleted
	 * @param {fdb.Transaction} tn
	 * @param {{
	 *   attachments?: boolean,
	 *   binary?: boolean
	 *   conflicts?: boolean,
	 *   include_docs?: boolean,
	 * }} opts
	 * @returns {Promise<import('./types.js').AllDocsResultRow>}
	 */
	async function processAllDocsRow(metadata, winningRev, deleted, tn, opts) {
		/** @type {import('./types.js').AllDocsResultRow} */
		const doc = {
			id: metadata.id,
			key: metadata.id,
			value: {
				rev: winningRev
			}
		};

		if (deleted) {
			doc.doc = null;
			doc.value.deleted = true;
		} else if (opts.include_docs) {
			const originalDoc = await getBySeq(metadata.rev_map[winningRev], tn);

			assert(originalDoc);

			originalDoc._rev = doc.value.rev;

			/** @type {import('./types.js').AllDocsResultRowDoc} */
			const responseDoc = /** @type {any} */({ ...originalDoc });

			if (opts.conflicts) {
				const conflicts = collectConflicts(metadata);

				if (conflicts.length) {
					responseDoc._conflicts = conflicts;
				}
			}

			doc.doc = responseDoc;
		}

		return doc;
	}

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
				const it = await tn.at(stores.bySeqStore).getRangeAll(
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
					: await doReadTn(tn => tn.at(stores.bySeqStore).get(metadata.rev_map[winningRev]));

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

	/**
	 * @param {import('./types.js').Id} docId
	 * @param {import('./types.js').Rev[]} revs
	 * @param {fdb.Transaction} tn
	 */
	async function doCompaction(docId, revs, tn) {
		if (!revs.length) {
			return Promise.resolve();
		}

		const metadata = await getDoc(docId, tn);

		assert(metadata);

		traverseRevTree(metadata.rev_tree, (
			isLeaf,
			pos,
			revHash,
			tn,
			opts
		) => {
			const rev = /** @type {import('./types.js').Rev} */(`${pos}-${revHash}`);

			if (revs.includes(rev)) {
				opts.status = 'missing';
			}
		});

		await Promise.all(revs.map(async rev => {
			const seq = metadata.rev_map[rev];

			if (seq == null) {
				return [];
			}

			const doc = await tn.at(stores.bySeqStore).get(seq);

			assert(doc);

			tn.at(stores.bySeqStore).clear(seq);
			delete metadata.rev_map[rev];
		}));

		setDoc(metadata.id, metadata, tn);
	}
}
