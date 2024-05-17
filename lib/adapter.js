import assert from 'assert';
import { randomUUID } from 'crypto';
import Deque from 'double-ended-queue';
import { clone, filterChange } from 'pouchdb-utils';
import {
	collectConflicts,
	compactTree,
	isLocalId,
	latest as getLatest,
	traverseRevTree
} from 'pouchdb-merge';
import {
	MISSING_DOC,
	MISSING_STUB,
	NOT_OPEN,
	REV_CONFLICT,
	createError
} from 'pouchdb-errors';
import * as fdb from '@arbendium/foundationdb';
import parseDoc from './parseDoc.js';
import { handleNewDoc, handleUpdatedDoc } from './processDocs.js';
import {
	getIsDeleted,
	getWinningRev,
	isLocalDoc,
	parseLocalRevision
} from './utils.js';

const ks = fdb.keySelector;

const DOC_STORE = 'document-store';
const BY_SEQ_STORE = 'by-sequence';
const ATTACHMENT_STORE = 'attach-store';
const BINARY_STORE = 'attach-binary-store';
const LOCAL_STORE = 'local-store';

// store the value of update_seq in the by-sequence store the key name will
// never conflict, since the keys in the by-sequence store are integers
const UPDATE_SEQ_KEY = '_local_last_update_seq';
const DOC_COUNT_KEY = '_local_doc_count';
const UUID_KEY = '_local_uuid';

const emptyBuffer = Buffer.allocUnsafe(0);

/**
 * @type {import('@arbendium/foundationdb').Transformer<number, number>}
 */
const seqEncoder = {
	pack(n) {
		assert(Number.isSafeInteger(n) && n >= 0);

		return `${n}`.padStart(16, '0');
	},
	unpack(s) {
		const n = +s;

		assert(Number.isSafeInteger(n) && n >= 0);

		return n;
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
 * @typedef {{ queue: Deque<Task> }} TransactionState
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

	const subspace = new fdb.Subspace(db.subspace.prefix);

	const stores = {
		/* eslint-disable stylistic/max-len */
		docStore: /** @type {fdb.Subspace<import('./types.js').Id, import('./types.js').Id, import('./types.js').Metadata, import('./types.js').Metadata>} */(
			subspace.at(fdb.tuple.pack(DOC_STORE), fdb.encoders.string, fdb.encoders.json)
		),
		bySeqStore: /** @type {fdb.Subspace<number, number, import('./types.js').Doc, import('./types.js').Doc>} */(
			subspace.at(fdb.tuple.pack(BY_SEQ_STORE), seqEncoder, fdb.encoders.json)
		),
		attachmentStore: /** @type {fdb.Subspace<import('./types.js').Digest, import('./types.js').Digest, import('./types.js').AttachmentRef, import('./types.js').AttachmentRef>} */(
			/** @type {unknown} */(subspace.at(fdb.tuple.pack(ATTACHMENT_STORE), undefined, fdb.encoders.json))
		),
		binaryStore: /** @type {fdb.Subspace<import('./types.js').Digest, import('./types.js').Digest>} */(
			/** @type {unknown} */(subspace.at(fdb.tuple.pack(BINARY_STORE)))
		),
		localStore: /** @type {fdb.Subspace<import('./types.js').LocalId, import('./types.js').LocalId, import('./types.js').LocalDoc, import('./types.js').LocalDoc>} */(
			/** @type {unknown} */(subspace.at(fdb.tuple.pack(LOCAL_STORE), undefined, fdb.encoders.json))
		),
		docCountStore: subspace.at(fdb.tuple.pack(DOC_COUNT_KEY), fdb.encoders.buf, fdb.encoders.uint32LE),
		instanceIdStore: subspace.at(fdb.tuple.pack(UUID_KEY), fdb.encoders.buf, fdb.encoders.buf),
		updateSeqStore: subspace.at(fdb.tuple.pack(UPDATE_SEQ_KEY), fdb.encoders.buf, seqEncoder)
		/* eslint-enable stylistic/max-len */
	};

	api._remote = false;

	api.type = function () {
		return 'foundationdb';
	};

	api._id = callbackify(
		/**
		 * TODO: That is concurrency-safe with most other read operations and, in most cases, with
		 * itself so it might not be necessary registeer it as a write transaction.
		 * @returns {Promise<string>}
		 */
		() => doWriteTn(async tn => {
			let instanceId = await tn.at(stores.instanceIdStore).get(emptyBuffer);

			if (instanceId == null) {
				instanceId = Buffer.from(randomUUID());

				tn.at(stores.instanceIdStore).set(emptyBuffer, instanceId);
			}

			return instanceId.toString();
		})
	);

	api._info = callbackify(
		/**
		 * @returns {Promise<{ doc_count: number, update_seq: number }>}
		 */
		async () => doReadTn(async tn => {
			const [docCount = 0, updateSeq = 0] = await Promise.all([
				tn.at(stores.docCountStore).get(emptyBuffer),
				tn.at(stores.updateSeqStore).get(emptyBuffer)
			]);

			return {
				doc_count: docCount,
				update_seq: updateSeq
			};
		})
	);

	api._get = callbackify(
		/**
		 * @param {import('./types.js').Id} id
		 * @param {{ rev?: import('./types.js').Rev, latest?: boolean }} param
		 */
		(id, { rev, latest } = {}) => doReadTn(async tn => {
			const metadata = await tn.at(stores.docStore).get(id);

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

			const doc = await tn.at(stores.bySeqStore).get(metadata.rev_map[rev]);

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

			return { doc, metadata };
		})
	);

	// not technically part of the spec, but if putAttachment has its own
	// method...
	api._getAttachment = callbackify(
		/**
		 * @param {import('./types.js').Id} docId
		 * @param {import('./types.js').AttachmentId} attachId
		 * @param {import('./types.js').Attachment} attachment
		 * @param {{ binary?: boolean }} param3
		 * @returns {Promise<Buffer | string>}
		 */
		(docId, attachId, attachment, { binary } = {}) => doReadTn(async tn => {
			const { digest } = attachment;

			const attach = await tn.at(stores.binaryStore).get(digest) ?? Buffer.allocUnsafe(0);

			return binary ? attach : attach.toString('base64');
		})
	);

	api._bulkDocs = callbackify(
		/**
		 * @param {{
		 *   docs: Array<
		 *     | import('./types.js').InputDoc
		 *     | import('./types.js').LocalDoc
		 *   >
		 * }} request
		 * @param {{ new_edits: boolean, was_delete?: boolean }} opts
		 * @returns {Promise<import('./types.js').BulkDocsResultRow[]>}
		 */
		({ docs: userDocs }, opts) => doWriteTn(async tn => {
			const docInfos = userDocs.map(doc => {
				if (isLocalDoc(doc)) {
					return doc;
				}

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
			let updateSeq = await tn.at(stores.updateSeqStore).get(emptyBuffer) ?? 0;

			/**
			 * @type {Record<
			 *   import('./types.js').Digest,
			 *   Promise<import('./types.js').AttachmentRef | undefined>
			 * >}
			 */
			const attachmentQueues = {};

			/**
			 * @param {import('./types.js').Id} id
			 * @param {import('./types.js').Rev} rev
			 * @param {{ digest: import('./types.js').Digest, data: Buffer | undefined }} att
			 */
			function saveAttachment(id, rev, { digest, data }) {
				// put attachments in a per-digest queue, to avoid two docs with the same
				// attachment overwriting each other
				const queue = attachmentQueues[digest]
					?? Promise.resolve(tn.at(stores.attachmentStore).get(digest));

				attachmentQueues[digest] = queue.then(async oldAtt => {
					const ref = /** @type {import('./types.js').Ref} */([id, rev].join('@'));
					/** @type {import('./types.js').AttachmentRef} */
					const newAtt = {};

					if (oldAtt) {
						newAtt.refs = oldAtt.refs;
						newAtt.refs[ref] = true;
					} else {
						newAtt.refs = { [ref]: true };
					}

					tn.at(stores.attachmentStore).set(digest, newAtt);

					if (data != null && data.length !== 0 && !oldAtt) {
						tn.at(stores.binaryStore).set(digest, data);
					}

					return newAtt;
				});
			}

			/**
			 * @param {import('./types.js').DocInfo} doc
			 */
			async function fetchDoc(doc) {
				const info = await tn.at(stores.docStore).get(doc.metadata.id);

				if (info != null) {
					fetchedDocs.set(doc.metadata.id, info);
				}
			}

			/**
			 * @param {import('./types.js').DocInfo} doc
			 */
			async function verifyStubAttachments(doc) {
				if (doc.data._attachments != null) {
					return Promise.all(
						Object.values(doc.data._attachments)
							.filter(({ stub }) => stub)
							.map(async ({ digest }) => {
								const value = await tn.at(stores.attachmentStore).get(digest);

								if (value == null) {
									throw createError(MISSING_STUB, `unknown stub attachment with digest ${digest}`);
								}
							})
					);
				}
			}

			await Promise.all(
				docInfos
					.filter(/** @returns {doc is import('./types.js').DocInfo} */doc => !isLocalDoc(doc))
					.flatMap(doc => [
						fetchDoc(doc),
						verifyStubAttachments(doc)
					])
			);

			const results = await Promise.allSettled(docInfos.map(async docInfo => {
				if (isLocalDoc(docInfo)) {
					return docInfo._deleted
						? removeLocal(docInfo, tn)
						: putLocal(docInfo, tn);
				}

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

				const seq = ++updateSeq;

				docInfo.metadata.seq = seq;
				docInfo.metadata.rev_map[rev] = seq;

				tn.at(stores.bySeqStore).set(docInfo.metadata.seq, docInfo.data);
				tn.at(stores.docStore).set(id, docInfo.metadata);

				result.docInfo.attachments?.forEach(att => saveAttachment(id, rev, att));

				return { ok: true, id: docInfo.metadata.id, rev: docInfo.metadata.rev };
			}));

			await Promise.all(Object.values(attachmentQueues));

			for (const [docId, revs] of stemmedRevs) {
				await doCompaction(docId, revs, tn);
			}

			if (api.auto_compaction) {
				for (const [docId, metadata] of fetchedDocs) {
					const revs = compactTree(metadata);

					await doCompaction(docId, revs, tn);
				}
			}

			tn.at(stores.updateSeqStore).set(emptyBuffer, updateSeq);

			if (docCountDelta) {
				tn.at(stores.docCountStore).add(emptyBuffer, docCountDelta);
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
		 * @returns {Promise<import('./types.js').AllDocsResult>}
		 */
		opts => doReadTn(async tn => {
			const [rows, docCount = 0, updateSeq = 0] = await Promise.all([
				getAllDocsRows(opts, tn),
				tn.at(stores.docCountStore).get(emptyBuffer),
				opts.update_seq ? tn.at(stores.updateSeqStore).get(emptyBuffer) : undefined
			]);

			/** @type {import('./types.js').AllDocsResult} */
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
	async function getAllDocsRows(opts, tn) {
		switch (true) {
		case 'keys' in opts:
			return Promise.all(opts.keys.map(async key => {
				const metadata = await tn.at(stores.docStore).get(key);

				if (metadata) {
					const winningRev = getWinningRev(metadata);
					const deleted = getIsDeleted(metadata, winningRev);

					return processAllDocsRow(metadata, winningRev, deleted, tn, opts);
				}

				return { key, error: 'not_found' };
			}));
		case 'key' in opts:
			return tn.at(stores.docStore).get(opts.key).then(async metadata => {
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

		const it = tn.at(stores.docStore).getRange(
			start,
			end,
			reverse ? { reverse: true } : undefined
		);

		const results = [];

		for await (const [, metadata] of it) {
			const winningRev = getWinningRev(metadata);
			const deleted = getIsDeleted(metadata, winningRev);

			if (!deleted) {
				if (skip-- > 0) {
					continue;
				}

				if (limit-- <= 0) {
					break;
				}
			} else if (!includeDeleted) {
				continue;
			}

			results.push(processAllDocsRow(metadata, winningRev, deleted, tn, opts));
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
			const originalDoc = await tn.at(stores.bySeqStore).get(metadata.rev_map[winningRev]);

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

			if (originalDoc._attachments != null) {
				responseDoc._attachments = Object.fromEntries(
					await Promise.all(Object.entries(originalDoc._attachments).map(
						async ([fileName, att]) => [
							fileName,
							{
								content_type: att.content_type,
								digest: att.digest,
								revpos: att.revpos,
								...opts.attachments
									? {
										data: opts.binary
											? await tn.at(stores.binaryStore).get(att.digest) ?? Buffer.allocUnsafe(0)
											: (await tn.at(stores.binaryStore).get(att.digest))?.toString('base64') ?? ''
									}
									: {
										length: att.length,
										stub: true
									}
							}
						]
					))
				);
			}

			doc.doc = responseDoc;
		}

		return doc;
	}

	/**
	 * @param {{
	 *   attachments?: boolean,
	 *   binary?: boolean,
	 *   complete: (e?: null | Error, result?: import('./types.js').ChangesResult) => void
	 *   continuous?: boolean,
	 *   descending?: boolean,
	 *   doc_ids?: import('./types.js').Id[],
	 *   limit?: number,
	 *   onChange: (change: import('./types.js').Change) => void,
	 *   processChange: (
	 *     doc: import('./types.js').Doc,
	 *     metadata: import('./types.js').Metadata,
	 *     opts: unknown
	 *   ) => import('./types.js').Change,
	 *   return_docs?: boolean,
	 *   since: number,
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
					opts.complete(null, /** @type {import('./types.js').ChangesResult} */(v));
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
	 *   complete: (e?: null | Error, result?: import('./types.js').ChangesResult) => void
	 *   continuous?: boolean,
	 *   descending?: boolean,
	 *   doc_ids?: import('./types.js').Id[],
	 *   limit?: number,
	 *   onChange: (change: import('./types.js').Change) => void,
	 *   processChange: (
	 *     doc: import('./types.js').Doc,
	 *     metadata: import('./types.js').Metadata,
	 *     opts: unknown
	 *   ) => import('./types.js').Change,
	 *   return_docs?: boolean,
	 *   since: number,
	 * }} opts
	 * @returns {AsyncGenerator<
	 *   import('./types.js').Change,
	 *   undefined | import('./types.js').ChangesResult
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

		let lastSeq = !opts.descending && opts.since != null ? opts.since : 0;

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
						await doReadTn(tn => tn.at(stores.docStore).get(doc._id))
					);

					if (isLocalId(metadata.id)) {
						continue;
					}

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

				/** @type {import('./types.js').Change} */
				const change = opts.processChange(winningDoc, metadata, opts);
				change.seq = metadata.seq;

				const filtered = filter(change);

				if (typeof filtered === 'object') {
					throw filtered;
				}

				if (filtered) {
					called++;

					if (opts.attachments && change.doc?._attachments != null) {
						const attachments = /** @type {import('./types.js').Doc} */(change.doc)._attachments;

						// fetch attachment immediately for the benefit
						// of live listeners
						change.doc._attachments = await doReadTn(async tn => Object.fromEntries(
							await Promise.all(Object.entries(attachments).map(
								async ([fileName, att]) => [
									fileName,
									{
										content_type: att.content_type,
										digest: att.digest,
										revpos: att.revpos,
										...opts.attachments
											? {
												data: opts.binary
													? await tn.at(stores.binaryStore).get(att.digest) ?? Buffer.allocUnsafe(0)
													: (await tn.at(stores.binaryStore).get(att.digest))?.toString('base64') ?? ''
											}
											: {
												length: att.length,
												stub: true
											}
									}
								]
							))
						));
					}

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
				return { results, last_seq: lastSeq };
			}

			const changed = await watch.promise;

			if (!changed) {
				break;
			}
		}
	}

	api._close = callbackify(async () => {
		if (db == null) {
			throw createError(NOT_OPEN);
		}

		db = /** @type {any} */(undefined);
	});

	api._getRevisionTree = callbackify(
		/**
		 * @param {import('./types.js').Id} docId
		 * @returns {Promise<import('./types.js').RevTreePath[]>}
		 */
		async docId => {
			const metadata = await doReadTn(tn => tn.at(stores.docStore).get(docId));

			if (metadata == null) {
				throw createError(MISSING_DOC);
			}

			return metadata.rev_tree;
		}
	);

	api._doCompaction = callbackify(
		/**
		 * @param {import('./types.js').Id} docId
		 * @param {import('./types.js').Rev[]} revs
		 */
		(docId, revs) => doWriteTn(tn => doCompaction(docId, revs, tn))
	);

	/**
	 * @param {import('./types.js').Id} docId
	 * @param {import('./types.js').Rev[]} revs
	 * @param {fdb.Transaction} tn
	 */
	async function doCompaction(docId, revs, tn) {
		if (!revs.length) {
			return Promise.resolve();
		}

		const metadata = await tn.at(stores.docStore).get(docId);

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

		const possiblyOrphanedAttachments = [...new Set(
			(await Promise.all(
				revs.map(async rev => {
					const seq = metadata.rev_map[rev];

					if (seq == null) {
						return [];
					}

					const doc = await tn.at(stores.bySeqStore).get(seq);

					assert(doc);

					tn.at(stores.bySeqStore).clear(seq);
					delete metadata.rev_map[rev];

					if (doc._attachments == null) {
						return [];
					}

					return Object.values(doc._attachments).map(({ digest }) => digest);
				})
			))
				.flat()
		)];

		tn.at(stores.docStore).set(metadata.id, metadata);

		if (!possiblyOrphanedAttachments.length) {
			return;
		}

		const refsToDelete = new Set(revs.map(rev => `${docId}@${rev}`));

		await Promise.all(possiblyOrphanedAttachments.map(async digest => {
			const attData = await tn.at(stores.attachmentStore).get(digest);

			if (attData != null) {
				/** @type {import('./types.js').Ref[]} */
				const refs = attData.refs != null
					? /** @type {any} */(Object.keys(attData.refs).filter(ref => !refsToDelete.has(ref)))
					: [];

				if (refs.length) { // not orphaned
					/** @type {Record<import('./types.js').Ref, true>} */
					const newRefs = {};

					refs.forEach(ref => {
						newRefs[ref] = true;
					});

					tn.at(stores.attachmentStore).set(digest, { refs: newRefs });
				} else { // orphaned, can safely delete
					tn.at(stores.attachmentStore).clear(digest);
					tn.at(stores.binaryStore).clear(digest);
				}
			}
		}));
	}

	api._getLocal = callbackify(
		/**
		 * @param {import('./types.js').LocalId} id
		 * @returns {Promise<import('./types.js').LocalDoc>}
		 */
		async id => {
			const value = await doReadTn(tn => tn.at(stores.localStore).get(id));

			if (value == null) {
				throw createError(MISSING_DOC);
			}

			return value;
		}
	);

	api._putLocal = callbackify(
		/**
		 * @param {import('./types.js').LocalDoc} doc
		 * @param {{ ctx?: fdb.Transaction }} [opts]
		 * @returns {ReturnType<typeof putLocal>}
		 */
		(doc, opts) => opts?.ctx
			? putLocal(doc, opts.ctx)
			: doWriteTn(tn => putLocal(doc, tn))
	);

	/**
	 * @param {import('./types.js').LocalDoc} doc
	 * @param {fdb.Transaction} tn
	 * @returns {Promise<{
	 *   ok: true,
	 *   id: import('./types.js').LocalId,
	 *   rev: import('./types.js').LocalRev
	 * }>}
	 */
	async function putLocal(doc, tn) {
		delete doc._revisions; // ignore this, trust the rev
		const oldRev = doc._rev;
		const id = doc._id;

		const resp = await tn.at(stores.localStore).get(id);

		if (resp == null ? oldRev : resp._rev !== oldRev) {
			throw createError(REV_CONFLICT);
		}

		doc._rev = /** @type {import('./types.js').LocalRev} */(
			oldRev ? `0-${parseLocalRevision(oldRev).id + 1}` : '0-1'
		);

		tn.at(stores.localStore).set(id, doc);

		return { ok: true, id: doc._id, rev: doc._rev };
	}

	api._removeLocal = callbackify(
		/**
		 * @param {import('./types.js').LocalDoc} doc
		 * @param {{ ctx?: fdb.Transaction }} [opts]
		 * @returns {ReturnType<typeof removeLocal>}
		 */
		(doc, opts) => opts?.ctx
			? removeLocal(doc, opts.ctx)
			: doWriteTn(tn => removeLocal(doc, tn))
	);

	/**
	 * @param {import('./types.js').LocalDoc} doc
	 * @param {fdb.Transaction} tn
	 * @returns {Promise<{
	 *   ok: true,
	 *   id: import('./types.js').LocalId,
	 *   rev: import('./types.js').LocalRev
	 * }>}
	 */
	async function removeLocal(doc, tn) {
		const resp = await tn.at(stores.localStore).get(doc._id);

		if (resp == null) {
			throw createError(MISSING_DOC);
		}

		if (resp._rev !== doc._rev) {
			throw createError(REV_CONFLICT);
		}

		tn.at(stores.localStore).clear(doc._id);

		return { ok: true, id: doc._id, rev: /** @type {import('./types.js').LocalRev} */('0-0') };
	}

	api._destroy = callbackify(async () => {
		if (db == null) {
			throw createError(NOT_OPEN);
		}

		if ('doTn' in db) {
			await db.at(subspace).clearRange();
		} else {
			db.at(subspace).clearRange();
		}

		db = /** @type {any} */(undefined);
	});
}
