// @ts-check

import assert from 'assert';
import crypto from 'crypto';
import { clone, filterChange, uuid } from 'pouchdb-utils';
import {
	collectConflicts,
	compactTree,
	isLocalId,
	latest as getLatest,
	traverseRevTree
} from 'pouchdb-merge';
import {
	BAD_ARG,
	MISSING_DOC,
	MISSING_STUB,
	NOT_OPEN,
	REV_CONFLICT,
	createError
} from 'pouchdb-errors';
import * as fdb from 'foundationdb';
import { defaultTransformer } from 'foundationdb/dist/lib/transformer.js';
import parseDoc from './parseDoc.js';
import processDocs from './processDocs.js';
import {
	getIsDeleted,
	getWinningRev,
	isLocalDoc,
	parseLocalRevision,
	parseRevision
} from './utils.js';

const ks = fdb.keySelector;

const DOC_STORE = 'document-store';
const BY_SEQ_STORE = 'by-sequence';
const ATTACHMENT_STORE = 'attach-store';
const BINARY_STORE = 'attach-binary-store';
const LOCAL_STORE = 'local-store';
const META_STORE = 'meta-store';

// store the value of update_seq in the by-sequence store the key name will
// never conflict, since the keys in the by-sequence store are integers
const UPDATE_SEQ_KEY = '_local_last_update_seq';
const DOC_COUNT_KEY = '_local_doc_count';
const UUID_KEY = '_local_uuid';

const MD5_PREFIX = 'md5-';

/**
 * @type {import('foundationdb/dist/lib/transformer.js').Transformer<number, number>}
 */
const seqEncoder = {
	pack(n) {
		if (n === Infinity) {
			n = Number.MAX_SAFE_INTEGER;
		} else {
			assert(Number.isSafeInteger(n) && n >= 0);
		}

		return `${n}`.padStart(16, '0');
	},
	unpack(s) {
		const n = +s;

		assert(Number.isSafeInteger(n) && n >= 0 && n < Number.MAX_SAFE_INTEGER);

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
 * @typedef {(
 *   | fdb.Database
 *   | fdb.Transaction
 * )} Actionable
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
	function doTn(fn) {
		if ('doTn' in db) {
			return db.doTn(fn);
		}

		return fn(db);
	}

	const subspace = db.subspace.at(fdb.tuple.pack(name), defaultTransformer, defaultTransformer);

	const stores = {
		/* eslint-disable max-len */
		docStore: /** @type {fdb.Subspace<import('./types.js').Id, import('./types.js').Id, import('./types.js').Metadata, import('./types.js').Metadata>} */(
			/** @type {unknown} */(subspace.at(fdb.tuple.pack(DOC_STORE), undefined, fdb.encoders.json))
		),
		bySeqStore: /** @type {fdb.Subspace<number, number, import('./types.js').Doc, import('./types.js').Doc>} */(
			/** @type {unknown} */(subspace.at(fdb.tuple.pack(BY_SEQ_STORE), seqEncoder, fdb.encoders.json))
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
		metaStore: subspace.at(fdb.tuple.pack(META_STORE), undefined, fdb.encoders.json)
		/* eslint-enable max-len */
	};

	api._remote = false;

	api.type = function () {
		return 'foundationdb';
	};

	api._id = callbackify(
		/**
		 * @returns {Promise<string>}
		 */
		() => doTn(async tn => {
			let instanceId = await /** @type {Promise<undefined | string>} */(
				tn.at(stores.metaStore).get(UUID_KEY)
			);

			if (instanceId == null) {
				instanceId = uuid();

				tn.at(stores.metaStore).set(UUID_KEY, instanceId);
			}

			return instanceId;
		})
	);

	api._info = callbackify(
		/**
		 * @returns {Promise<{ doc_count: number, update_seq: number }>}
		 */
		async () => doTn(async tn => {
			const [docCount = 0, updateSeq = 0] = await Promise.all([
				/** @type {Promise<undefined | number>} */(tn.at(stores.metaStore).get(DOC_COUNT_KEY)),
				/** @type {Promise<undefined | number>} */(tn.at(stores.metaStore).get(UPDATE_SEQ_KEY))
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
		(id, { rev, latest } = {}) => doTn(async tn => {
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
		(docId, attachId, attachment, { binary } = {}) => doTn(async tn => {
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
		 * @param {{ new_edits: boolean }} opts
		 * @returns {Promise<import('./types.js').BulkDocsResultRow[]>}
		 */
		({ docs: userDocs }, opts) => doTn(async tn => {
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

			const fetchedDocs = new Map();
			const stemmedRevs = new Map();

			let docCountDelta = 0;
			const [docCount = 0, updateSeq = 0] = await Promise.all([
				tn.at(stores.metaStore).get(DOC_COUNT_KEY),
				tn.at(stores.metaStore).get(UPDATE_SEQ_KEY)
			]);

			let newUpdateSeq = updateSeq;

			/**
			 * @param {import('./types.js').DocInfo} docInfo
			 * @param {import('./types.js').Rev} winningRev
			 * @param {boolean} winningRevIsDeleted
			 * @param {boolean} newRevIsDeleted
			 * @param {boolean} isUpdate
			 * @param {number} delta
			 * @returns {Promise<import('./types.js').BulkDocsResultRow>}
			 */
			async function writeDoc(
				docInfo,
				winningRev,
				winningRevIsDeleted,
				newRevIsDeleted,
				isUpdate,
				delta
			) {
				const { id, rev } = docInfo.metadata;

				docCountDelta += delta;

				docInfo.metadata.winningRev = winningRev;
				docInfo.metadata.deleted = winningRevIsDeleted;

				if (newRevIsDeleted) {
					docInfo.data._deleted = true;
				}

				assert(docInfo.stemmedRevs);

				if (docInfo.stemmedRevs.length) {
					stemmedRevs.set(docInfo.metadata.id, docInfo.stemmedRevs);
				}

				if (docInfo.data._attachments) {
					docInfo.data._attachments = Object.fromEntries(await Promise.all(
						Object.entries(docInfo.data._attachments).map(async ([key, att]) => {
							if (att.stub) {
								// still need to update the refs mapping
								await saveAttachmentRefs(id, rev, att.digest);
							} else {
								/** @type {Buffer} */
								let data;
								if (typeof att.data === 'string') {
									// input is assumed to be a base64 string
									try {
										data = Buffer.from(att.data, 'base64');

										// Node.js will just skip the characters it can't decode instead of
										// throwing an exception
										if (data.toString('base64') !== att.data) {
											throw new Error('attachment is not a valid base64 string');
										}
									} catch (e) {
										throw createError(
											BAD_ARG,
											'Attachment is not a valid base64 string'
										);
									}
								} else {
									data = /** @type {Buffer} */(att.data);
								}

								const digest = /** @type {import('./types.js').Digest} */(MD5_PREFIX + crypto.createHash('md5').update(data).digest('base64'));

								att = {
									...att,
									digest,
									length: data.length,
									revpos: parseRevision(rev).prefix
								};

								delete att.data;

								const isNewAttachment = await saveAttachmentRefs(id, rev, digest);

								if (data.length !== 0 && isNewAttachment) {
									tn.at(stores.binaryStore).set(digest, data);
								}
							}

							return [key, att];
						})
					));
				}

				assert(docInfo.metadata.rev_map[docInfo.metadata.rev] == null);

				const seq = ++newUpdateSeq;
				docInfo.metadata.seq = seq;
				docInfo.metadata.rev_map[rev] = seq;
				tn.at(stores.bySeqStore).set(seq, docInfo.data);
				tn.at(stores.docStore).set(id, docInfo.metadata);
				fetchedDocs.set(id, docInfo.metadata);

				return { ok: true, id, rev };
			}

			// attachments are queued per-digest, otherwise the refs could be
			// overwritten by concurrent writes in the same bulkDocs session
			/** @type {Record<import('./types.js').Digest, Promise<boolean>>} */
			const attachmentQueues = {};

			/**
			 * @param {import('./types.js').Id} id
			 * @param {import('./types.js').Rev} rev
			 * @param {import('./types.js').Digest} digest
			 * @returns {Promise<boolean>}
			 */
			function saveAttachmentRefs(id, rev, digest) {
				// put attachments in a per-digest queue, to avoid two docs with the same
				// attachment overwriting each other
				const queue = attachmentQueues[digest] ?? Promise.resolve();

				attachmentQueues[digest] = queue.then(async () => {
					const oldAtt = await tn.at(stores.attachmentStore).get(digest);

					const ref = /** @type {import('./types.js').Ref} */([id, rev].join('@'));
					/** @type {import('./types.js').AttachmentRef} */
					const newAtt = {};

					if (oldAtt) {
						if (oldAtt.refs) {
							// only update references if this attachment already has them
							// since we cannot migrate old style attachments here without
							// doing a full db scan for references
							newAtt.refs = oldAtt.refs;
							newAtt.refs[ref] = true;
						}
					} else {
						newAtt.refs = { [ref]: true };
					}

					tn.at(stores.attachmentStore).set(digest, newAtt);

					return !oldAtt;
				});

				return attachmentQueues[digest];
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

			const results = await processDocs(
				revLimit,
				docInfos,
				api,
				fetchedDocs,
				tn,
				writeDoc,
				opts
			);

			for (const [docId, revs] of stemmedRevs) {
				await doCompaction(docId, revs, tn);
			}

			if (api.auto_compaction) {
				for (const [docId, metadata] of fetchedDocs) {
					const revs = compactTree(metadata);

					await doCompaction(docId, revs, tn);
				}
			}

			tn.at(stores.metaStore).set(UPDATE_SEQ_KEY, newUpdateSeq);
			tn.at(stores.metaStore).set(DOC_COUNT_KEY, docCount + docCountDelta);

			return results;
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
		opts => doTn(async tn => {
			/** @type {[import('./types.js').AllDocsResultRow[], number, number]} */
			const [rows, docCount = 0, updateSeq = 0] = await Promise.all([
				getAllDocsRows(opts, tn),
				tn.at(stores.metaStore).get(DOC_COUNT_KEY),
				opts.update_seq ? tn.at(stores.metaStore).get(UPDATE_SEQ_KEY) : undefined
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
			: '';
		// eslint-disable-next-line no-nested-ternary
		const end = lte !== undefined
			? (!inclusiveEnd && !reverse ? lte : ks.firstGreaterThan(lte))
			: '\xff'; // TODO: make sure that ID-s starting with 0xff are also included

		const it = tn.at(stores.docStore).getRange(
			/** @type {any} */(start),
			/** @type {any} */(end),
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
			e => { if (!cancelled) { opts.complete(e); } }
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
			const { it, watch } = await doTn(async tn => {
				const it = await tn.at(stores.bySeqStore).getRangeAll(
					lastSeq,
					Infinity,
					{ reverse }
				);

				return {
					it,
					...continuous ? { watch: tn.at(stores.metaStore).watch(UPDATE_SEQ_KEY) } : {}
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
						await doTn(tn => tn.at(stores.docStore).get(doc._id))
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
					: await doTn(tn => tn.at(stores.bySeqStore).get(metadata.rev_map[winningRev]));

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
						change.doc._attachments = await doTn(async tn => Object.fromEntries(
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
			const metadata = await doTn(tn => tn.at(stores.docStore).get(docId));

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
		(docId, revs) => doTn(tn => doCompaction(docId, revs, tn))
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
			if (revs.indexOf(rev) !== -1) {
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
			const value = await doTn(tn => tn.at(stores.localStore).get(id));

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
			: doTn(tn => putLocal(doc, tn))
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
			: doTn(tn => removeLocal(doc, tn))
	);

	/**
	 *
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
			await db.at(subspace).clearRange(Buffer.from([]), Buffer.from([0xff]));
		} else {
			db.at(subspace).clearRange(Buffer.from([]), Buffer.from([0xff]));
		}

		db = /** @type {any} */(undefined);
	});
}
