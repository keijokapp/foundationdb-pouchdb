import levelup from 'levelup';
import PouchDB from 'pouchdb-core';
import {
	clone,
	changesHandler as Changes,
	filterChange,
	functionName,
	uuid
} from 'pouchdb-utils';
import {
	isDeleted,
	isLocalId,
	parseDoc,
	processDocs
} from 'pouchdb-adapter-utils';
import {
	winningRev as calculateWinningRev,
	traverseRevTree,
	compactTree,
	collectConflicts,
	latest as getLatest
} from 'pouchdb-merge';
import { safeJsonParse, safeJsonStringify } from 'pouchdb-json';
import { binaryMd5 } from 'pouchdb-md5';
import {
	MISSING_DOC,
	REV_CONFLICT,
	NOT_OPEN,
	BAD_ARG,
	MISSING_STUB,
	createError
} from 'pouchdb-errors';
import Deque from './double-ended-queue.js';
import sublevel from './sublevel.js';
import LevelTransaction from './transaction.js';

/**
 * @typedef {unknown} DbStore
 * @typedef {{ sublevel: (prefix: string, opts: { valueEncoding: any }) => Sublevel }} Sublevel
 */

const DOC_STORE = 'document-store';
const BY_SEQ_STORE = 'by-sequence';
const ATTACHMENT_STORE = 'attach-store';
const BINARY_STORE = 'attach-binary-store';
const LOCAL_STORE = 'local-store';
const META_STORE = 'meta-store';

// leveldb barks if we try to open a db multiple times
// so we cache opened connections here for initstore()
const dbStores = new Map();

// store the value of update_seq in the by-sequence store the key name will
// never conflict, since the keys in the by-sequence store are integers
const UPDATE_SEQ_KEY = '_local_last_update_seq';
const DOC_COUNT_KEY = '_local_doc_count';
const UUID_KEY = '_local_uuid';

const MD5_PREFIX = 'md5-';

const safeJsonEncoding = {
	encode: safeJsonStringify,
	decode: safeJsonParse,
	buffer: false,
	type: 'cheap-json'
};

const levelChanges = new Changes();

// https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Global_Objects/Promise/withResolvers
function withResolvers() {
	let resolve;
	let reject;
	const promise = new Promise((res, rej) => {
		resolve = res;
		reject = rej;
	});

	return { resolve, reject, promise };
}

// winningRev and deleted are performance-killers, but
// in newer versions of PouchDB, they are cached on the metadata
function getWinningRev(metadata) {
	return 'winningRev' in metadata
		? metadata.winningRev
		: calculateWinningRev(metadata);
}

function getIsDeleted(metadata, winningRev) {
	return 'deleted' in metadata
		? metadata.deleted
		: isDeleted(metadata, winningRev);
}

function callbackify(fn) {
	if (typeof fn !== 'function') {
		fn = Promise.resolve(fn);
	}

	return (...args) => {
		const callback = args[args.length - 1];

		const promise = typeof fn === 'function'
			? fn(...args.slice(0, -1))
			: fn;

		promise.then(
			v => callback(undefined, v),
			callback
		);
	};
}

async function fetchAttachment(att, stores, opts) {
	const type = att.content_type;

	const buffer = await stores.binaryStore.get(att.digest) ?? Buffer.allocUnsafe(0);

	buffer.type = type;

	delete att.stub;
	delete att.length;
	att.data = opts.binary ? buffer : buffer.toString('base64');
}

function fetchAttachments(results, stores, opts) {
	const atts = [];
	results.forEach(row => {
		if (!(row.doc && row.doc._attachments)) {
			return;
		}
		const attNames = Object.keys(row.doc._attachments);
		attNames.forEach(attName => {
			const att = row.doc._attachments[attName];
			if (!('data' in att)) {
				atts.push(att);
			}
		});
	});

	return Promise.all(atts.map(att => fetchAttachment(att, stores, opts)));
}

/**
 * @param {string} leveldownName
 * @returns {Map<string, Promise<Sublevel>>}
 */
function getDbStore(leveldownName) {
	if (dbStores.has(leveldownName)) {
		return dbStores.get(leveldownName);
	}

	const dbStore = new Map();
	dbStores.set(leveldownName, dbStore);

	return dbStore;
}

/**
 * @param {Leveldown} leveldown
 * @param {string} leveldownName
 * @param {string} name
 * @returns {Promise<Sublevel>}
 */
function getSublevel(leveldown, leveldownName, name, opts) {
	const dbStore = getDbStore(leveldownName);

	if (dbStore.has(name)) {
		return /** @type {Promise<Sublevel>} */(dbStore.get(name));
	}

	const promise = new Promise((resolve, reject) => {
		const db = sublevel(levelup(leveldown(name), opts, err => {
			/* istanbul ignore if */
			if (err) {
				dbStore.delete(name);

				return reject(err);
			}

			db._docCount = -1;
			db._queue = new Deque();

			resolve(db);
		}));
	});

	dbStore.set(name, promise);

	return typeof opts.migrate === 'object'
		? promise.then(db => {
			opts.migrate.doMigrationOne(name, db);

			return db;
		})
		: promise;
}

export default async function LevelPouch(opts) {
	opts = clone(opts);
	const api = this;
	const revLimit = opts.revs_limit;
	const { name } = opts;
	// TODO: this is undocumented and unused probably
	/* istanbul ignore else */
	if (typeof opts.createIfMissing === 'undefined') {
		opts.createIfMissing = true;
	}

	const leveldown = opts.db;
	const leveldownName = /** @type {string} */(functionName(leveldown));

	const dbStore = getDbStore(leveldownName);
	const db = await getSublevel(leveldown, leveldownName, name, opts);

	// afterDBCreated
	const stores = {
		docStore: db.sublevel(DOC_STORE, { valueEncoding: safeJsonEncoding }),
		bySeqStore: db.sublevel(BY_SEQ_STORE, { valueEncoding: 'json' }),
		attachmentStore: db.sublevel(ATTACHMENT_STORE, { valueEncoding: 'json' }),
		binaryStore: db.sublevel(BINARY_STORE, { valueEncoding: 'binary' }),
		localStore: db.sublevel(LOCAL_STORE, { valueEncoding: 'json' }),
		metaStore: db.sublevel(META_STORE, { valueEncoding: 'json' })
	};

	/* istanbul ignore else */
	if (typeof opts.migrate === 'object') { // migration for leveldown
		await opts.migrate.doMigrationTwo(db, stores);
	}

	// afterLastMigration
	if (typeof db._updateSeq === 'undefined') {
		db._updateSeq = await stores.metaStore.get(UPDATE_SEQ_KEY) ?? 0;
	}

	db._docCount = await stores.metaStore.get(DOC_COUNT_KEY) ?? 0;
	const instanceId = await stores.metaStore.get(UUID_KEY).then(async instanceId => {
		if (instanceId == null) {
			const instanceId = uuid();

			await stores.metaStore.put(UUID_KEY, instanceId);
		}

		return instanceId;
	});

	function countDocs() {
		/* istanbul ignore if */
		if (db.isClosed()) {
			throw new Error('database is closed');
		}

		return db._docCount; // use cached value
	}

	api._remote = false;

	/* istanbul ignore next */
	api.type = function () {
		return 'leveldb';
	};

	api._id = callbackify(instanceId);

	api._info = callbackify(async () => ({
		doc_count: db._docCount,
		update_seq: db._updateSeq,
		backend_adapter: leveldownName
	}));

	async function executeNext() {
		const queue = db._queue;

		while (queue.length > 0) {
			const task = queue[queue.front];

			if (task.type === 'read') {
				const readTasks = [task];

				for (let i = 1; i < queue.length; i++) {
					// eslint-disable-next-line no-bitwise
					const nextTask = queue[(queue.front + i) & (queue.capacity - 1)];

					if (nextTask.type !== 'read') {
						break;
					}

					readTasks.push(nextTask);
				}

				await Promise.all(readTasks.map(
					task => task.fun.call(undefined, ...task.args).then(task.resolve, task.reject)
				));

				readTasks.forEach(() => {
					queue[queue.front] = undefined;
					// eslint-disable-next-line no-bitwise
					queue.front = (queue.front + 1) & (queue.capacity - 1);
					queue.length--;
				});
			} else {
				await task.fun.call(undefined, ...task.args).then(task.resolve, task.reject);

				queue[queue.front] = undefined;
				// eslint-disable-next-line no-bitwise
				queue.front = (queue.front + 1) & (queue.capacity - 1);
				queue.length--;
			}
		}
	}

	// all read/write operations to the database are done in a queue,
	// similar to how websql/idb works. this avoids problems such
	// as e.g. compaction needing to have a lock on the database while
	// it updates stuff. in the future we can revisit this.
	function writeLock(fun) {
		return function (...args) {
			const { resolve, reject, promise } = withResolvers();

			db._queue.push({
				fun,
				args,
				type: 'write',
				resolve,
				reject
			});

			if (db._queue.length === 1) {
				Promise.resolve().then(executeNext);
			}

			return promise;
		};
	}

	// same as the writelock, but multiple can run at once
	function readLock(fun) {
		return function (...args) {
			const { resolve, reject, promise } = withResolvers();

			db._queue.push({
				fun,
				args,
				type: 'read',
				resolve,
				reject
			});

			if (db._queue.length === 1) {
				Promise.resolve().then(executeNext);
			}

			return promise;
		};
	}

	function formatSeq(n) {
		return (`0000000000000000${n}`).slice(-16);
	}

	function parseSeq(s) {
		return parseInt(s, 10);
	}

	api._get = callbackify(readLock(async (id, opts = {}) => {
		opts = clone(opts);

		const metadata = await stores.docStore.get(id);

		if (metadata == null) {
			throw createError(MISSING_DOC, 'missing');
		}

		let rev;
		if (!opts.rev) {
			rev = getWinningRev(metadata);
			const deleted = getIsDeleted(metadata, rev);
			if (deleted) {
				throw createError(MISSING_DOC, 'deleted');
			}
		} else {
			rev = opts.latest ? getLatest(opts.rev, metadata) : opts.rev;
		}

		const seq = metadata.rev_map[rev];

		const doc = await stores.bySeqStore.get(formatSeq(seq));

		if (doc == null) {
			throw createError(MISSING_DOC);
		}

		/* istanbul ignore if */
		if ('_id' in doc && doc._id !== metadata.id) {
			// this failing implies something very wrong
			throw new Error('wrong doc returned');
		}

		doc._id = metadata.id;
		if ('_rev' in doc) {
			/* istanbul ignore if */
			if (doc._rev !== rev) {
				// this failing implies something very wrong
				throw new Error('wrong doc returned');
			}
		} else {
			// we didn't always store this
			doc._rev = rev;
		}

		return { doc, metadata };
	}));

	// not technically part of the spec, but if putAttachment has its own
	// method...
	api._getAttachment = callbackify(async (docId, attachId, attachment, opts = {}) => {
		const { digest } = attachment;
		const type = attachment.content_type;

		const attach = await stores.binaryStore.get(digest) ?? Buffer.allocUnsafe(0);

		attach.type = type;

		return opts.binary ? attach : attach.toString('base64');
	});

	api._bulkDocs = callbackify(writeLock(async (req, opts = {}) => {
		const newEdits = opts.new_edits;
		const results = new Array(req.docs.length);
		const fetchedDocs = new Map();
		const stemmedRevs = new Map();

		const txn = new LevelTransaction();
		let docCountDelta = 0;
		let newUpdateSeq = db._updateSeq;

		// parse the docs and give each a sequence number
		const userDocs = req.docs;
		const docInfos = userDocs.map(doc => {
			if (doc._id && isLocalId(doc._id)) {
				return doc;
			}
			const newDoc = parseDoc(doc, newEdits, api.__opts);

			if (newDoc.metadata && !newDoc.metadata.rev_map) {
				newDoc.metadata.rev_map = {};
			}

			return newDoc;
		});

		const infoErrors = docInfos.filter(doc => doc.error);

		if (infoErrors.length) {
			throw infoErrors[0];
		}

		if (!docInfos.length) {
			return [];
		}

		// verify any stub attachments as a precondition test
		async function verifyAttachment(digest) {
			const value = await txn.get(stores.attachmentStore, digest);

			if (value == null) {
				throw createError(MISSING_STUB, `unknown stub attachment with digest ${digest}`);
			}
		}

		async function verifyAttachments() {
			const digests = [];

			userDocs.forEach(doc => {
				if (doc && doc._attachments) {
					Object.keys(doc._attachments).forEach(filename => {
						const att = doc._attachments[filename];
						if (att.stub) {
							digests.push(att.digest);
						}
					});
				}
			});

			await Promise.all(digests.map(digest => verifyAttachment(digest)));
		}

		async function fetchExistingDocs() {
			await Promise.all(userDocs.map(async doc => {
				if (doc._id && isLocalId(doc._id)) {
					// skip local docs
					return;
				}

				const info = await txn.get(stores.docStore, doc._id);

				if (info != null) {
					fetchedDocs.set(doc._id, info);
				}
			}));
		}

		async function compact(revsMap) {
			let promise = Promise.resolve();

			revsMap.forEach((revs, docId) => {
				// TODO: parallelize, for now need to be sequential to
				// pass orphaned attachment tests
				promise = promise.then(() => doCompactionNoLock(docId, revs, { ctx: txn }));
			});

			return promise;
		}

		async function autoCompact() {
			const revsMap = new Map();
			fetchedDocs.forEach((metadata, docId) => {
				revsMap.set(docId, compactTree(metadata));
			});

			await compact(revsMap);
		}

		async function writeDoc(
			docInfo,
			winningRev,
			winningRevIsDeleted,
			newRevIsDeleted,
			isUpdate,
			delta,
			resultsIdx
		) {
			docCountDelta += delta;

			docInfo.metadata.winningRev = winningRev;
			docInfo.metadata.deleted = winningRevIsDeleted;

			docInfo.data._id = docInfo.metadata.id;
			docInfo.data._rev = docInfo.metadata.rev;

			if (newRevIsDeleted) {
				docInfo.data._deleted = true;
			}

			if (docInfo.stemmedRevs.length) {
				stemmedRevs.set(docInfo.metadata.id, docInfo.stemmedRevs);
			}

			const attachments = docInfo.data._attachments
				? Object.keys(docInfo.data._attachments)
				: [];

			await Promise.all(attachments.map(async key => {
				const att = docInfo.data._attachments[key];

				if (att.stub) {
					// still need to update the refs mapping
					const id = docInfo.data._id;
					const rev = docInfo.data._rev;

					return saveAttachmentRefs(id, rev, att.digest);
				}

				let data;
				if (typeof att.data === 'string') {
					// input is assumed to be a base64 string
					try {
						const base64 = Buffer.from(att.data, 'base64');

						// Node.js will just skip the characters it can't decode instead of
						// throwing an exception
						if (base64.toString('base64') !== att.data) {
							throw new Error('attachment is not a valid base64 string');
						}

						data = base64.toString('binary');
					} catch (e) {
						throw createError(
							BAD_ARG,
							'Attachment is not a valid base64 string'
						);
					}
				} else {
					data = att.data;
				}

				const result = await new Promise(resolve => { binaryMd5(data, resolve); });

				await saveAttachment(docInfo, MD5_PREFIX + result, key, data);
			}));

			let seq = docInfo.metadata.rev_map[docInfo.metadata.rev];

			/* istanbul ignore if */
			if (seq) {
				// check that there aren't any existing revisions with the same
				// revision id, else we shouldn't do anything
				return;
			}

			seq = ++newUpdateSeq;
			docInfo.metadata.seq = seq;
			docInfo.metadata.rev_map[docInfo.metadata.rev] = seq;
			const seqKey = formatSeq(seq);
			txn.batch([
				{
					key: seqKey,
					value: docInfo.data,
					prefix: stores.bySeqStore,
					type: 'put'
				},
				{
					key: docInfo.metadata.id,
					value: docInfo.metadata,
					prefix: stores.docStore,
					type: 'put'
				}
			]);
			results[resultsIdx] = {
				ok: true,
				id: docInfo.metadata.id,
				rev: docInfo.metadata.rev
			};
			fetchedDocs.set(docInfo.metadata.id, docInfo.metadata);
		}

		// attachments are queued per-digest, otherwise the refs could be
		// overwritten by concurrent writes in the same bulkDocs session
		const attachmentQueues = {};

		function saveAttachmentRefs(id, rev, digest) {
			async function fetchAtt() {
				const oldAtt = await txn.get(stores.attachmentStore, digest);

				saveAtt(oldAtt);

				return !oldAtt;
			}

			function saveAtt(oldAtt) {
				const ref = [id, rev].join('@');
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
					newAtt.refs = {};
					newAtt.refs[ref] = true;
				}

				txn.batch([{
					type: 'put',
					prefix: stores.attachmentStore,
					key: digest,
					value: newAtt
				}]);
			}

			// put attachments in a per-digest queue, to avoid two docs with the same
			// attachment overwriting each other
			const queue = attachmentQueues[digest] || Promise.resolve();
			attachmentQueues[digest] = queue.then(fetchAtt);

			return attachmentQueues[digest];
		}

		async function saveAttachment(docInfo, digest, key, data) {
			const att = docInfo.data._attachments[key];
			delete att.data;
			att.digest = digest;
			att.length = data.length;
			const { id } = docInfo.metadata;
			const { rev } = docInfo.metadata;
			att.revpos = parseInt(rev, 10);

			const isNewAttachment = await saveAttachmentRefs(id, rev, digest);

			// do not try to store empty attachments
			if (data.length === 0) {
				return;
			}

			if (!isNewAttachment) {
				// small optimization - don't bother writing it again
				return;
			}

			txn.batch([{
				type: 'put',
				prefix: stores.binaryStore,
				key: digest,
				value: Buffer.from(data, 'binary')
			}]);
		}

		async function complete() {
			txn.batch([
				{
					prefix: stores.metaStore,
					type: 'put',
					key: UPDATE_SEQ_KEY,
					value: newUpdateSeq
				},
				{
					prefix: stores.metaStore,
					type: 'put',
					key: DOC_COUNT_KEY,
					value: db._docCount + docCountDelta
				}
			]);

			await txn.execute(db);

			db._docCount += docCountDelta;
			db._updateSeq = newUpdateSeq;
			levelChanges.notify(name);
		}

		await verifyAttachments();
		await fetchExistingDocs();

		await new Promise((resolve, reject) => {
			processDocs(
				revLimit,
				docInfos,
				api,
				fetchedDocs,
				txn,
				results,
				(...args) => {
					const callback = args[args.length - 1];

					writeDoc(...args.slice(0, -1)).then(callback, reject);
				},
				opts,
				resolve
			);
		});

		await compact(stemmedRevs);

		if (api.auto_compaction) {
			await autoCompact();
		}

		await complete();

		return results;
	}));

	api._allDocs = callbackify(async opts => {
		if ('keys' in opts) {
			const finalResults = {
				offset: opts.skip
			};

			finalResults.rows = await Promise.all(opts.keys.map(async key => {
				const subOpts = { key, deleted: 'ok', ...opts };

				delete subOpts.limit;
				delete subOpts.skip;
				delete subOpts.keys;

				const res = await allDocsWithLock(subOpts);

				/* istanbul ignore if */
				if (opts.update_seq && res.update_seq !== undefined) {
					finalResults.update_seq = res.update_seq;
				}

				finalResults.total_rows = res.total_rows;

				return res.rows[0] ?? { key, error: 'not_found' };
			}));

			return finalResults;
		}

		return allDocsWithLock(opts);
	});

	const allDocsWithLock = readLock(async opts => {
		opts = clone(opts);

		const docCount = countDocs();

		const readstreamOpts = {};
		let skip = opts.skip || 0;

		if (opts.startkey) {
			readstreamOpts.gte = opts.startkey;
		}

		if (opts.endkey) {
			readstreamOpts.lte = opts.endkey;
		}

		if (opts.key) {
			readstreamOpts.lte = opts.key;
			readstreamOpts.gte = opts.key;
		}

		if (opts.descending) {
			readstreamOpts.reverse = true;
			// switch start and ends
			const tmp = readstreamOpts.lte;
			readstreamOpts.lte = readstreamOpts.gte;
			readstreamOpts.gte = tmp;
		}

		let limit;

		if (typeof opts.limit === 'number') {
			limit = opts.limit;
		}

		if (limit === 0
				|| ('gte' in readstreamOpts && 'lte' in readstreamOpts
				&& readstreamOpts.gte > readstreamOpts.lte)) {
			// should return 0 results when start is greater than end.
			// normally level would "fix" this for us by reversing the order,
			// so short-circuit instead
			const returnVal = {
				total_rows: docCount,
				offset: opts.skip,
				rows: []
			};
			/* istanbul ignore if */
			if (opts.update_seq) {
				returnVal.update_seq = db._updateSeq;
			}

			return returnVal;
		}

		const results = [];
		const it = stores.docStore.iterator(readstreamOpts);

		for await (const entry of it) {
			const metadata = entry.value;
			// winningRev and deleted are performance-killers, but
			// in newer versions of PouchDB, they are cached on the metadata
			const winningRev = getWinningRev(metadata);
			const deleted = getIsDeleted(metadata, winningRev);
			if (!deleted) {
				if (skip-- > 0) {
					continue;
				}

				if (typeof limit === 'number' && limit-- <= 0) {
					break;
				}
			} else if (opts.deleted !== 'ok') {
				continue;
			}

			if (opts.inclusive_end === false && metadata.id === opts.endkey) {
				continue;
			}

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
				doc.doc = await stores.bySeqStore.get(formatSeq(metadata.rev_map[winningRev]));

				doc.doc._rev = doc.value.rev;

				if (opts.conflicts) {
					const conflicts = collectConflicts(metadata);
					if (conflicts.length) {
						doc.doc._conflicts = conflicts;
					}
				}

				for (const att in doc.doc._attachments) {
					if (Object.prototype.hasOwnProperty.call(doc.doc._attachments, att)) {
						doc.doc._attachments[att].stub = true;
					}
				}
			}

			results.push(doc);
		}

		if (opts.include_docs && opts.attachments) {
			await fetchAttachments(results, stores, opts);
		}

		const returnVal = {
			total_rows: docCount,
			offset: opts.skip,
			rows: results
		};

		/* istanbul ignore if */
		if (opts.update_seq) {
			returnVal.update_seq = db._updateSeq;
		}

		return returnVal;
	});

	api._changes = function (opts) {
		opts = clone(opts);

		if (opts.continuous) {
			const id = `${name}:${uuid()}`;
			levelChanges.addListener(name, id, api, opts);
			levelChanges.notify(name);

			return {
				cancel() {
					levelChanges.removeListener(name, id);
				}
			};
		}

		const { descending } = opts;
		const results = [];
		let lastSeq = opts.since || 0;
		let called = 0;

		const streamOpts = {
			reverse: descending
		};

		const limit = 'limit' in opts && opts.limit > 0 ? opts.limit : undefined;

		if (!streamOpts.reverse) {
			streamOpts.start = formatSeq(opts.since || 0);
		}

		const docIds = opts.doc_ids && new Set(opts.doc_ids);
		const filter = filterChange(opts);
		const docIdsToMetadata = new Map();

		const it = stores.bySeqStore.iterator(streamOpts);

		async function iterate() {
			for await (const data of it) {
				if (limit && called >= limit) {
					break;
				}

				if (opts.cancelled || opts.done) {
					continue;
				}

				const seq = parseSeq(data.key);
				const doc = data.value;

				if (seq === opts.since && !descending) {
					// couchdb ignores `since` if descending=true
					continue;
				}

				if (docIds && !docIds.has(doc._id)) {
					continue;
				}

				let metadata = docIdsToMetadata.get(doc._id);

				if (!metadata) {
					// metadata not cached, have to go fetch it
					metadata = await stores.docStore.get(doc._id);

					/* istanbul ignore if */
					if (opts.cancelled || opts.done || db.isClosed() || isLocalId(metadata.id)) {
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
					: await stores.bySeqStore.get(formatSeq(metadata.rev_map[winningRev]));

				const change = opts.processChange(winningDoc, metadata, opts);
				change.seq = metadata.seq;

				const filtered = filter(change);

				if (typeof filtered === 'object') {
					throw filtered;
				}

				if (filtered) {
					called++;

					if (opts.attachments && opts.include_docs) {
						// fetch attachment immediately for the benefit
						// of live listeners
						fetchAttachments([change], stores, opts).then(() => {
							opts.onChange(change);
						});
					} else {
						opts.onChange(change);
					}

					if (opts.return_docs) {
						results.push(change);
					}
				}
			}

			if (!opts.cancelled) {
				opts.done = true;

				if (opts.return_docs && opts.limit) {
					if (opts.limit < results.length) {
						results.length = opts.limit;
					}
				}

				if (opts.include_docs && opts.attachments && opts.return_docs) {
					await fetchAttachments(results, stores, opts);
				}

				return { results, last_seq: lastSeq };
			}
		}

		iterate().then(
			v => { if (!opts.cancelled) { opts.complete(null, v); } },
			e => { opts.complete(e); }
		);

		return {
			cancel() {
				opts.cancelled = true;
				opts.done = true;

				it.return();
			}
		};
	};

	api._close = callbackify(async () => {
		/* istanbul ignore if */
		if (db.isClosed()) {
			throw createError(NOT_OPEN);
		}

		await db.close();

		dbStore.delete(name);

		const adapterName = leveldownName;
		const adapterStore = dbStores.get(adapterName);
		const viewNamePrefix = `${PouchDB.prefix + name}-mrview-`;
		const keys = [...adapterStore.keys()].filter(k => k.includes(viewNamePrefix));
		keys.forEach(key => {
			const eventEmitter = adapterStore.get(key);
			eventEmitter.removeAllListeners();
			eventEmitter.close();
			adapterStore.delete(key);
		});
	});

	api._getRevisionTree = callbackify(async docId => {
		const metadata = await stores.docStore.get(docId);

		if (metadata == null) {
			throw createError(MISSING_DOC);
		}

		return metadata.rev_tree;
	});

	api._doCompaction = callbackify(writeLock(doCompactionNoLock));

	// the NoLock version is for use by bulkDocs
	async function doCompactionNoLock(docId, revs, opts = {}) {
		if (!revs.length) {
			return Promise.resolve();
		}
		const txn = opts.ctx ?? new LevelTransaction();

		const metadata = await txn.get(stores.docStore, docId);

		const seqs = revs.map(rev => {
			const seq = metadata.rev_map[rev];
			delete metadata.rev_map[rev];

			return seq;
		});

		traverseRevTree(metadata.rev_tree, (
			isLeaf,
			pos,
			revHash,
			ctx,
			opts
		) => {
			const rev = `${pos}-${revHash}`;
			if (revs.indexOf(rev) !== -1) {
				opts.status = 'missing';
			}
		});

		const batch = [];

		batch.push({
			key: metadata.id,
			value: metadata,
			type: 'put',
			prefix: stores.docStore
		});

		const digestMap = {};

		async function deleteOrphanedAttachments() {
			const possiblyOrphanedAttachments = Object.keys(digestMap);

			if (!possiblyOrphanedAttachments.length) {
				return;
			}

			const refsToDelete = new Map();
			revs.forEach(rev => {
				refsToDelete.set(`${docId}@${rev}`, true);
			});

			await Promise.all(possiblyOrphanedAttachments.map(async digest => {
				const attData = await txn.get(stores.attachmentStore, digest);

				if (attData != null) {
					const refs = Object.keys(attData.refs || {}).filter(ref => !refsToDelete.has(ref));
					const newRefs = {};

					refs.forEach(ref => {
						newRefs[ref] = true;
					});

					if (refs.length) { // not orphaned
						batch.push({
							key: digest,
							type: 'put',
							value: { refs: newRefs },
							prefix: stores.attachmentStore
						});
					} else { // orphaned, can safely delete
						batch.push(
							{
								key: digest,
								type: 'del',
								prefix: stores.attachmentStore
							},
							{
								key: digest,
								type: 'del',
								prefix: stores.binaryStore
							}
						);
					}
				}
			}));
		}

		await Promise.all(seqs.map(async seq => {
			batch.push({
				key: formatSeq(seq),
				type: 'del',
				prefix: stores.bySeqStore
			});

			const doc = await txn.get(stores.bySeqStore, formatSeq(seq));

			if (doc != null) {
				const atts = Object.keys(doc._attachments || {});
				atts.forEach(attName => {
					const { digest } = doc._attachments[attName];
					digestMap[digest] = true;
				});
			}
		}));

		await deleteOrphanedAttachments();

		txn.batch(batch);

		if (!opts.ctx) {
			await txn.execute(db);
		}
	}

	api._getLocal = callbackify(async id => {
		const value = await stores.localStore.get(id);

		if (value == null) {
			throw createError(MISSING_DOC);
		}

		return value;
	});

	api._putLocal = callbackify(
		(doc, opts = {}) => opts.ctx
			? putLocalNoLock(doc, opts)
			: putLocalWithLock(doc, opts)
	);

	const putLocalWithLock = writeLock(putLocalNoLock);

	// the NoLock version is for use by bulkDocs
	async function putLocalNoLock(doc, opts) {
		delete doc._revisions; // ignore this, trust the rev
		const oldRev = doc._rev;
		const id = doc._id;

		const txn = opts.ctx ?? new LevelTransaction();

		const resp = await txn.get(stores.localStore, id);

		if (resp == null ? oldRev : resp._rev !== oldRev) {
			throw createError(REV_CONFLICT);
		}

		doc._rev = oldRev ? `0-${parseInt(oldRev.split('-')[1], 10) + 1}` : '0-1';

		txn.batch([{
			type: 'put',
			prefix: stores.localStore,
			key: id,
			value: doc
		}]);

		if (!opts.ctx) {
			await txn.execute(db);
		}

		return { ok: true, id: doc._id, rev: doc._rev };
	}

	api._removeLocal = callbackify(
		(doc, opts = {}) => opts.ctx
			? removeLocalNoLock(doc, opts)
			: removeLocalWithLock(doc, opts)
	);

	const removeLocalWithLock = writeLock(removeLocalNoLock);

	// the NoLock version is for use by bulkDocs
	async function removeLocalNoLock(doc, opts) {
		const txn = opts.ctx ?? new LevelTransaction();

		const resp = await txn.get(stores.localStore, doc._id);

		if (resp == null) {
			throw createError(MISSING_DOC);
		}

		if (resp._rev !== doc._rev) {
			throw createError(REV_CONFLICT);
		}

		txn.batch([{
			prefix: stores.localStore,
			type: 'del',
			key: doc._id
		}]);

		if (!opts.ctx) {
			await txn.execute(db);
		}

		return { ok: true, id: doc._id, rev: '0-0' };
	}

	// close and delete open leveldb stores
	api._destroy = callbackify(async () => {
		if (dbStores.has(leveldownName)) {
			const dbStore = dbStores.get(leveldownName);

			if (dbStore.has(name)) {
				levelChanges.removeAllListeners(name);

				await dbStore.get(name).then(db => db.close()).catch(() => {});

				dbStore.delete(name);
			}
		}

		// May not exist if leveldown is backed by memory adapter
		/* istanbul ignore else */
		if ('destroy' in leveldown) {
			return new Promise((resolve, reject) => {
				leveldown.destroy(name, (e, v) => {
					if (e != null) {
						return reject(e);
					}

					resolve(v);
				});
			});
		}
	});
}
