/* eslint-disable camelcase,stylistic/max-len */
import {
	collate,
	toIndexableString,
	normalizeKey,
	parseIndexableString
} from 'pouchdb-collate';
import { uniq, mapToKeysArray } from 'pouchdb-mapreduce-utils';
import createView from './createView.js';
import evalFunction from './evalFunction.js';
import { handleNotFound } from './util.js';

/**
 * @typedef {object} PouchDB
 * @typedef {{
 *   adapter: Adapter,
 *   db: PouchDB,
 *   mapFun: Function,
 *   name: string,
 *   reduceFun: Function,
 *   seq: number,
 *   sourceDB: PouchDB
 * }} View
 * @typedef {unknown} Doc
 */

class TaskQueue {
	constructor() {
		/** @type {Promise<unknown>} */
		this.promise = Promise.resolve();
	}

	/**
	* @template T
	* @param {() => Promise<Awaited<T>>} task
	* @returns {Promise<Awaited<T>>}
	*/
	add(task) {
		const promise = this.promise
			// just recover
			.catch(() => { })
			.then(() => task());

		this.promise = promise;

		return promise;
	}

	finish() {
		return this.promise;
	}
}

/** @type {Record<string, TaskQueue>} */
const queues = {};
const CHANGES_BATCH_SIZE = 50;

/**
* @param {string} name
* @returns {[string, string]}
*/
function parseViewName(name) {
	// can be either 'ddocname/viewname' or just 'viewname'
	// (where the ddoc name is the same)
	return name.indexOf('/') === -1
		? [name, name]
		: /** @type {any} */(name.split('/', 2));
}

/**
 * @param {Array<{rev: string}>} changes
 * @returns {boolean}
 */
function isGenOne(changes) {
	// only return true if the current change is 1-
	// and there are no other leafs
	return changes.length === 1 && /^1-/.test(changes[0].rev);
}

/**
 * @param {Function} mapFun
 * @param {(_: unknown, __: unknown) => void} emit
 * @returns {(doc: unknown) => void}
 */
function mapper(mapFun, emit) {
	// for temp_views one can use emit(doc, emit), see #38
	if (typeof mapFun === 'function' && mapFun.length === 2) {
		return doc => mapFun(doc, emit);
	}

	return evalFunction(mapFun.toString(), emit);
}

/**
 * @param {Function} reduceFun
 * @returns {unknown}
 */
function reducer(reduceFun) {
	return evalFunction(reduceFun.toString());
}

/**
 * @param {{ key: unknown, value: unknown }} x
 * @param {{ key: unknown, value: unknown }} y
 * @returns {number}
 */
function sortByKeyThenValue(x, y) {
	const keyCompare = collate(x.key, y.key);

	return keyCompare !== 0 ? keyCompare : collate(x.value, y.value);
}

/**
 * @template T
 * @param {T[]} results
 * @param {number | undefined} limit
 * @param {number | undefined} skip
 * @returns {T[]}
 */
function sliceResults(results, limit, skip) {
	skip = skip != null && Number.isFinite(skip) && skip > 0 ? skip : 0;

	if (limit != null && Number.isFinite(limit) && limit > 0) {
		return results.slice(skip, limit + skip);
	}

	if (skip > 0) {
		return results.slice(skip);
	}

	return results;
}

/**
 * @param {{ id: string, value: unknown }} row
 * @returns {string}
 */
function rowToDocId(row) {
	const val = row.value;
	// Users can explicitly specify a joined doc _id, or it
	// defaults to the doc _id that emitted the key/value.
	const docId = (val && typeof val === 'object' && val._id) ?? row.id;

	return docId;
}

// returns a promise for a list of docs to update, based on the input docId.
// the order doesn't matter, because post-3.2.0, bulkDocs
// is an atomic operation in all three adapters.
async function getDocsToPersist(docId, view, docIdsToChangesAndEmits) {
	const metaDocId = `_local/doc_${docId}`;
	const defaultMetaDoc = { _id: metaDocId, keys: [] };
	const docData = docIdsToChangesAndEmits.get(docId);
	const indexableKeysToKeyValues = docData[0];
	const changes = docData[1];

	const metaDoc = isGenOne(changes)
		? defaultMetaDoc
		: await view.db.get(metaDocId).catch(handleNotFound(defaultMetaDoc));

	const keyValueDocs = metaDoc.keys.length
		? await view.db.allDocs({
			keys: metaDoc.keys,
			include_docs: true
		})
		: { rows: [] };

	const kvDocs = [];
	const oldKeys = new Set();

	for (const row of keyValueDocs.rows) {
		const { doc } = row;

		if (!doc) { // deleted
			continue;
		}

		kvDocs.push(doc);
		oldKeys.add(doc._id);
		doc._deleted = !indexableKeysToKeyValues.has(doc._id);

		if (!doc._deleted) {
			const keyValue = indexableKeysToKeyValues.get(doc._id);

			if ('value' in keyValue) {
				doc.value = keyValue.value;
			}
		}
	}

	const newKeys = mapToKeysArray(indexableKeysToKeyValues);

	for (const key of newKeys) {
		if (!oldKeys.has(key)) {
			// new doc
			const kvDoc = {
				_id: key
			};
			const keyValue = indexableKeysToKeyValues.get(key);

			if ('value' in keyValue) {
				kvDoc.value = keyValue.value;
			}

			kvDocs.push(kvDoc);
		}
	}

	metaDoc.keys = uniq(newKeys.concat(metaDoc.keys));
	kvDocs.push(metaDoc);

	return kvDocs;
}

/**
 * @param {View} view
 */
async function updatePurgeSeq(view) {
	try {
		const result = await view.sourceDB.get('_local/purges');

		const { purgeSeq } = result;

		const res = await view.db.get('_local/purgeSeq').catch(handleNotFound());

		await view.db.put({
			_id: '_local/purgeSeq',
			_rev: res?._rev,
			purgeSeq
		});
	} catch (/** @type {any} */e) {
		if (e.status !== 404) {
			throw e;
		}
	}
}

// updates all emitted key/value docs and metaDocs in the mrview database
// for the given batch of documents from the source database
async function saveKeyValues(view, docIdsToChangesAndEmits, seq) {
	const lastSeqDoc = await view.db.get('_local/lastSeq').catch(handleNotFound());

	const docIds = mapToKeysArray(docIdsToChangesAndEmits);

	const listOfDocsToPersist = await Promise.all(docIds.map(docId => getDocsToPersist(docId, view, docIdsToChangesAndEmits)));

	const docsToPersist = listOfDocsToPersist.flat();
	lastSeqDoc.seq = seq;
	docsToPersist.push({
		...lastSeqDoc,
		_id: '_local/lastSeq',
		seq
	});

	await view.db.bulkDocs({ docs: docsToPersist });

	await updatePurgeSeq(view);
}

/**
 * @param {string} viewName
 * @returns {TaskQueue}
 */
function getQueue(viewName) {
	queues[viewName] ??= new TaskQueue();

	return queues[viewName];
}

/**
 *
 * @param {View} view
 */
async function updateView(view) {
	let currentSeq = view.seq ?? 0;

	// bind the emit function once
	let mapResults;
	let doc;

	const mapFun = mapper(view.mapFun, (key, value) => {
		mapResults.push({
			id: doc._id,
			key: normalizeKey(key),
			value: value != null
				? normalizeKey(value)
				: undefined
		});
	});

	function processChange(docIdsToChangesAndEmits, seq) {
		return function () {
			return saveKeyValues(view, docIdsToChangesAndEmits, seq);
		};
	}

	const queue = new TaskQueue();

	async function processNextBatch() {
		const response = await view.sourceDB.changes({
			return_docs: true,
			conflicts: true,
			include_docs: true,
			style: 'all_docs',
			since: currentSeq,
			limit: CHANGES_BATCH_SIZE
		});

		const purges = await getRecentPurges();

		return processBatch(response, purges);
	}

	async function getRecentPurges() {
		/** @type {[{ purgeSeq: number } | undefined, { purges: Array<{ docId: string }> } | undefined} */
		const [purge, purges] = await Promise.all([
			view.db.get('_local/purgeSeq').catch(handleNotFound()),
			view.sourceDB.get('_local/purges').catch(handleNotFound())
		]);

		if (purges == null) {
			return [];
		}

		const purgeSeq = purge != null ? purge.purgeSeq : -1;

		const recentPurges = purges.purges
			.filter((purge, index) => index > purgeSeq)
			.map(purge => purge.docId);

		const uniquePurges = recentPurges
			.filter((docId, index) => recentPurges.indexOf(docId) === index);

		return Promise.all(uniquePurges.map(async docId => {
			const doc = await view.sourceDB.get(docId).catch(handleNotFound());

			return { docId, doc };
		}));
	}

	function processBatch(response, purges) {
		const { results } = response;

		if (!results.length && !purges.length) {
			return;
		}

		for (const purge of purges) {
			const index = results.findIndex(change => change.id === purge.docId);

			if (index < 0) {
				// mimic a db.remove() on the changes feed
				const entry = {
					_id: purge.docId,
					doc: {
						_id: purge.docId,
						_deleted: 1
					},
					changes: []
				};

				if (purge.doc) {
					// update with new winning rev after purge
					entry.doc = purge.doc;
					entry.changes.push({ rev: purge.doc._rev });
				}

				results.push(entry);
			}
		}

		const docIdsToChangesAndEmits = createDocIdsToChangesAndEmits(results);

		queue.add(processChange(docIdsToChangesAndEmits, currentSeq));

		indexed_docs += results.length;

		if (results.length < CHANGES_BATCH_SIZE) {
			return;
		}

		return processNextBatch();
	}

	function createDocIdsToChangesAndEmits(results) {
		const docIdsToChangesAndEmits = new Map();

		for (const change of results) {
			if (change.doc._id[0] !== '_') {
				mapResults = [];
				doc = change.doc;

				if (!doc._deleted) {
					mapFun(doc);
				}

				mapResults.sort(sortByKeyThenValue);

				const indexableKeysToKeyValues = createIndexableKeysToKeyValues(mapResults);
				docIdsToChangesAndEmits.set(change.doc._id, [
					indexableKeysToKeyValues,
					change.changes
				]);
			}

			currentSeq = change.seq;
		}

		return docIdsToChangesAndEmits;
	}

	function createIndexableKeysToKeyValues(mapResults) {
		const indexableKeysToKeyValues = new Map();
		let lastKey;

		for (let i = 0, len = mapResults.length; i < len; i++) {
			const emittedKeyValue = mapResults[i];
			const complexKey = [emittedKeyValue.key, emittedKeyValue.id];

			if (i > 0 && collate(emittedKeyValue.key, lastKey) === 0) {
				complexKey.push(i); // dup key+id, so make it unique
			}

			indexableKeysToKeyValues.set(toIndexableString(complexKey), emittedKeyValue);
			lastKey = emittedKeyValue.key;
		}

		return indexableKeysToKeyValues;
	}

	await processNextBatch();
	await queue.finish();
	view.seq = currentSeq;
}

function reduceView(view, results, options) {
	if (options.group_level === 0) {
		delete options.group_level;
	}

	const shouldGroup = options.group || options.group_level;
	const reduceFun = reducer(view.reduceFun);
	const groups = [];
	const lvl = !Number.isFinite(options.group_level)
		? Number.POSITIVE_INFINITY
		: options.group_level;

	for (const result of results) {
		const last = groups[groups.length - 1];
		let groupKey = shouldGroup ? result.key : null;

		// only set group_level for array keys
		if (shouldGroup && Array.isArray(groupKey)) {
			groupKey = groupKey.slice(0, lvl);
		}

		if (last && collate(last.groupKey, groupKey) === 0) {
			last.keys.push([result.key, result.id]);
			last.values.push(result.value);
			continue;
		}

		groups.push({
			keys: [[result.key, result.id]],
			values: [result.value],
			groupKey
		});
	}

	results = groups.map(group => ({
		value: reduceFun(group.keys, group.values, false),
		key: group.groupKey
	}));

	return { rows: sliceResults(results, options.limit, options.skip) };
}

/**
 * @param {View} view
 * @param {{ reduce?: boolean, skip?: number, keys?: string[], limit?: number }} opts
 * @returns
 */
async function queryViewInQueue(view, opts) {
	const shouldReduce = view.reduceFun && opts.reduce !== false;
	const skip = opts.skip || 0;

	if (typeof opts.keys !== 'undefined' && !opts.keys.length) {
		// equivalent query
		opts.limit = 0;
		delete opts.keys;
	}

	let totalRows;

	async function fetchFromView(viewOpts) {
		viewOpts.include_docs = true;
		const res = await view.db.allDocs(viewOpts);
		totalRows = res.total_rows;

		return res.rows.map(result => {
			// implicit migration - in older versions of PouchDB,
			// we explicitly stored the doc as {id: ..., key: ..., value: ...}
			// this is tested in a migration test
			/* istanbul ignore next */
			if ('value' in result.doc && typeof result.doc.value === 'object'
				&& result.doc.value !== null) {
				const keys = Object.keys(result.doc.value).sort();
				// this detection method is not perfect, but it's unlikely the user
				// emitted a value which was an object with these 3 exact keys
				const expectedKeys = ['id', 'key', 'value'];

				if (!(keys < expectedKeys || keys > expectedKeys)) {
					return result.doc.value;
				}
			}

			const parsedKeyAndDocId = parseIndexableString(result.doc._id);

			return {
				key: parsedKeyAndDocId[0],
				id: parsedKeyAndDocId[1],
				value: ('value' in result.doc ? result.doc.value : null)
			};
		});
	}

	async function onMapResultsReady(rows) {
		let finalResults;

		if (shouldReduce) {
			finalResults = reduceView(view, rows, opts);
		} else if (typeof opts.keys === 'undefined') {
			finalResults = {
				total_rows: totalRows,
				offset: skip,
				rows
			};
		} else {
			// support limit, skip for keys query
			finalResults = {
				total_rows: totalRows,
				offset: skip,
				rows: sliceResults(rows, opts.limit, opts.skip)
			};
		}

		/* istanbul ignore if */
		if (opts.update_seq) {
			finalResults.update_seq = view.seq;
		}

		if (opts.include_docs) {
			const docIds = uniq(rows.map(rowToDocId));

			const allDocsRes = await view.sourceDB.allDocs({
				keys: rows.map(rowToDocId),
				include_docs: true,
				conflicts: opts.conflicts,
				attachments: opts.attachments,
				binary: opts.binary
			});
			const docIdsToDocs = new Map();

			for (const row of allDocsRes.rows) {
				docIdsToDocs.set(row.id, row.doc);
			}

			for (const row of rows) {
				const docId = rowToDocId(row);
				const doc = docIdsToDocs.get(docId);

				if (doc) {
					row.doc = doc;
				}
			}
		}

		return finalResults;
	}

	if (typeof opts.keys !== 'undefined') {
		const { keys } = opts;
		const fetchPromises = keys.map(key => {
			const viewOpts = {
				startkey: toIndexableString([key]),
				endkey: toIndexableString([key, {}])
			};

			/* istanbul ignore if */
			if (opts.update_seq) {
				viewOpts.update_seq = true;
			}

			return fetchFromView(viewOpts);
		});
		const result = await Promise.all(fetchPromises);
		const flattenedResult = result.flat();

		return onMapResultsReady(flattenedResult);
	} // normal query, no 'keys'

	const viewOpts = {
		descending: opts.descending
	};

	/* istanbul ignore if */
	if (opts.update_seq) {
		viewOpts.update_seq = true;
	}

	if (typeof opts.startkey !== 'undefined') {
		viewOpts.startkey = opts.descending
			? toIndexableString([opts.startkey, {}])
			: toIndexableString([opts.startkey]);
	}

	if (typeof opts.endkey !== 'undefined') {
		let inclusiveEnd = opts.inclusive_end !== false;

		if (opts.descending) {
			inclusiveEnd = !inclusiveEnd;
		}

		viewOpts.endkey = toIndexableString(
			inclusiveEnd ? [opts.endkey, {}] : [opts.endkey]
		);
	}

	if (typeof opts.key !== 'undefined') {
		const keyStart = toIndexableString([opts.key]);
		const keyEnd = toIndexableString([opts.key, {}]);

		if (viewOpts.descending) {
			viewOpts.endkey = keyStart;
			viewOpts.startkey = keyEnd;
		} else {
			viewOpts.startkey = keyStart;
			viewOpts.endkey = keyEnd;
		}
	}

	if (!shouldReduce) {
		if (typeof opts.limit === 'number') {
			viewOpts.limit = opts.limit;
		}

		viewOpts.skip = skip;
	}

	const result = await fetchFromView(viewOpts);

	return onMapResultsReady(result);
}

/**
 * @param {PouchDB} db
 * @param {{ map: Function, reduce?: Function } | string} fun
 * @param {object} opts
 */
export async function query(db, fun, opts) {
	if (typeof fun !== 'string') {
		return getQueue('temp_view/temp_view').add(async () => {
			const { map, reduce } = typeof fun === 'function'
				? { map: fun }
				: fun;

			const view = await createView(db, 'temp_view/temp_view', map, reduce);

			try {
				await updateView(view);

				return await queryViewInQueue(view, opts);
			} finally {
				await view.db.destroy();
			}
		});
	}

	const [designDocName, viewName] = parseViewName(fun);
	const fullViewName = `${designDocName}/${viewName}`;

	return getQueue(fullViewName).add(async () => {
		const doc = await db.get(`_design/${designDocName}`);

		if (!doc.views[viewName]) {
			throw new Error(`ddoc ${doc._id} has no view named ${viewName}`);
		}

		const { map, reduce } = doc.views[viewName];

		const view = await createView(db, fullViewName, map, reduce);

		await updateView(view);

		return queryViewInQueue(view, opts);
	});
}

/**
 * @this {PouchDB}
 * @returns {Promise<{ ok: true }>}
 */
export async function viewCleanup() {
	try {
		const metaDoc = await this.get('_local/mrviews');
		const docsToViews = new Map();

		for (const fullViewName of Object.keys(metaDoc.views)) {
			const parts = parseViewName(fullViewName);
			const designDocName = `_design/${parts[0]}`;
			const viewName = parts[1];
			let views = docsToViews.get(designDocName);

			if (!views) {
				views = new Set();
				docsToViews.set(designDocName, views);
			}

			views.add(viewName);
		}

		const opts = {
			keys: mapToKeysArray(docsToViews),
			include_docs: true
		};

		const res = await this.allDocs(opts);
		/** @type {Record<string, boolean>} */
		const viewsToStatus = {};

		for (const row of res.rows) {
			const ddocName = row.key.substring(8); // cuts off '_design/'

			for (const viewName of docsToViews.get(row.key)) {
				let fullViewName = `${ddocName}/${viewName}`;

				/* istanbul ignore if */
				if (!metaDoc.views[fullViewName]) {
					// new format, without slashes, to support PouchDB 2.2.0
					// migration test in pouchdb's browser.migration.js verifies this
					fullViewName = viewName;
				}

				const viewDBNames = Object.keys(metaDoc.views[fullViewName]);
				// design doc deleted, or view function nonexistent
				const statusIsGood = row.doc && row.doc.views && row.doc.views[viewName];

				for (const viewDBName of viewDBNames) {
					viewsToStatus[viewDBName] ??= !!statusIsGood;
				}
			}
		}

		const dbsToDelete = Object.keys(viewsToStatus).filter(viewDBName => !viewsToStatus[viewDBName]);

		const destroyPromises = dbsToDelete.map(viewDBName => getQueue(viewDBName).add(() => new this.constructor(viewDBName, this.__opts).destroy()));

		await Promise.all(destroyPromises);
	} catch (/** @type {any} */err) {
		if (err.status !== 404) {
			throw err;
		}
	}

	return { ok: true };
}
