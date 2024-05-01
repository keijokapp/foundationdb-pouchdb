import assert from 'assert';
import { MISSING_DOC, REV_CONFLICT, createError } from 'pouchdb-errors';
import * as pouchdbMerge from 'pouchdb-merge';
import parseDoc from './parseDoc.js';
import { getIsDeleted, getWinningRev, isLocalDoc } from './utils.js';

export default async function processDocs(
	revLimit,
	docInfos,
	api,
	fetchedDocs,
	tx,
	writeDoc,
	opts
) {
	// Default to 1000 locally
	revLimit ??= 1000;

	const results = [];

	const pairs = docInfos.map((doc, i) => [i, doc]);
	const localDocs = pairs.filter(
		pair => isLocalDoc(pair[1])
	);
	const idsToDocs = Object.groupBy(
		pairs.filter(pair => !isLocalDoc(pair[1])),
		([, currentDoc]) => currentDoc.metadata.id
	);

	await Promise.all([
		Promise.all(localDocs.map(([i, currentDoc]) => {
			const fun = currentDoc._deleted ? '_removeLocal' : '_putLocal';

			return new Promise(resolve => {
				api[fun](currentDoc, { ctx: tx }, (e, res) => {
					results[i] = e ?? /** @type {NonNullable<typeof res>} */(res);

					resolve(undefined);
				});
			});
		})),
		Promise.all(Object.values(idsToDocs).map(async docs => {
			for (const [i, currentDoc] of /** @type {NonNullable<typeof docs>} */(docs)) {
				results[i] = await handleDoc(
					fetchedDocs,
					currentDoc.metadata.id,
					currentDoc,
					revLimit,
					writeDoc,
					opts
				);
			}
		}))
	]);

	return results;
}

async function handleDoc(fetchedDocs, id, docInfo, revLimit, writeDoc, opts) {
	const newEdits = opts.new_edits;

	if (fetchedDocs.has(id)) {
		const prev = fetchedDocs.get(id);

		if (pouchdbMerge.revExists(prev.rev_tree, docInfo.metadata.rev) && !newEdits) {
			// This response is not actually used by PouchDB (it's skipped), but it needs
			// to be something dereferencable without an `error` property.
			return {};
		}

		// sometimes this is pre-calculated. historically not always
		const previousWinningRev = getWinningRev(prev);
		const previouslyDeleted = getIsDeleted(prev, previousWinningRev);
		const deleted = getIsDeleted(docInfo.metadata);
		const isRoot = /^1-/.test(docInfo.metadata.rev);

		if (previouslyDeleted && !deleted && newEdits && isRoot) {
			const newDoc = docInfo.data;
			newDoc._rev = previousWinningRev;
			newDoc._id = docInfo.metadata.id;
			docInfo = /** @type {import('./types.js').DocInfo} */(parseDoc(newDoc, newEdits));
		}

		assert(docInfo.metadata.rev_tree);

		const merged = pouchdbMerge.merge(prev.rev_tree, docInfo.metadata.rev_tree[0], revLimit);

		const inConflict = newEdits && (
			(previouslyDeleted && deleted && merged.conflicts !== 'new_leaf')
				|| (!previouslyDeleted && merged.conflicts !== 'new_leaf')
				|| (previouslyDeleted && !deleted && merged.conflicts === 'new_branch')
		);

		if (inConflict) {
			return createError(REV_CONFLICT);
		}

		const newRev = docInfo.metadata.rev;
		docInfo.metadata.rev_tree = merged.tree;
		docInfo.stemmedRevs = merged.stemmedRevs ?? [];

		if (prev.rev_map) {
			docInfo.metadata.rev_map = prev.rev_map; // used only by leveldb
		}

		// recalculate
		const winningRev = pouchdbMerge.winningRev(docInfo.metadata);
		const winningRevIsDeleted = pouchdbMerge.isDeleted(docInfo.metadata, winningRev);

		// calculate the total number of documents that were added/removed,
		// from the perspective of total_rows/doc_count
		// eslint-disable-next-line no-nested-ternary
		const delta = (previouslyDeleted === winningRevIsDeleted) ? 0
			: previouslyDeleted < winningRevIsDeleted ? -1 : 1;

		let newRevIsDeleted;
		if (newRev === winningRev) {
			// if the new rev is the same as the winning rev, we can reuse that value
			newRevIsDeleted = winningRevIsDeleted;
		} else {
			// if they're not the same, then we need to recalculate
			newRevIsDeleted = pouchdbMerge.isDeleted(docInfo.metadata, newRev);
		}

		return writeDoc(
			docInfo,
			winningRev,
			winningRevIsDeleted,
			newRevIsDeleted,
			true,
			delta
		);
	}

	// Ensure stemming applies to new writes as well
	const merged = pouchdbMerge.merge([], docInfo.metadata.rev_tree[0], revLimit);
	docInfo.metadata.rev_tree = merged.tree;
	docInfo.stemmedRevs = merged.stemmedRevs ?? [];

	// Cant insert new deleted documents
	const winningRev = pouchdbMerge.winningRev(docInfo.metadata);
	const deleted = pouchdbMerge.isDeleted(docInfo.metadata, winningRev);

	if ('was_delete' in opts && deleted) {
		return createError(MISSING_DOC, 'deleted');
	}

	// 4712 - detect whether a new document was inserted with a _rev
	const inConflict = newEdits && docInfo.metadata.rev_tree[0].ids[1].status === 'missing';

	if (inConflict) {
		return createError(REV_CONFLICT);
	}

	const delta = deleted ? 0 : 1;

	return writeDoc(
		docInfo,
		winningRev,
		deleted,
		deleted,
		false,
		delta
	);
}
