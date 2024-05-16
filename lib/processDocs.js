import crypto from 'crypto';
import {
	BAD_ARG, MISSING_DOC, REV_CONFLICT, createError
} from 'pouchdb-errors';
import * as pouchdbMerge from 'pouchdb-merge';
import parseDoc from './parseDoc.js';
import { getIsDeleted, getWinningRev, parseRevision } from './utils.js';

/**
 * @param {import('./types.js').Metadata} prev
 * @param {import('./types.js').DocInfo} docInfo
 * @param {number} revLimit
 * @param {{ new_edits?: boolean }} opts
 * @returns {{ docInfo: import('./types.js').DocInfo, delta: number } | undefined}
 */
export function handleUpdatedDoc(prev, docInfo, revLimit, opts) {
	const newEdits = opts.new_edits;

	if (pouchdbMerge.revExists(prev.rev_tree, docInfo.metadata.rev) && !newEdits) {
		return;
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

	const merged = pouchdbMerge.merge(prev.rev_tree, docInfo.metadata.rev_tree[0], revLimit);

	const inConflict = newEdits && (
		(previouslyDeleted && deleted && merged.conflicts !== 'new_leaf')
			|| (!previouslyDeleted && merged.conflicts !== 'new_leaf')
			|| (previouslyDeleted && !deleted && merged.conflicts === 'new_branch')
	);

	if (inConflict) {
		throw createError(REV_CONFLICT);
	}

	const { rev } = docInfo.metadata;
	docInfo.metadata.rev_tree = merged.tree;
	docInfo.stemmedRevs = merged.stemmedRevs;

	if (prev.rev_map) {
		docInfo.metadata.rev_map = prev.rev_map; // used only by leveldb
	}

	// recalculate
	const winningRev = pouchdbMerge.winningRev(docInfo.metadata);
	const winningRevIsDeleted = pouchdbMerge.isDeleted(docInfo.metadata, winningRev);

	// calculate the total number of documents that were added/removed,
	// from the perspective of total_rows/doc_count
	const delta = +previouslyDeleted - +winningRevIsDeleted;

	const newRevIsDeleted = rev === winningRev
		// if the new rev is the same as the winning rev, we can reuse that value
		? winningRevIsDeleted
		// if they're not the same, then we need to recalculate
		: pouchdbMerge.isDeleted(docInfo.metadata, rev);

	docInfo = transformDoc(
		docInfo,
		winningRev,
		winningRevIsDeleted,
		newRevIsDeleted
	);

	return { docInfo, delta };
}

/**
 * @param {import('./types.js').DocInfo} docInfo
 * @param {number} revLimit
 * @param {{ new_edits?: boolean, was_delete?: unknown }} opts
 * @returns {{ docInfo: import('./types.js').DocInfo, delta: number }}
 */
export function handleNewDoc(docInfo, revLimit, opts) {
	const newEdits = opts.new_edits;

	// Ensure stemming applies to new writes as well
	const merged = pouchdbMerge.merge([], docInfo.metadata.rev_tree[0], revLimit);
	docInfo.metadata.rev_tree = merged.tree;
	docInfo.stemmedRevs = merged.stemmedRevs;

	// Cant insert new deleted documents
	const winningRev = pouchdbMerge.winningRev(docInfo.metadata);
	const deleted = pouchdbMerge.isDeleted(docInfo.metadata, winningRev);

	if ('was_delete' in opts && deleted) {
		throw createError(MISSING_DOC, 'deleted');
	}

	// 4712 - detect whether a new document was inserted with a _rev
	const inConflict = newEdits && docInfo.metadata.rev_tree[0].ids[1].status === 'missing';

	if (inConflict) {
		throw createError(REV_CONFLICT);
	}

	docInfo = transformDoc(docInfo, winningRev, deleted, deleted);

	const delta = deleted ? 0 : 1;

	return { docInfo, delta };
}

/**
 * @param {import('./types.js').DocInfo} docInfo
 * @param {import('./types.js').Rev} winningRev
 * @param {boolean} winningRevIsDeleted
 * @param {boolean} newRevIsDeleted
 * @returns {import('./types.js').DocInfo}
 */
function transformDoc(
	docInfo,
	winningRev,
	winningRevIsDeleted,
	newRevIsDeleted
) {
	const { rev } = docInfo.metadata;

	docInfo.metadata.winningRev = winningRev;
	docInfo.metadata.deleted = winningRevIsDeleted;

	if (newRevIsDeleted) {
		docInfo.data._deleted = true;
	}

	if (docInfo.data._attachments) {
		docInfo.attachments = [];

		Object.entries(docInfo.data._attachments).forEach(([key, att]) => {
			/** @type {Buffer | undefined} */
			let data;

			if (!att.stub) {
				if (typeof att.data === 'string') {
					// input is assumed to be a base64 string
					try {
						data = Buffer.from(att.data, 'base64');

						// Node.js will just skip the characters it can't decode instead of
						// throwing an exception
						if (data.toString('base64') !== att.data) {
							throw new Error('attachment is not a valid base64 string');
						}
					} catch {
						throw createError(
							BAD_ARG,
							'Attachment is not a valid base64 string'
						);
					}
				} else {
					data = /** @type {Buffer} */(att.data);
				}

				const digest = /** @type {import('./types.js').Digest} */(`md5-${crypto.createHash('md5').update(data).digest('base64')}`);

				att = {
					...att,
					digest,
					length: data.length,
					revpos: parseRevision(rev).prefix
				};

				delete att.data;
			}

			// @ts-ignore
			docInfo.data._attachments[key] = att;
			// @ts-ignore
			docInfo.attachments.push({
				digest: att.digest,
				data
			});
		});
	}

	return docInfo;
}
