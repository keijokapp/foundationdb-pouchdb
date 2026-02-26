import {
	isDeleted,
	isLocalId,
	winningRev as calculateWinningRev
} from 'pouchdb-merge';

/**
 * @import {
 *   LocalDoc,
 *   LocalRev,
 *   Metadata,
 *   Rev,
 *   RevId,
 *   RevNum,
 * } from './types'
 */

/**
 * @param {Metadata} metadata
 * @returns {Rev}
 */
export function getWinningRev(metadata) {
	return metadata.winningRev ?? calculateWinningRev(metadata);
}

/**
 * @param {Metadata} metadata
 * @param {Rev} [winningRev]
 * @returns {boolean}
 */
export function getIsDeleted(metadata, winningRev) {
	return metadata.deleted ?? isDeleted(metadata, winningRev);
}

/**
 * @param {Record<any, any>} doc
 * @return {doc is LocalDoc}
 */
export function isLocalDoc(doc) {
	return doc._id != null && isLocalId(doc._id);
}

/**
 * @param {Rev} rev
 * @returns {{ prefix: RevNum, id: RevId }}
 */
export function parseRevision(rev) {
	const idx = rev.indexOf('-');
	const left = rev.slice(0, idx);
	const right = rev.slice(idx + 1);

	return {
		prefix: /** @type {RevNum} */(+left),
		id: /** @type {RevId} */(right)
	};
}

/**
 * @param {LocalRev} rev
 * @returns {{ prefix: RevNum, id: number }}
 */
export function parseLocalRevision(rev) {
	const idx = rev.indexOf('-');
	const left = rev.slice(0, idx);
	const right = rev.slice(idx + 1);

	return {
		prefix: /** @type {RevNum} */(+left),
		id: +right
	};
}
