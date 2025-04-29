import {
	isDeleted,
	isLocalId,
	winningRev as calculateWinningRev
} from 'pouchdb-merge';

/**
 * @param {import('./types.js').Metadata} metadata
 * @returns {import('./types.js').Rev}
 */
export function getWinningRev(metadata) {
	return metadata.winningRev ?? calculateWinningRev(metadata);
}

/**
 * @param {import('./types.js').Metadata} metadata
 * @param {import('./types.js').Rev} [winningRev]
 * @returns {boolean}
 */
export function getIsDeleted(metadata, winningRev) {
	return metadata.deleted ?? isDeleted(metadata, winningRev);
}

/**
 * @param {Record<any, any>} doc
 * @return {doc is import('./types.js').LocalDoc}
 */
export function isLocalDoc(doc) {
	return doc._id != null && isLocalId(doc._id);
}

/**
 * @param {import('./types.js').Rev} rev
 * @returns {{ prefix: import('./types.js').RevNum, id: import('./types.js').RevId }}
 */
export function parseRevision(rev) {
	const idx = rev.indexOf('-');
	const left = rev.slice(0, idx);
	const right = rev.slice(idx + 1);

	return {
		prefix: /** @type {import('./types.js').RevNum} */(+left),
		id: /** @type {import('./types.js').RevId} */(right)
	};
}

/**
 * @param {import('./types.js').LocalRev} rev
 * @returns {{ prefix: import('./types.js').RevNum, id: number }}
 */
export function parseLocalRevision(rev) {
	const idx = rev.indexOf('-');
	const left = rev.slice(0, idx);
	const right = rev.slice(idx + 1);

	return {
		prefix: /** @type {import('./types.js').RevNum} */(+left),
		id: +right
	};
}

/**
 * @template T
 * @param {Promise<T>} promise
 * @returns {Promise<NonNullable<T>>}
 */
export async function nonNullPromise(promise) {
	const value = await promise;

	if (value == null) {
		const e = new TypeError('value is nullish');

		Error.captureStackTrace(e, nonNullPromise);

		throw e;
	}

	return value;
}
