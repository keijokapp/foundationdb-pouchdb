import {
	isDeleted,
	isLocalId,
	winningRev as calculateWinningRev
} from 'pouchdb-merge';

export function getWinningRev(metadata) {
	return metadata.winningRev ?? calculateWinningRev(metadata);
}

export function getIsDeleted(metadata, winningRev) {
	return metadata.deleted ?? isDeleted(metadata, winningRev);
}

export function isLocalDoc(doc) {
	return doc._id != null && isLocalId(doc._id);
}

export function parseRevision(rev) {
	const idx = rev.indexOf('-');
	const left = rev.slice(0, idx);
	const right = rev.slice(idx + 1);

	return {
		prefix: +left,
		id: right
	};
}
