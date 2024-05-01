import {
	DOC_VALIDATION,
	INVALID_REV,
	createError
} from 'pouchdb-errors';
import * as pouchdbUtils from 'pouchdb-utils';
import { parseRevision } from './utils.js';

// List of top level reserved words for doc
const reservedWords = new Set([
	'_id',
	'_rev',
	'_access',
	'_attachments',
	'_deleted',
	'_revisions',
	'_revs_info',
	'_conflicts',
	'_deleted_conflicts',
	'_local_seq',
	'_rev_tree',
	// replication documents
	'_replication_id',
	'_replication_state',
	'_replication_state_time',
	'_replication_state_reason',
	'_replication_stats',
	// Specific to Couchbase Sync Gateway
	'_removed'
]);

// List of reserved words that should end up in the document
const dataWords = new Set([
	'_access',
	'_attachments',
	// replication documents
	'_replication_id',
	'_replication_state',
	'_replication_state_time',
	'_replication_state_reason',
	'_replication_stats'
]);

function parseRevisionInfo(rev) {
	if (!/^\d+-/.test(rev)) {
		return createError(INVALID_REV);
	}

	return parseRevision(/** @type {import('./types.js').Rev} */(rev));
}

function makeRevTreeFromRevisions(revisions, opts) {
	const pos = revisions.start - revisions.ids.length + 1;

	const revisionIds = revisions.ids;
	let ids = [revisionIds[0], opts, []];

	for (let i = 1, len = revisionIds.length; i < len; i++) {
		ids = [revisionIds[i], { status: 'missing' }, [ids]];
	}

	return [{
		pos,
		ids
	}];
}

export default function parseDoc(
	doc,
	newEdits,
	{ deterministic_revs: deterministicRevs = true } = {}
) {
	const metadata = {};
	const data = {};

	for (const key in doc) {
		if (Object.prototype.hasOwnProperty.call(doc, key)) {
			const specialKey = key[0] === '_';
			if (specialKey && !reservedWords.has(key)) {
				const error = createError(DOC_VALIDATION, key);
				error.message = `${DOC_VALIDATION.message}: ${key}`;
				throw error;
			} else if (specialKey && !dataWords.has(key)) {
				metadata[key.slice(1)] = doc[key];
			} else {
				data[key] = doc[key];
			}
		}
	}

	let nRevNum;
	let newRevId;
	const opts = {
		status: 'available',
		...metadata.deleted ? { deleted: true } : {}
	};

	if (newEdits) {
		if (metadata.id == null) {
			metadata.id = pouchdbUtils.uuid();
		}

		newRevId = pouchdbUtils.rev(
			!deterministicRevs || doc._id
				? doc
				: { ...doc, _id: metadata.id },
			deterministicRevs
		);

		if (metadata.rev != null) {
			const revInfo = parseRevisionInfo(metadata.rev);

			if ('error' in revInfo) {
				return revInfo;
			}

			metadata.rev_tree = [{
				pos: revInfo.prefix,
				ids: [revInfo.id, { status: 'missing' }, [[newRevId, opts, []]]]
			}];

			nRevNum = revInfo.prefix + 1;
		} else {
			metadata.rev_tree = [{
				pos: 1,
				ids: [newRevId, opts, []]
			}];
			nRevNum = 1;
		}
	} else {
		if (metadata.revisions) {
			metadata.rev_tree = makeRevTreeFromRevisions(metadata.revisions, opts);
			nRevNum = metadata.revisions.start;
			[newRevId] = metadata.revisions.ids;
		}

		if (!metadata.rev_tree) {
			const revInfo = parseRevisionInfo(metadata.rev);

			if ('error' in revInfo) {
				return revInfo;
			}

			nRevNum = revInfo.prefix;
			newRevId = revInfo.id;
			metadata.rev_tree = [{
				pos: nRevNum,
				ids: [newRevId, opts, []]
			}];
		}
	}

	pouchdbUtils.invalidIdError(metadata.id);

	metadata.rev = `${nRevNum}-${newRevId}`;

	return { metadata, data };
}
