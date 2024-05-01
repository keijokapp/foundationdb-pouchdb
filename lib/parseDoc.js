import {
	DOC_VALIDATION,
	INVALID_REV,
	createError
} from 'pouchdb-errors';
import * as pouchdbUtils from 'pouchdb-utils';
import { parseRevision } from './utils.js';

const KEYWORD_IGNORE = 0;
const KEYWORD_METADATA = 1;
const KEYWORD_DATA = 2;

// List of top level reserved words for doc
const reservedWords = {
	_id: KEYWORD_METADATA,
	_rev: KEYWORD_METADATA,
	_access: KEYWORD_DATA,
	_attachments: KEYWORD_DATA,
	_deleted: KEYWORD_METADATA,
	_revisions: KEYWORD_IGNORE,
	_revs_info: KEYWORD_IGNORE,
	_conflicts: KEYWORD_IGNORE,
	_deleted_conflicts: KEYWORD_IGNORE,
	_local_seq: KEYWORD_IGNORE,
	_rev_tree: KEYWORD_METADATA,
	// replication documents
	_replication_id: KEYWORD_DATA,
	_replication_state: KEYWORD_DATA,
	_replication_state_time: KEYWORD_DATA,
	_replication_state_reason: KEYWORD_DATA,
	_replication_stats: KEYWORD_DATA,
	// Specific to Couchbase Sync Gateway
	_removed: KEYWORD_IGNORE
};

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
			switch (reservedWords[key]) {
			case KEYWORD_METADATA:
				metadata[key.slice(1)] = doc[key];
				break;
			case KEYWORD_DATA:
				data[key] = doc[key];
				break;
			case KEYWORD_IGNORE:
				break;
			default:
				if (key[0] === '_') {
					const error = createError(DOC_VALIDATION, key);
					error.message = `${DOC_VALIDATION.message}: ${key}`;
					throw error;
				} else {
					data[key] = doc[key];
				}
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
	} else if (doc._revisions != null) {
		metadata.rev_tree = makeRevTreeFromRevisions(doc._revisions, opts);
		nRevNum = doc._revisions.start;
		[newRevId] = doc._revisions.ids;
	} else if (metadata.rev_tree == null) {
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
	} else {
		nRevNum = metadata.rev_tree[0].pos;
		[newRevId] = metadata.rev_tree[0].ids;
	}

	pouchdbUtils.invalidIdError(metadata.id);

	metadata.rev = `${nRevNum}-${newRevId}`;
	data._id = metadata.id;
	data._rev = metadata.rev;

	return { metadata, data };
}
