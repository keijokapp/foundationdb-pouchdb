import foundationdbAdapter from './adapter.js';

/**
 * @this {unknown}
 * @param {{
 *   db: import('@arbendium/foundationdb').Database | import('@arbendium/foundationdb').Transaction
 *   name: string
 *   revs_limit?: number
 * }} opts
 * @param {Function} callback
 */
function FoundationdbAdapter(opts, callback) {
	callback(undefined, foundationdbAdapter(this, opts));
}

FoundationdbAdapter.valid = function () {
	return true;
};

FoundationdbAdapter.use_prefix = false;

/**
 * @param {any} PouchDB
 */
export default function FoundationdbPlugin(PouchDB) {
	PouchDB.adapter('foundationdb', FoundationdbAdapter, true);
}
