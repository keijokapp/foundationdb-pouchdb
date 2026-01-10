import foundationdbAdapter from './adapter.js';

/**
 * @import { Database, Transaction } from 'foundationdb'
 */

/**
 * @this {unknown}
 * @param {{
 *   db: Database | Transaction
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
