import leveldown from 'leveldown';
import leveldbAdapter from './adapter.js';
import * as migrate from './migrate.js';

function LeveldbAdapter(opts, callback) {
	leveldbAdapter.call(this, { db: leveldown, migrate, ...opts }).then(
		value => callback(undefined, value),
		callback
	);
}

LeveldbAdapter.valid = function () {
	return true;
};

LeveldbAdapter.use_prefix = false;

export default function LeveldbPlugin(PouchDB) {
	PouchDB.adapter('leveldb', LeveldbAdapter, true);
}
