import leveldown from 'leveldown';
import leveldbAdapter from './pouchdb-adapter-sitamaja/index.js';
import migrate from './migrate.js';

function LeveldbAdapter(opts, callback) {
	leveldbAdapter.call(this, { db: leveldown, migrate, ...opts }, callback);
}

LeveldbAdapter.valid = function () {
	return true;
};

LeveldbAdapter.use_prefix = false;

export default function LeveldbPlugin(PouchDB) {
	PouchDB.adapter('leveldb', LeveldbAdapter, true);
}
