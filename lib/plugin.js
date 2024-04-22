import foundationdbAdapter from './adapter.js';

function FoundationdbAdapter(opts, callback) {
	callback(undefined, foundationdbAdapter(this, opts));
}

FoundationdbAdapter.valid = function () {
	return true;
};

FoundationdbAdapter.use_prefix = false;

export default function FoundationdbPlugin(PouchDB) {
	PouchDB.adapter('foundationdb', FoundationdbAdapter, true);
}
