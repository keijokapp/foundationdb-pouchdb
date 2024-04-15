// @ts-check

/**
 * @typedef {any} Database
 * @typedef {any} Store
 * @typedef {any} Operation
 * @typedef {any} Key
 * @typedef {any} Value
 */

/**
 * @param {LevelTransaction} transaction
 * @param {Store} store
 * @returns {Map<Key, Value>}
 */
function getCacheFor(transaction, store) {
	const prefix = store.prefix()[0];
	const cache = transaction._cache;
	let subCache = cache.get(prefix);
	if (!subCache) {
		subCache = new Map();
		cache.set(prefix, subCache);
	}

	return subCache;
}

export default class LevelTransaction {
	constructor() {
		this._batch = /** @type {Operation[]} */([]);
		this._cache = new Map();
	}

	/**
	 * @param {Store} store
	 * @param {Key} key
	 * @returns {Promise<Value>}
	 */
	async get(store, key) {
		const cache = getCacheFor(this, store);
		const exists = cache.get(key);

		if (exists != null) {
			return exists;
		}

		if (exists === null) { // deleted marker
			/* istanbul ignore next */
			// eslint-disable-next-line no-throw-literal
			throw { name: 'NotFoundError' };
		}

		const res = await store.get(key).catch(/** @param {any} e */e => {
			if (e.name === 'NotFoundError') {
				cache.set(key, null);
			}

			throw e;
		});

		cache.set(key, res);

		return res;
	}

	/**
	 * @param {Operation[]} batch
	 */
	batch(batch) {
		for (const operation of batch) {
			const cache = getCacheFor(this, operation.prefix);

			cache.set(operation.key, operation.type === 'put' ? operation.value : null);
		}

		this._batch.push(...batch);
	}

	/**
	 * @param {Database} db
	 */
	async execute(db) {
		const keys = new Set();
		const uniqBatches = [];

		// remove duplicates; last one wins
		for (let i = this._batch.length - 1; i >= 0; i--) {
			const operation = this._batch[i];
			const lookupKey = `${operation.prefix.prefix()[0]}\xff${operation.key}`;
			if (keys.has(lookupKey)) {
				continue;
			}
			keys.add(lookupKey);
			uniqBatches.push(operation);
		}

		await db.batch(uniqBatches);
	}
}
