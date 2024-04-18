// @ts-check

/**
 * @typedef {{
 *   batch: (batch: any[]) => Promise<void>,
 *   get(key: Key): Promise<Value>
 * }} LevelDatabase
 * @typedef {{ get(key: Key): Promise<any> }} Subspace
 * @typedef {{ key: Key, value: Value, prefix: Subspace, type: 'put' | 'del' }} Operation
 * @typedef {unknown} Key
 * @typedef {unknown} Value
 */

/**
 * @param {LevelTransaction} transaction
 * @param {Subspace} store
 * @returns {Map<Key, Value>}
 */
function getCacheFor(transaction, store) {
	const cache = transaction._cache;
	let subCache = cache.get(store);
	if (!subCache) {
		subCache = new Map();
		cache.set(store, subCache);
	}

	return subCache;
}

export default class LevelTransaction {
	constructor() {
		this._batch = /** @type {Operation[]} */([]);
		this._cache = new Map();
	}

	/**
	 * @param {Subspace} store
	 * @param {Key} key
	 * @returns {Promise<Value>}
	 */
	async get(store, key) {
		const cache = getCacheFor(this, store);

		if (cache.has(key)) {
			return cache.get(key);
		}

		const res = await store.get(key);

		cache.set(key, res);

		return res;
	}

	/**
	 * @param {Operation[]} batch
	 */
	batch(batch) {
		for (const operation of batch) {
			const cache = getCacheFor(this, operation.prefix);

			cache.set(operation.key, operation.type === 'put' ? operation.value : undefined);
		}

		this._batch.push(...batch);
	}

	/**
	 * @param {LevelDatabase} db
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
