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
		this._batch = [];
		this._cache = new Map();
	}

	async get(store, key) {
		const cache = getCacheFor(this, store);

		if (cache.has(key)) {
			return cache.get(key);
		}

		const res = await store.get(key);

		cache.set(key, res);

		return res;
	}

	batch(batch) {
		for (const operation of batch) {
			const cache = getCacheFor(this, operation.prefix);

			cache.set(operation.key, operation.type === 'put' ? operation.value : undefined);
		}

		this._batch.push(...batch);
	}

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
