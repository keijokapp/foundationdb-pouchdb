import { createViewSignature, handleNotFound } from './util.js';

/**
 * @param {any} sourceDB
 * @param {string} viewName
 * @param {Function} mapFun
 * @param {Function} [reduceFun]
 * @returns {Promise<import('./index.js').View>}
 */
export default async function createView(
	sourceDB,
	viewName,
	mapFun,
	reduceFun
) {
	const viewSignature = createViewSignature(mapFun, reduceFun);

	return sourceDB.info().then(/** @param {{ db_name: string }} info */async info => {
		const depDbName = `${info.db_name}-mrview-${viewName === 'temp_view/temp_view' ? 'temp' : viewSignature}`;

		for (;;) {
			const doc = await sourceDB.get('_local/mrviews').catch(handleNotFound()) ?? { views: {} };

			doc.views[viewName] ??= {};

			if (doc.views[viewName][depDbName]) {
				break;
			}

			doc.views[viewName][depDbName] = true;

			try {
				await sourceDB.put(doc);

				break;
			} catch (/** @type {any} */e) {
				if (e.status !== 409) {
					throw e;
				}
			}
		}

		const { db } = await sourceDB.registerDependentDatabase(depDbName);

		db.auto_compaction = true;

		const lastSeqDoc = await db.get('_local/lastSeq').catch(handleNotFound());

		return {
			db,
			mapFun,
			name: depDbName,
			reduceFun,
			seq: lastSeqDoc != null ? lastSeqDoc.seq : 0,
			sourceDB
		};
	});
}
