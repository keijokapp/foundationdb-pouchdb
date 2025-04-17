import { createViewSignature, handleNotFound, stringMd5 } from './util.js';

export default async function createView(
	sourceDB,
	viewName,
	mapFun,
	reduceFun
) {
	const viewSignature = createViewSignature(mapFun, reduceFun);

	/** @type {Record<string, unknown> | undefined} */
	let cachedViews;

	if (viewName !== 'temp_view/temp_view') {
		sourceDB._cachedViews ??= {};
		cachedViews = sourceDB._cachedViews;

		if (sourceDB._cachedViews[viewSignature]) {
			return sourceDB._cachedViews[viewSignature];
		}
	}

	const promise = sourceDB.info().then(async info => {
		const depDbName = `${info.db_name}-mrview-${viewName === 'temp_view/temp_view' ? 'temp' : stringMd5(viewSignature)}`;

		for (;;) {
			const doc = await sourceDB.get('_local/mrviews').catch(handleNotFound()) ?? {};

			doc.views ??= {};
			doc.views[viewName] ??= {};

			if (doc.views[viewName][depDbName]) {
				break;
			}

			doc.views[viewName][depDbName] = true;

			try {
				await sourceDB.put(doc);

				break;
			} catch (e) {
				if (e.status !== 409) {
					throw e;
				}
			}
		}

		const res = await sourceDB.registerDependentDatabase(depDbName);
		const { db } = res;
		db.auto_compaction = true;

		const lastSeqDoc = await db.get('_local/lastSeq').catch(handleNotFound());

		const view = {
			adapter: sourceDB.adapter,
			db,
			mapFun,
			name: depDbName,
			reduceFun,
			seq: lastSeqDoc != null ? lastSeqDoc.seq : 0,
			sourceDB
		};

		if (cachedViews) {
			view.db.once('destroyed', () => {
				delete cachedViews[viewSignature];
			});
		}

		return view;
	});

	if (cachedViews) {
		cachedViews[viewSignature] = promise;
	}

	return promise;
}
