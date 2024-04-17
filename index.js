/* eslint-disable no-console,no-debugger */
import { v4 as uuid } from 'uuid';
import PouchDB from 'pouchdb-core';
import LeveldbPlugin from './plugin.js';

PouchDB.plugin(LeveldbPlugin);

const db = new PouchDB({
	name: `pouchdb-server/${uuid()}`,
	adapter: 'leveldb'
});

try {
	await Promise.all([
		db.put({ _id: 'a' }),
		db.put({ _id: 'b' }),
		db.put({ _id: 'c' })
	]);

	console.log(await db.allDocs());
} catch (e) {
	console.log(e);

	debugger;
}
