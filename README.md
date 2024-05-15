**This library is not yet production-ready and the `master` is regularly overwritten until the first "working" version to simplify keeping track of changes.**

---

**FoundationDB adapter for PouchDB** or **PouchDB layer for FoundationDB**, whichever way to look at it.

This layer/adapter takes advantage of both systems. PouchDB provides high-level concepts- API, indices/views, asynchronous replication, etc. FoundationDB provides distributed data storage with causally consistent ACID-compliant interactive transactions. Multiple PouchDB interactions could be encapsulated into a single FoundationDB transaction which means they can be committed, rolled back or retryed together.

It is based on heavily refactored [pouchdb-adapter-leveldb-core](https://github.com/pouchdb/pouchdb/tree/14a566f2e7bb780c1af37fd468f419f029a0adc5/packages/node_modules/pouchdb-adapter-leveldb-core) and related code.

## Examples

Running individual PouchDB actions in their own transactions:

```js
import PouchDB from 'pouchdb';
import * as fdb from 'foundationdb';
import FoundationdbPlugin from 'foundationdb-pouchdb/plugin';
import { database } from './server/database.js';

PouchDB.plugin(FoundationdbPlugin);

fdb.setAPIVersion(720);

const pouch = new PouchDB({
	name: 'foo', // used as a raw prefix of the keyspace
	adapter: 'foundationdb',
	db: fdb.open()
});

const doc = await pouch.put({ _id: 'foo' });

// doc = { ok: true, id: 'foo', rev: '1-50d2688956aa2ab56d6eec5ccf262a1c' }

const docs = await pouch.allDocs();

// docs = {
//   total_rows: 1,
//   offset: 0,
//    rows: [ { id: 'foo', key: 'foo', value: { rev: '1-50d2688956aa2ab56d6eec5ccf262a1c' } } ]
// }
```

Encapsulating multiple PouchDB actions in a single transaction:

```js
await db.doTn(tn => {
	const pouch = new PouchDB({
		name: 'foo',
		adapter: 'foundationdb',
		db: tn
	});

	const doc = await pouch.put({ _id: 'foo' });

	const docs = await pouch.allDocs();
}
```

## Limitations

The adapter is subject to the fundamental [limitations](https://apple.github.io/foundationdb/known-limitations.html) of FoundationDB. In particular:

 - Trying to insert large attachments or documents is an undefined behavior.
 - Transactions can last at most 5 seconds from the first read.
 - Write transactions have overall size limitations.

Also, there are non-FoundationDB-specific limitations:

- All write operations update "last update seq" and "doc count" keys, so concurrent write transactions are effectively guaranteed to conflict and wouldn't benefit from concurrency. This can also affect read operations, eg if they need to update views.

Some of these limitations could be easily solved. If these limitations are a significant problem for you, please create an issue.

## Testing

The adapter is tested by [PouchDB](https://github.com/pouchdb/pouchdb)'s test suite over HTTP.

A couple of tests need to be modified and disabled:

 - `test total_rows with a variety of criteria * 100` - needs a slightly longer timeout. 6000 ms should be fine.
 - `putAttachment and getAttachment with big png data` - large attachments are not supported so this test should be disabled.

To run tests, first start the server:

```
node test/server.js
```

An then in PouchDB's repository:

```
COUCH_HOST='http://localhost:5984' npm test
```

# License

The code is heavily based on PouchDB's LevelDB layer licensed under [Apache License version 2.0](https://github.com/pouchdb/pouchdb/blob/master/LICENSE). All original work is licensed under ISC.
