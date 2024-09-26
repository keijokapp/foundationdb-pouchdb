**This library is not yet production-ready.**

---

**FoundationDB adapter for PouchDB** or **PouchDB layer for FoundationDB**, whichever way to look at it.

This layer/adapter takes advantage of both systems. PouchDB provides high-level concepts - API, indices/views, asynchronous replication, etc. FoundationDB provides distributed data storage with causally consistent ACID-compliant interactive transactions. Multiple PouchDB interactions could be encapsulated into a single FoundationDB transaction which means they can be committed, rolled back or retried together.

It is based on heavily refactored [pouchdb-adapter-leveldb-core](https://github.com/pouchdb/pouchdb/tree/14a566f2e7bb780c1af37fd468f419f029a0adc5/packages/node_modules/pouchdb-adapter-leveldb-core) and related code.

> **Note:** Some plugins, like mapreduce, rely on the fact that other PouchDB backends (eg LevelDB or IndexedDB) are non-distributed. They rely on internal application-level locks which makes them unsuitable for use with distributed backends like FoundationDB. Alternatives to these plugins may be implemented in the future.

## Examples

**Running individual PouchDB operations in separate transactions:**

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

**Encapsulating multiple PouchDB operations in a single transaction:**

```js
await db.doTn(tn => {
	const pouch = new PouchDB({
		name: 'foo',
		adapter: 'foundationdb',
		db: tn
	});

	const doc = await pouch.put({ _id: 'foo' });

	const docs = await pouch.allDocs();
})
```

> **Note:** It is generally safe to create multiple PouchDB instances with the same transaction, but make sure that all instances with the same `name` use the same version of this adapter.

> **Note:** Running multiple operations in a single transaction is not necessarily faster than running them in separate transactions. Internally, operations on a single transaction would be serialized to ensure correctness. In the future, a more sophisticated locking might mitigate the need to serialize operations.

## Sparse sequences

It is recommended to enable _sparse sequences_ mode if possible.

Mutations in PouchDB databases are organized into sequences. By default, the adapter keeps these sequences strictly sequential (ie without gaps), just like LevelDB and other adapters. In a distributed database like FoundationDB, this would quickly become a bottleneck by making write concurrency impossible. Concurrent writes would conflict and need to be retried or canceled. Instead, the adapter can make use of FoundationDB commit versions. Versions are strictly monotonically increasing (like sequences) but not sequential. This should be, on its own, okay for most applications. The problem is that versions are big - 12 bytes each - so they don't fit into typical numeric data types. Instead, they are exposed as 24-character _hex strings_.

**Caveats:**

- Sparse sequences are not strictly sequential. This can cause issues when, for example, expecting `seq - 1` to exist for each reported `seq`.
- Sparse sequences are too large to fit into basic numeric data types. Instead, a 24-character hex strings are used. This can cause issues with PouchDB ecosystem and existing application code.
- Commit versions are, by definition, not finalized until the transaction is committed. When using multi-operation transactions, the sequences reported by the adapter are **non-final**. A transaction read version `+ 1` is used as a placeholder to ensure some level of robustness within the transaction. If the final sequences are needed outside the transaction, it's up to the userland code to replace the first 10 bytes (20 hex characters) with the actual commit version.

> **Changing the mode for an existing database is not supported.** But effort has been made to make it possible. If this is an important feature for you, please raise an issue.

Sparse sequences can be enabled by passing `sparseSeq: true` to PouchDB constructor:

```
const pouch = new PouchDB({
	name: 'foo',
	adapter: 'foundationdb',
	db,
	sparseSeq: true
});

db.changes({
  since: 0n,
  include_docs: true
})
```

> **Note:** It is generally safe to create multiple PouchDB instances with the same transaction, but make sure that all instances with the same `name` use the same version of this adapter.

> **Note:** Running multiple operations in a single transaction is not _necessarily_ faster than running them in separate transactions. Internally, operations on a single transaction would be serialized to ensure correctness. In the future, a more sophisticated locking might mitigate the need to serialize operations.

## Limitations

The adapter is subject to the fundamental [limitations](https://apple.github.io/foundationdb/known-limitations.html) of FoundationDB. In particular:

 - Trying to insert large attachments or documents is an undefined behavior.
 - Transactions can last at most 5 seconds from the first read.
 - Write transactions have overall size limitations.

Some of these limitations are solvable. If these limitations are a significant problem for you, please create an issue.

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
