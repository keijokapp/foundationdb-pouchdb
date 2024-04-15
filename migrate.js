import fs from 'fs';
import path from 'path';
import { Writable } from 'stream';
import { pipeline } from 'stream/promises';
import level from 'level';
import leveldown from 'leveldown';
import { isLocalId, winningRev } from 'pouchdb-merge';

const stores = [
	'document-store',
	'by-sequence',
	'attach-store',
	'attach-binary-store'
];
function formatSeq(n) {
	return (`0000000000000000${n}`).slice(-16);
}
const UPDATE_SEQ_KEY = '_local_last_update_seq';
const DOC_COUNT_KEY = '_local_doc_count';
const UUID_KEY = '_local_uuid';

export async function doMigrationOne(name, db) {
	const base = path.resolve(name);

	async function move(store, index) {
		const storePath = path.join(base, store);
		const opts = {
			valueEncoding: index === 3 ? 'binary' : 'json'
		};
		const sub = db.sublevel(store, opts);
		const orig = level(storePath, opts);

		await pipeline(
			orig.createReadStream(),
			new Writable({
				objectMode: true,
				write(chunk, encoding, callback) {
					sub.put(chunk.key, chunk.value).then(
						() => callback(),
						callback
					);
				},
				writev(chunks, callback) {
					sub.batch(chunks.map(({ key, value }) => ({ key, value, type: 'put' }))).then(
						() => callback(),
						callback
					);
				}
			})
		);

		return new Promise((resolve, reject) => {
			orig.close(e => {
				if (e) {
					return reject(e);
				}

				resolve(storePath);
			});
		});
	}

	try {
		await fs.promises.unlink(`${base}.uuid`);
	} catch (e) {
		return;
	}

	await Promise.all(stores.map(async (store, i) => {
		const storePath = await move(store, i);

		await new Promise(resolve => {
			leveldown.destroy(storePath, () => { resolve(); });
		});

		await fs.promises.rmdir(base);
	}));
}

export async function doMigrationTwo(db, stores) {
	const batches = [];

	const value = await stores.bySeqStore.get(UUID_KEY).catch(() => {});

	if (value == null) {
		return;
	}

	batches.push(
		{
			key: UUID_KEY,
			value,
			prefix: stores.metaStore,
			type: 'put',
			valueEncoding: 'json'
		},
		{
			key: UUID_KEY,
			prefix: stores.bySeqStore,
			type: 'del'
		}
	);

	await stores.bySeqStore.get(DOC_COUNT_KEY).then(
		value => {
			batches.push(
				{
					key: DOC_COUNT_KEY,
					value,
					prefix: stores.metaStore,
					type: 'put',
					valueEncoding: 'json'
				},
				{
					key: DOC_COUNT_KEY,
					prefix: stores.bySeqStore,
					type: 'del'
				}
			);
		},
		() => {}
	);

	await stores.bySeqStore.get(UPDATE_SEQ_KEY).then(
		value => {
			// if no UPDATE_SEQ_KEY
			// just skip
			// we've gone to far to stop.
			batches.push(
				{
					key: UPDATE_SEQ_KEY,
					value,
					prefix: stores.metaStore,
					type: 'put',
					valueEncoding: 'json'
				},
				{
					key: UPDATE_SEQ_KEY,
					prefix: stores.bySeqStore,
					type: 'del'
				}
			);
		},
		() => {}
	);

	const deletedSeqs = {};

	const readStream = stores.docStore.createReadStream({ startKey: '_', endKey: '_\xFF' });

	await pipeline(
		readStream,
		async function* (it) {
			for await (const ch of it) {
				if (!isLocalId(ch.key)) {
					continue;
				}

				batches.push({
					key: ch.key,
					prefix: stores.docStore,
					type: 'del'
				});

				const winner = winningRev(ch.value);

				for (const key of Object.keys(ch.value.rev_map)) {
					if (key !== 'winner') {
						yield formatSeq(ch.value.rev_map[key]);
					}
				}

				const winningSeq = ch.value.rev_map[winner];

				try {
					const value = await stores.bySeqStore.get(formatSeq(winningSeq));

					batches.push({
						key: ch.key,
						value,
						prefix: stores.localStore,
						type: 'put',
						valueEncoding: 'json'
					});
				} catch (e) {
					// ignored intentionally
				}
			}
		},
		async it => {
			for await (const seq of it) {
				if (!deletedSeqs[seq]) {
					deletedSeqs[seq] = true;

					const resp = await stores.bySeqStore.get(seq).catch(() => {});

					if (resp != null && isLocalId(resp._id)) {
						batches.push({
							key: seq,
							prefix: stores.bySeqStore,
							type: 'del'
						});
					}
				}
			}
		}
	);

	await db.batch(batches);
}
