import express from 'express';
import * as fdb from '@arbendium/foundationdb';
import HttpPouch from 'pouchdb-adapter-http';
import pouchdbExpressRouter from 'pouchdb-express-router';
import mapreduce from 'pouchdb-mapreduce';
import replication from 'pouchdb-replication';
import PouchDB from 'pouchdb-core';
import FoundationdbPlugin from '../lib/plugin.js';

fdb.setAPIVersion(720);

PouchDB
	.plugin(FoundationdbPlugin)
	.plugin(HttpPouch)
	.plugin(mapreduce)
	.plugin(replication);

const app = express();

app.use((req, res, next) => {
	res.header('access-control-allow-origin', req.headers.origin);
	res.header('access-control-allow-headers', 'Origin, X-Requested-With, Content-Type, Accept');
	res.header('access-control-allow-methods', 'GET,PUT,POST,DELETE,OPTIONS');
	res.header('access-control-allow-credentials', 'true');
	res.header('access-control-max-age', '1000000000');

	if (req.method === 'OPTIONS') {
		res.sendStatus(200);
	} else {
		next();
	}
});

app.use(pouchdbExpressRouter(PouchDB.defaults({
	db: fdb.open()
})));

app.listen(5984);
