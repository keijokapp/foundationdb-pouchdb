import express from 'express';
import HttpPouch from 'pouchdb-adapter-http';
import pouchdbExpressRouter from 'pouchdb-express-router';
import mapreduce from 'pouchdb-mapreduce';
import replication from 'pouchdb-replication';
import PouchDB from 'pouchdb-core';
import LeveldbPlugin from '../lib/plugin.js';

PouchDB
	.plugin(LeveldbPlugin)
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

app.use(pouchdbExpressRouter(PouchDB));

app.listen(5984);
