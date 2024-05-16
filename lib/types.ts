export type PartialBy<T, K extends keyof T> = Omit<T, K> & Partial<Pick<T, K>>

declare const idSymbol: unique symbol
export type Id = string & { [idSymbol]: true }

declare const localIdSymbol: unique symbol
export type LocalId = string & { [localIdSymbol]: true }

declare const revNumSymbol: unique symbol
export type RevNum = number & { [revNumSymbol]: true }

declare const revIdSymbol: unique symbol
export type RevId = string & { [revIdSymbol]: true }

export type Rev = `${RevNum}-${RevId}`

export type LocalRev = `${RevNum}-${number}`

declare const attachmentIdSymbol: unique symbol
export type AttachmentId = string & { [attachmentIdSymbol]: true }

export type Digest = `md5-${string}`;

export type Ref = `${Id}@${Rev}`

export type Doc = {
	_id: Id,
	_rev: Rev,
	_attachments?: Record<string, Attachment>,
	_deleted?: true
};
export type LocalDoc = {
	_id: LocalId,
	_rev: LocalRev,
	_deleted?: true,
	_revisions?: unknown
};
export type Metadata = {
	deleted?: boolean,
	id: Id,
	rev: Rev,
	rev_map: Record<Rev, number>,
	rev_tree: RevTreePath[],
	revisions?: { start: RevNum, ids: RevId[] },
	seq: number,
	winningRev?: Rev
}
export type InputDoc = {
	_id?: Id,
	_rev?: Rev,
	_revisions?: {
    start: RevNum,
    ids: RevId[]
	},
	rev_tree?: RevTreePath[]
};
export type RevTreePath = { pos: RevNum, ids: RevTreeNode }
export type RevTreeNode = [RevId, RevTreeNodeStatus, RevTreeNode[]]
export type RevTreeNodeStatus = {
	status: 'available' | 'missing',
	deleted?: true
}
export type Attachment = {
	content_type: string,
	digest: Digest,
	length: number,
	revpos: RevNum,
	stub?: true,
	data?: string | unknown
}
export type AttachmentRef = {
	refs: Record<Ref, true>
};
export type DocInfo = {
	data: Doc,
	metadata: Metadata,
	attachments?: { digest: Digest, data: Buffer | undefined }[],
	stemmedRevs?: Rev[] | undefined
};

export type BulkDocsResultRow = Error | {} | { ok: true, id: Id, rev: Rev }
export type AllDocsResult = {
	offset: number | undefined,
	total_rows: number,
	update_seq?: number,
	rows: AllDocsResultRow[]
}

export type AllDocsResultRow =
	| { key: Id, error: 'not_found' }
	| {
		id: Id,
		key: Id,
		value: {
			rev: Rev,
			deleted?: true
		},
		doc?: null | AllDocsResultRowDoc
	}

export type AllDocsResultRowDoc = {
	_attachments?: Record<
		string,
		| { content_type: string, digest: Digest, revpos: RevNum, data: string }
		| { content_type: string, digest: Digest, length: number, revpos: RevNum, stub: true }
	>,
	_conflicts?: Rev[],
	_deleted?: true,
	_id: Id,
	_rev: Rev
}

export type Change = {
	doc?: {
		_attachments?: Record<
			string,
			| { content_type: string, digest: Digest, revpos: RevNum, data: string }
			| { content_type: string, digest: Digest, length: number, revpos: RevNum, stub: true }
		>
	},
	seq: number
}

export type ChangesResult = {
	results: import('./types.js').Change[],
	last_seq: number
}
