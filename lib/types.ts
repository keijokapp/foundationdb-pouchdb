export type PartialBy<T, K extends keyof T> = Omit<T, K> & Partial<Pick<T, K>>

declare const seqStringSymbol: unique symbol;
export type SeqString = string & { [seqStringSymbol]: true }

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

export type Digest = `md5-${string}`

export type Ref = `${Id}@${Rev}`

export type Doc = {
	_id: Id
	_rev: Rev
	_attachments?: Record<string, Attachment>
	_deleted?: true
}
export type LocalDoc = {
	_id: LocalId
	_rev: LocalRev
	_deleted?: true
	_revisions?: unknown
}
export type Metadata<Seq = SeqString | bigint> = {
	deleted?: boolean
	id: Id
	rev_map: Record<Rev, Seq>
	rev_tree: RevTreePath[]
	rev: Rev
	revisions?: { start: RevNum, ids: RevId[] }
	seq: Seq
	winningRev?: Rev
}
export type FinalizedMetadata = Metadata<SeqString>
export type InputDoc = {
	_id?: Id
	_rev?: Rev
	_revisions?: {
    start: RevNum
    ids: RevId[]
	}
	rev_tree?: RevTreePath[]
}
export type RevTreePath = { pos: RevNum, ids: RevTreeNode }
export type RevTreeNode = [RevId, RevTreeNodeStatus, RevTreeNode[]]
export type RevTreeNodeStatus = {
	status: 'available' | 'missing'
	deleted?: true
}
export type Attachment = {
	content_type: string
	data?: string | unknown
	digest: Digest
	length: number
	revpos: RevNum
	stub?: true
}
export type AttachmentRef = {
	refs: Record<Ref, true>
}
export type DocInfo<Seq = SeqString | bigint> = {
	attachments?: { digest: Digest, data: Buffer | undefined }[]
	data: Doc
	metadata: Metadata<Seq>
	stemmedRevs?: Rev[] | undefined
}

export type BulkDocsResultRow = Error | {} | { ok: true, id: Id, rev: Rev }
export type AllDocsResult<Seq extends number | SeqString> = {
	offset: number | undefined
	rows: AllDocsResultRow[]
	total_rows: number
	update_seq?: Seq
}

export type AllDocsResultRow =
	| { key: Id, error: 'not_found' }
	| {
		id: Id
		key: Id
		value: {
			rev: Rev
			deleted?: true
		}
		doc?: null | AllDocsResultRowDoc
	}

export type AllDocsResultRowDoc = {
	_attachments?: Record<
		string,
		| { content_type: string, digest: Digest, revpos: RevNum, data: string }
		| { content_type: string, digest: Digest, length: number, revpos: RevNum, stub: true }
	>
	_conflicts?: Rev[]
	_deleted?: true
	_id: Id
	_rev: Rev
}

export type Change<Seq extends number | SeqString> = {
	doc?: {
		_attachments?: Record<
			string,
			| { content_type: string, digest: Digest, revpos: RevNum, data: string }
			| { content_type: string, digest: Digest, length: number, revpos: RevNum, stub: true }
		>
	}
	seq: Seq
}

export type ChangesResult<Seq extends number | SeqString> = {
	results: import('./types.js').Change<Seq>[],
	last_seq: Seq
}
