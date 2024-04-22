import type { Id, LocalId, Metadata, Rev, RevId, RevNum, RevTreePath, RevTreeNode } from '../lib/types.ts';

export declare function collectConflicts(metadata: Metadata): Rev[]
export declare function compactTree(metadata: Metadata): Rev[]
export declare function isDeleted(metadata: Metadata, rev?: Rev): boolean
export declare function isLocalId(id: Id | LocalId): id is LocalId
export declare function latest(rev: Rev, metadata: Metadata): Rev
export declare function merge(tree: RevTreePath[], path: RevTreePath, depth: number): {
	tree: RevTreePath[],
	stemmedRevs?: unknown[],
	conflicts: unknown
}
export declare function revExists(revTree: RevTreePath[], rev: Rev): boolean
export declare function traverseRevTree(
	revs: RevTreePath[],
	callback: (
		isLeaf: boolean,
		pos: RevNum,
		revHash: RevId,
		tn: unknown,
		opts: SitahädaStatus
	) => void
): void
export declare function winningRev(metadata: Metadata): Rev
