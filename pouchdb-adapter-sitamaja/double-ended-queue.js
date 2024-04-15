/* eslint-disable no-bitwise */
export default class Deque {
	constructor() {
		this.capacity = 16;
		this.front = 0;
		this.length = 0;
	}

	push(arg) {
		this._checkCapacity(this.length + 1);

		const i = (this.front + this.length) & (this.capacity - 1);

		this[i] = arg;

		this.length++;
	}

	_checkCapacity(size) {
		if (this.capacity < size) {
			const capacity = getCapacity(this.capacity * 1.5 + 16);

			const oldCapacity = this.capacity;
			this.capacity = capacity;

			if (this.front + this.length > oldCapacity) {
				const moveItemsCount = (this.front + this.length) & (oldCapacity - 1);
				arrayMove(this, 0, this, oldCapacity, moveItemsCount);
			}
		}
	}
}

function arrayMove(src, srcIndex, dst, dstIndex, len) {
	for (let j = 0; j < len; ++j) {
		dst[j + dstIndex] = src[j + srcIndex];
		src[j + srcIndex] = undefined;
	}
}

function pow2AtLeast(n) {
	n >>>= 0;
	n -= 1;
	n |= (n >> 1);
	n |= (n >> 2);
	n |= (n >> 4);
	n |= (n >> 8);
	n |= (n >> 16);

	return n + 1;
}

function getCapacity(capacity) {
	return pow2AtLeast(
		Math.min(Math.max(16, capacity), 1073741824)
	);
}
