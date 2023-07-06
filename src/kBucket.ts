import { set as ImmutableSet } from "immutable";

type Node = {
  target: Uint8Array;
  leafs: ImmutableSet<string> | null;
  left: Node | null;
  right: Node | null;
  splittable: boolean;
};

const distance = (x: Uint8Array, y: Uint8Array) => {
  let distance = 0;
  for (let i = 0; i < x.length; ++i) {
    distance = distance * 256 + (x[i] ^ y[i]);
  }
  return distance;
};

const determineNode = (
  bitIndex: number,
  target: Uint8Array,
) => !!(target[~~(bitIndex / 8)] & Math.pow(2, 7 - bitIndex % 8));

export const closest = (
  target: Uint8Array,
  root: Node,
  k: number,
): Uint8Array[] => {
  let results: Uint8Array[] = [];
  const nodesToCheck: Node[] = [root];
  let bitIndex = 0;
  while (nodesToCheck.length && results.length < k) {
    const { left, right, leafs } = nodesToCheck.pop() as Node;
    if (!leafs.size) {
      bitIndex++;
      if (determineNode(bitIndex, target)) nodesToCheck.push(left, right);
      else nodesToCheck.push(right, left);
    } else {
      results = results.concat(Array.from(leafs));
    }
  }
  return results
    .map((a): [number, Uint8Array] => [distance(a, target), a])
    .sort(([a], [b]) => a - b)
    .slice(0, k)
    .map(([, x]) => x);
};

const kBucketSync = (target: ArrayBufferView, size: number) => ({
  target,
  size,
  root: {
    leafs: null,
    left: null,
    right: null,
    splittable: true,
  },
});

const set = (id: Uint8Array, node: Node, bitIndex: number): Node => ({
  leafs: node.leafs,
  right: determineNode(bitIndex, id)
    ? set(id, node.right, bitIndex + 1)
    : node.right,
  left: !determineNode(bitIndex, id)
    ? set(id, node.left, bitIndex + 1)
    : node.left,
  target: node.target,
  splittable: node.splittable,
  //   while (!node.leafs) {
  //     node = determineNode(bitIndex, id);
  //     bitIndex++;
  //   }
  //   if (this._node_data.has(id)) {
  //     node.contacts.delete(id);
  //     node.contacts.add(id);
  //     return true;
  //   }
  //   if (node.contacts.size < this._bucket_size) {
  //     node.contacts.add(id);
  //     return true;
  //   }
  //   if (node.splittable) {
  //     splitNodeLeafs(bitIndex, node);
  //     return set(id, on_full);
  //   }
  //   on_full(Array.from(node.contacts));
  //   return false;
});

const transitiveLeafs = ({ left, right, leafs }: Node): ImmutableSet<string> =>
  (leafs.size)
    ? leafs
    : (left ? transitiveLeafs(left) : ImmutableSet<string>()).union(
      right ? transitiveLeafs(right) : ImmutableSet<string>(),
    );

const del = (node: Node, bitIndex: number): Node =>
  (leafs.size) ? { ...node, leafs: node.contacts.remove(id) } : {
    ...node,
    left: determineNode(bitIndex, id)
      ? del(id, node.left, bitIndex + 1)
      : node.left,
    right: determineNode(bitIndex, id)
      ? node.right
      : del(id, node.right, bitIndex + 1),
  };

const splitNodeLeafs = (
  bitIndex: number,
  { leafs, target }: Node,
): Node => ({
  target,
  leafs: ImmutableSet<string>(),
  splittable: true,
  left: {
    target,
    splittable: determineNode(bitIndex, target),
    leafs: ImmutableSet<string>(
      leafs.filter((id: Uint8Array) => determineNode(bitIndex, id)),
    ),
    left: null,
    right: null,
  },
  right: {
    target,
    splittable: !determineNode(bitIndex, target),
    leafs: ImmutableSet<string>(
      leafs.filter((id: Uint8Array) => !determineNode(bitIndex, id)),
    ),
    left: null,
    right: null,
  },
});
