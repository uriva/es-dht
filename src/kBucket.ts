import {
  ArraySet,
  arraySetAdd,
  arraySetElements,
  arraySetFilter,
  arraySetHas,
  arraySetRemove,
  makeArraySet,
} from "./containers.ts";

type Node = TerminalNode | NonterminalNode;

type TerminalNode = {
  type: "terminal";
  size: number;
  target: Uint8Array;
  leafs: ArraySet;
  splittable: boolean;
};

type NonterminalNode = {
  type: "nonterminal";
  target: Uint8Array;
  left: Node;
  right: Node;
};

const distance = (x: Uint8Array, y: Uint8Array) => {
  let distance = 0;
  for (let i = 0; i < x.length; ++i) {
    distance = distance * 256 + (x[i] ^ y[i]);
  }
  return distance;
};

const mask = (n: number) => Math.pow(2, 7 - n);

const determineNode = (bitIndex: number, target: Uint8Array) =>
  !!(mask(bitIndex % 8) & target[Math.floor(bitIndex / 8)]);

export const closest = (
  root: Node,
  target: Uint8Array,
  k: number,
): Uint8Array[] => {
  let results: Uint8Array[] = [];
  const nodesToCheck: Node[] = [root];
  let bitIndex = 0;
  while (nodesToCheck.length > 0 && results.length < k) {
    const node = nodesToCheck.pop() as Node;
    if (node.type === "nonterminal") {
      bitIndex++;
      if (determineNode(bitIndex, target)) {
        nodesToCheck.push(node.left, node.right);
      } else nodesToCheck.push(node.right, node.left);
    } else {
      results = results.concat(arraySetElements(node.leafs));
    }
  }
  return results
    .map((a): [number, Uint8Array] => [distance(a, target), a])
    .sort(([a], [b]) => a - b)
    .slice(0, k)
    .map(([, x]) => x);
};

export const kBucket = (target: Uint8Array, size: number): Node => ({
  target,
  splittable: true,
  leafs: makeArraySet([]),
  type: "terminal",
  size,
});
export type Bucket = Node;

export const bucketAddAll = (bucket: Node, elements: Uint8Array[]) => {
  for (const element of elements) bucket = bucketAdd(bucket, element);
  return bucket;
};

export const bucketAdd = (node: Node, element: Uint8Array) =>
  set(element, node, 0);

export const bucketHas = (node: Node, element: Uint8Array) =>
  bucketHasInternal(node, element, 0);

const bucketHasInternal = (
  node: Node,
  element: Uint8Array,
  bitIndex: number,
): boolean =>
  node.type === "terminal"
    ? arraySetHas(node.leafs, element)
    : bucketHasInternal(
      determineNode(bitIndex, element) ? node.right : node.left,
      element,
      bitIndex + 1,
    );

const set = (element: Uint8Array, node: Node, bitIndex: number): Node =>
  node.type === "terminal"
    ? (node.leafs.size < node.size
      ? { ...node, leafs: arraySetAdd(node.leafs, element) }
      : (
        node.splittable
          ? set(element, splitNodeLeafs(bitIndex, node), bitIndex)
          : node
      ))
    : ({
      ...node,
      right: determineNode(bitIndex, element)
        ? set(element, node.right, bitIndex + 1)
        : node.right,
      left: !determineNode(bitIndex, element)
        ? set(element, node.left, bitIndex + 1)
        : node.left,
    });

export const bucketElements = (bucket: Node) =>
  arraySetElements(transitiveLeafs(bucket));

const transitiveLeafs = (
  node: Node,
): ArraySet =>
  node.type === "terminal"
    ? node.leafs
    : (node.left ? transitiveLeafs(node.left) : makeArraySet([])).union(
      node.right ? transitiveLeafs(node.right) : makeArraySet([]),
    );

export const bucketRemove = (node: Node, element: Uint8Array) =>
  del(element, node, 0);

const del = (id: Uint8Array, node: Node, bitIndex: number): Node =>
  node.type === "terminal"
    ? { ...node, leafs: arraySetRemove(node.leafs, id) }
    : {
      ...node,
      right: determineNode(bitIndex, id)
        ? node.right
        : del(id, node.right, bitIndex + 1),
      left: !determineNode(bitIndex, id)
        ? del(id, node.left, bitIndex + 1)
        : node.left,
    };

const splitNodeLeafs = (bitIndex: number, node: TerminalNode): Node => ({
  type: "nonterminal",
  target: node.target,
  left: {
    size: node.size,
    type: "terminal",
    target: node.target,
    splittable: determineNode(bitIndex, node.target),
    leafs: arraySetFilter(
      node.leafs,
      (id: Uint8Array) => !determineNode(bitIndex, id),
    ),
  },
  right: {
    size: node.size,
    type: "terminal",
    target: node.target,
    splittable: !determineNode(bitIndex, node.target),
    leafs: arraySetFilter(
      node.leafs,
      (id: Uint8Array) => determineNode(bitIndex, id),
    ),
  },
});
