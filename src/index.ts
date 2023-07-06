import { Map as ImmutableMap, Set as ImmutableSet } from "npm:immutable";
import { concatUint8Array, uint8ArraysEqual } from "./utils.ts";

import arrayMapSet from "npm:array-map-set@^1.0.1";
import kBucketSync from "npm:k-bucket-sync@^0.1.3";
import merkleTreeBinary from "npm:merkle-tree-binary@^0.1.0";

const arrayToKey = (arr: Uint8Array) => arr.join(",");
type ArrayMap<V> = ImmutableMap<string, V>;

const mapGetArrayImmutable = <V>(
  map: ArrayMap<V>,
  key: Uint8Array,
): V | undefined => map.get(arrayToKey(key));

const mapSetArrayImmutable = <V>(
  map: ArrayMap<V>,
  key: Uint8Array,
  value: V,
): ArrayMap<V> => map.set(arrayToKey(key), value);

type ArraySet = ImmutableSet<string>;

const setHasArrayImmutable = (set: ArraySet, arr: Uint8Array) =>
  set.has(arrayToKey(arr));
const setAddArrayImmutable = (set: ArraySet, arr: Uint8Array) =>
  set.add(arrayToKey(arr));

export type Bucket = ReturnType<typeof kBucketSync>;
export type HashFunction = (data: any) => HashedValue;
type State = Map<PeerId, [StateVersion, PeerId[]]>;
type Proof = Uint8Array;
export type StateVersion = ReturnType<typeof computeStateVersion>;
export type DHT = ReturnType<typeof makeDHT>;
export type HashedValue = any;
type StateCache = ReturnType<typeof ArrayMap>;
export const { ArrayMap } = arrayMapSet;

export const makeDHT = (
  id: PeerId,
  hash: HashFunction,
  bucketSize: number,
  cacheHistorySize: number, // How many versions of local history will be kept
  fractionOfNodesFromSamePeer: number, // Max fraction of nodes originated from single peer allowed on lookup start, e.g. 0.2
) => ({
  data: ArrayMap(),
  id,
  hash,
  bucketSize,
  fractionOfNodesFromSamePeer,
  cacheHistorySize,
  stateCache: ArrayMap(),
  latestState: [computeStateVersion(hash, id, new Map()), new Map()] as [
    StateVersion,
    State,
  ],
  peers: kBucketSync(id, bucketSize),
  lookups: ArrayMap(),
});

export type PeerId = Uint8Array;
// [nodeId, parentPeerId, parentPeerStateVersion]
export type Item = [PeerId, PeerId, StateVersion];

const EFFECT_bla = (
  state: State,
  bucket: Bucket,
  maxCountAllowed: number,
  originatedFrom: ReturnType<typeof ArrayMap>,
  closestNode: PeerId,
  parentPeer: PeerId,
): Item | null => {
  if (parentPeer) {
    const count = originatedFrom.get(parentPeer) || 0;
    originatedFrom.set(parentPeer, count + 1);
    if (count > maxCountAllowed) {
      bucket.del(closestNode);
      return null;
    }
    const parentPeerState = state.get(parentPeer);
    if (parentPeerState) return [closestNode, parentPeer, parentPeerState[0]];
    throw "no state for parent id";
  }
  const count = originatedFrom.get(closestNode) || 0;
  originatedFrom.set(closestNode, count + 1);
  if (count > maxCountAllowed) bucket.del(closestNode);
  return null;
};

export const EFFECT_startLookup = (
  { id, lookups, fractionOfNodesFromSamePeer, bucketSize, peers, latestState }:
    DHT,
  target: PeerId,
): Item[] => {
  if (peers.has(target)) return [];
  const bucket = kBucketSync(target, bucketSize);
  const parents = ArrayMap();
  const [_, state] = latestState;
  const alreadyConnected = ImmutableSet(state.keys());
  state.forEach(([_, peerPeers], peerId: PeerId) => {
    bucket.set(peerId);
    for (const peerPeerId of peerPeers) {
      if (!parents.has(peerPeerId) && bucket.set(peerPeerId)) {
        parents.set(peerPeerId, peerId);
      }
    }
  });
  if (bucket.has(target)) {
    const parentPeerState = state.get(parents.get(target));
    if (!parentPeerState) throw "id exists but no parent";
    lookups.set(target, [bucket, bucketSize, alreadyConnected]);
    return [[target, parents.get(target), parentPeerState[0]]];
  }
  bucket.del(id);
  const maxFraction = Math.max(fractionOfNodesFromSamePeer, 1 / peers.count());
  while (true) {
    const closestSoFar = bucket.closest(target, bucketSize);
    const originatedFrom = ArrayMap();
    const nodesToConnectTo = closestSoFar.map((closestNodeId: PeerId) =>
      EFFECT_bla(
        state,
        bucket,
        Math.ceil(closestSoFar.length * maxFraction),
        originatedFrom,
        closestNodeId,
        parents.get(closestNodeId),
      )
    ).filter((x: Item | null) => x);
    if (nodesToConnectTo.length) {
      lookups.set(target, [bucket, bucketSize, alreadyConnected]);
      return nodesToConnectTo;
    }
  }
};

export const EFFECT_updateLookup = (
  peers: Bucket,
  lookups: ReturnType<typeof ArrayMap>,
  id: PeerId,
  target: HashedValue,
  nodeId: PeerId,
  // Corresponding state version for `nodeId`
  nodeStateVersion: StateVersion,
  // Peers of `nodeId` at state `nodeStateVersion` or `null` if connection to `nodeId` have failed
  nodePeers: PeerId[],
): Item[] => {
  const lookup = lookups.get(target);
  if (!lookup) return [];
  const [bucket, number, alreadyConnected] = lookup;
  if (!nodePeers) {
    bucket.del(nodeId);
    return [];
  }
  lookup[2] = setAddArrayImmutable(alreadyConnected, nodeId);
  if (peers.has(target) || bucket.has(target)) return [];
  const addedNodes = ImmutableSet<string>(
    nodePeers.filter((nodePeerId) =>
      !bucket.has(nodePeerId) && bucket.set(nodePeerId)
    ).map(arrayToKey),
  );
  if (bucket.has(target)) return [[target, nodeId, nodeStateVersion]];
  bucket.del(id);
  return bucket.closest(target, number).filter((peer: PeerId) =>
    setHasArrayImmutable(addedNodes, peer)
  ).map((peer: PeerId) => [
    peer,
    nodeId,
    nodeStateVersion,
  ]);
};

// Returns `[id]` if node with specified ID was connected directly,
// an array of closest IDs if exact node wasn't found and `null` otherwise.
export const EFFECT_finishLookup = (
  lookups: ReturnType<typeof ArrayMap>,
  peers: Bucket,
  id: PeerId,
): PeerId[] => {
  const lookup = lookups.get(id);
  lookups.delete(id);
  const [bucket, number, alreadyConnected] = lookup;
  return (peers.has(id) || setHasArrayImmutable(alreadyConnected, id))
    ? [id]
    : bucket.closest(id, number);
};

const checkProofLength = (
  hash: HashFunction,
  proofBlockSize: number,
  peerId: PeerId,
  proof: Proof,
  neighborsLength: number,
) => {
  const expectedNumberOfItems = neighborsLength * 2 + 2;
  const expectedProofHeight = Math.log2(expectedNumberOfItems);
  const proofHeight = proof.length / proofBlockSize;
  if (proofHeight === expectedProofHeight) return true;
  if (proofHeight !== Math.ceil(expectedProofHeight)) return false;
  let lastBlock = peerId;
  for (
    let i = 0;
    i <= Math.ceil(
          Math.pow(Math.log2(expectedNumberOfItems), 2) -
            expectedNumberOfItems,
        ) / 2;
    ++i
  ) {
    if (
      proof[i * proofBlockSize] !== 0 ||
      !uint8ArraysEqual(
        proof.subarray(i * proofBlockSize + 1, (i + 1) * proofBlockSize),
        lastBlock,
      )
    ) return false;
    lastBlock = hash(concatUint8Array(lastBlock, lastBlock));
  }
  return true;
};

// Returns `false` if proof is not valid and `true` when means there were no errors
// `true` does not imply peer was necessarily added to k-bucket.
export const EFFECT_setPeer = (
  { id, hash, peers, latestState }: DHT,
  peerId: PeerId,
  peerStateVersion: StateVersion,
  proof: Proof,
  neighbors: PeerId[],
): boolean => {
  if (
    uint8ArraysEqual(id, peerId) ||
    !checkProofLength(hash, id.length + 1, peerId, proof, neighbors.length)
  ) return false;
  const detectedPeer = checkStateProof(
    hash,
    peerStateVersion,
    proof,
    peerId,
  );
  if (!detectedPeer || !uint8ArraysEqual(detectedPeer, peerId)) return false;
  if (peers.set(peerId)) {
    const state = ArrayMap(Array.from(latestState[1]));
    state.set(peerId, [peerStateVersion, neighbors]);
    latestState[0] = computeStateVersion(hash, id, state);
    latestState[1] = state;
  }
  return true;
};

export const EFFECT_deletePeer = (
  { latestState, peers, hash, id }: DHT,
  peerId: PeerId,
) => {
  const state = ArrayMap(Array.from(latestState[1]));
  if (!state.has(peerId)) return;
  peers.del(peerId);
  state.delete(peerId);
  latestState[0] = computeStateVersion(hash, id, state);
  latestState[1] = state;
};

export const getState = (
  { latestState, stateCache, id, hash }: DHT,
  stateVersion: StateVersion | null,
): [StateVersion, Proof, PeerId[]] => {
  const [stateVersion2, state] = stateVersion
    ? getStateHelper(latestState, stateCache, stateVersion)
    : latestState;
  return [
    stateVersion2,
    getStateProof(latestState, stateCache, id, hash, stateVersion2, id),
    Array.from(state.keys()),
  ];
};

// Commit current state into state history.
// Needs to be called if current state was sent to any peer.
// This allows to only store useful state versions in cache known to other peers
// and discard the rest.
export const EFFECT_commitState = (
  size: number,
  stateCache: StateCache,
  latestState: [StateVersion, State],
) => {
  const [key, value] = latestState;
  if (stateCache.has(key)) return;
  stateCache.set(key, value);
  if (stateCache.size > size) stateCache.delete(stateCache.keys().next().value);
};

const getStateHelper = (
  latestState: [StateVersion, State],
  stateCache: StateCache,
  stateVersion: StateVersion,
): [StateVersion, State] =>
  (uint8ArraysEqual(stateVersion, latestState[0]))
    ? latestState
    : [stateVersion, stateCache.get(stateVersion)];

// Generate proof about peer in current state version.
export const getStateProof = (
  latestState: [StateVersion, State],
  stateCache: StateCache,
  id: PeerId,
  hash: HashFunction,
  stateVersion: StateVersion,
  peerId: PeerId,
): Proof => {
  const [_, state] = getStateHelper(latestState, stateCache, stateVersion);
  return (state.has(peerId) || uint8ArraysEqual(peerId, id))
    ? merkleTreeBinary.get_proof(
      reduceStateToProofItems(id, state),
      peerId,
      hash,
    )
    : new Uint8Array(0);
};

const reduceStateToProofItems = (
  id: PeerId,
  state: State,
): (PeerId | StateVersion)[] => {
  const items = [];
  state.forEach(([peerStateVersion], peerId: PeerId) => {
    items.push(peerId, peerStateVersion);
  });
  items.push(id, id);
  return items;
};

export const checkStateProof = (
  hash: HashFunction,
  stateVersion: Uint8Array,
  proof: Uint8Array,
  nodeIdForProofWasGenerated: PeerId,
): Uint8Array | null =>
  (
      proof[0] === 0 &&
      merkleTreeBinary.check_proof(
        stateVersion,
        proof,
        nodeIdForProofWasGenerated,
        hash,
      )
    )
    ? proof.subarray(1, nodeIdForProofWasGenerated.length + 1)
    : null;

const computeStateVersion = (
  hash: HashFunction,
  id: PeerId,
  newState: State,
): ReturnType<typeof merkleTreeBinary.get_root> =>
  merkleTreeBinary.get_root(
    reduceStateToProofItems(id, newState),
    hash,
  );
