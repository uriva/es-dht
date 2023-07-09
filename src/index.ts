import * as crypto from "https://deno.land/std@0.177.0/node/crypto.ts";

import {
  ArrayMap,
  ArraySet,
  entries,
  keys,
  makeMap,
  makeSet,
  mapGetArrayImmutable,
  mapHasArrayImmutable,
  mapRemoveArrayImmutable,
  mapSetArrayImmutable,
  setAddArrayImmutable,
  setHasArrayImmutable,
  values,
} from "./containers.ts";
import {
  Bucket,
  bucketAdd,
  bucketAddAll,
  bucketHas,
  bucketRemove,
  closest,
  kBucket,
} from "./kBucket.ts";
import { concatUint8Array, uint8ArraysEqual } from "./utils.ts";

import merkleTreeBinary from "npm:merkle-tree-binary@^0.1.0";

export const sha1 = (data: any): HashedValue =>
  crypto.createHash("sha1").update(data).digest();

type StateValue = [StateVersion, PeerId[]];
type State = ArrayMap<StateValue>;
type Proof = Uint8Array;
export type StateVersion = ReturnType<typeof computeStateVersion>;
export type DHT = ReturnType<typeof makeDHT>;
export type HashedValue = any;

type VersionAndState = [StateVersion, State];
export type LookupValue = [Bucket, number, ArraySet];

export const makeDHT = <V>(
  id: PeerId,
  bucketSize: number,
  cacheHistorySize: number, // How many versions of local history will be kept
  fractionOfNodesFromSamePeer: number, // Max fraction of nodes originated from single peer allowed on lookup start, e.g. 0.2
) => ({
  data: makeMap<V>([]),
  id,
  bucketSize,
  fractionOfNodesFromSamePeer,
  cacheHistorySize,
  stateCache: makeMap<State>([]),
  latestState: [
    computeStateVersion(id, makeMap<StateValue>([])),
    makeMap<StateValue>([]),
  ] as VersionAndState,
  peers: kBucket(id, bucketSize),
  lookups: makeMap<LookupValue>([]),
});

export type PeerId = Uint8Array;
// [nodeId, parentPeerId, parentPeerStateVersion]
export type Item = [PeerId, PeerId, StateVersion];

const parenthood = (state: State) => {
  const parents: [PeerId, PeerId][] = [];
  entries(state).forEach(([peerId, [_, peerPeers]]) => {
    for (const peerPeerId of peerPeers) {
      parents.push([peerPeerId, peerId]);
    }
  });
  return makeMap<PeerId>(parents);
};

const getBucket = (bucketSize: number, state: State, target: PeerId) => {
  let bucket = bucketAddAll(kBucket(target, bucketSize), keys(state));
  values(state).forEach(([, peerPeers]) => {
    bucket = bucketAddAll(bucket, peerPeers);
  });
  return bucket;
};

export const EFFECT_startLookup = (
  { id, lookups, fractionOfNodesFromSamePeer, bucketSize, peers, latestState }:
    DHT,
  target: PeerId,
): Item[] => {
  if (bucketHas(peers, target)) return [];
  const bucket = getBucket(bucketSize, latestState[1], target);
  const alreadyConnected = makeSet(keys(latestState[1]));
  const parents = parenthood(latestState[1]);
  if (bucketHas(bucket, target)) {
    lookups.set(target, [bucket, bucketSize, alreadyConnected]);
    return [[
      target,
      mapGetArrayImmutable<PeerId>(parents, target),
      mapGetArrayImmutable<StateValue>(
        latestState[1],
        mapGetArrayImmutable<PeerId>(parents, target),
      )[0],
    ]];
  }
  bucket.del(id);
  const maxFraction = Math.max(fractionOfNodesFromSamePeer, 1 / peers.count());
  while (true) {
    const closestSoFar = closest(bucket, target, bucketSize);
    const originatedFrom = makeMap<number>([]);
    const maxCountAllowed = Math.ceil(closestSoFar.length * maxFraction);
    const state = latestState[1];
    const nodesToConnectTo = closestSoFar.map((closestNodeId: PeerId) => {
      if (mapHasArrayImmutable(parents, closestNode)) {
        const parentPeer = mapGetArrayImmutable(parents, closestNodeId);
        const count = mapHasArrayImmutable(originatedFrom, parentPeer)
          ? mapGetArrayImmutable(originatedFrom, parentPeer)
          : 0;
        originatedFrom.set(parentPeer, count + 1);
        if (count > maxCountAllowed) {
          bucket.del(closestNode);
          return null;
        }
        const parentPeerState = state.get(parentPeer);
        if (parentPeerState) {
          return [closestNode, parentPeer, parentPeerState[0]];
        }
        throw "no state for parent id";
      }
      const count = mapHasArrayImmutable(originatedFrom, closestNode)
        ? mapGetArrayImmutable(originatedFrom, closestNode)
        : 0;
      originatedFrom.set(closestNode, count + 1);
      if (count > maxCountAllowed) bucket.del(closestNode);
      return null;
    })
      .filter((x: Item | null) => x);
    if (nodesToConnectTo.length) {
      lookups.set(target, [bucket, bucketSize, alreadyConnected]);
      return nodesToConnectTo;
    }
  }
};

export const EFFECT_updateLookup = (
  peers: Bucket,
  lookup: LookupValue,
  id: PeerId,
  target: HashedValue,
  nodeId: PeerId,
  // Corresponding state version for `nodeId`
  nodeStateVersion: StateVersion,
  // Peers of `nodeId` at state `nodeStateVersion` or `null` if connection to `nodeId` have failed
  nodePeers: PeerId[],
): Item[] => {
  const [bucket, number, alreadyConnected] = lookup;
  if (!nodePeers) {
    bucket.del(nodeId);
    return [];
  }
  lookup[2] = setAddArrayImmutable(alreadyConnected, nodeId);
  if (bucketHas(peers, target) || bucketHas(bucket, target)) return [];
  const addedNodes = makeSet(
    // todo this must be split into two filters...
    nodePeers.filter((nodePeerId) =>
      !bucketHas(bucket, nodePeerId) && bucket.set(nodePeerId)
    ),
  );
  if (bucketHas(bucket, target)) return [[target, nodeId, nodeStateVersion]];
  bucket.del(id);
  return closest(bucket, target, number).filter((peer: PeerId) =>
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
  lookups: ArrayMap<LookupValue>,
  peers: Bucket,
  id: PeerId,
): PeerId[] => {
  const [bucket, number, alreadyConnected] = mapGetArrayImmutable(lookups, id);
  lookups.delete(id);
  return (bucketHas(peers, id) || setHasArrayImmutable(alreadyConnected, id))
    ? [id]
    : closest(bucket, id, number);
};

const checkProofLength = (
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
    lastBlock = sha1(concatUint8Array(lastBlock, lastBlock));
  }
  return true;
};

// Returns `undefined` if proof is invalid, otherwise the new DHT (possibly unmodified).
// TODO: check handling of return value in the tests.
export const setPeer = (
  d: DHT,
  peerId: PeerId,
  peerStateVersion: StateVersion,
  proof: Proof,
  neighbors: PeerId[],
): DHT | undefined => {
  const { id, peers, latestState, ...rest } = d;
  if (
    uint8ArraysEqual(id, peerId) ||
    !checkProofLength(id.length + 1, peerId, proof, neighbors.length)
  ) return;
  const detectedPeer = checkStateProof(peerStateVersion, proof, peerId);
  if (!detectedPeer || !uint8ArraysEqual(detectedPeer, peerId)) return;
  if (bucketHas(peers, peerId)) return d;
  const state = mapSetArrayImmutable<StateValue>(latestState[1], peerId, [
    peerStateVersion,
    neighbors,
  ]);
  return {
    ...rest,
    id,
    peers: bucketAdd(peers, peerId),
    latestState: [computeStateVersion(id, state), state],
  };
};

// TODO: propagate effects of this being immutable.
export const deletePeer = (d: DHT, peerId: PeerId): DHT => {
  const { latestState, peers, id, ...rest } = d;
  if (!mapHasArrayImmutable(latestState[1], peerId)) return d;
  const newState = mapRemoveArrayImmutable(latestState[1], peerId);
  return {
    peers: bucketRemove(peers, peerId),
    latestState: [computeStateVersion(id, newState), newState],
    id,
    ...rest,
  };
};

export const getState = (
  { latestState, stateCache, id }: DHT,
  stateVersion: StateVersion | null,
): [StateVersion, Proof, PeerId[]] => {
  const [stateVersion2, state] = stateVersion
    ? getStateHelper(latestState, stateCache, stateVersion)
    : latestState;
  return [
    stateVersion2,
    getStateProof(latestState, stateCache, id, stateVersion2, id),
    keys(state),
  ];
};

// Commit current state into state history.
// Needs to be called if current state was sent to any peer.
// This allows to only store useful state versions in cache known to other peers
// and discard the rest.
export const EFFECT_commitState = (
  size: number,
  stateCache: ArrayMap<State>,
  latestState: [StateVersion, State],
) => {
  const [key, value] = latestState;
  if (stateCache.has(key)) return;
  stateCache.set(key, value);
  if (stateCache.size > size) stateCache.delete(stateCache.keys().next().value);
};

const getStateHelper = (
  latestState: [StateVersion, State],
  stateCache: ArrayMap<State>,
  stateVersion: StateVersion,
): [StateVersion, State] =>
  uint8ArraysEqual(stateVersion, latestState[0])
    ? latestState
    : [stateVersion, mapGetArrayImmutable(stateCache, stateVersion)];

// Generate proof about peer in current state version.
export const getStateProof = (
  latestState: [StateVersion, State],
  stateCache: ArrayMap<State>,
  id: PeerId,
  stateVersion: StateVersion,
  peerId: PeerId,
): Proof => {
  const [_, state] = getStateHelper(latestState, stateCache, stateVersion);
  return (mapHasArrayImmutable(state, peerId) || uint8ArraysEqual(peerId, id))
    ? merkleTreeBinary.get_proof(
      reduceStateToProofItems(id, state),
      peerId,
      sha1,
    )
    : new Uint8Array(0);
};

const reduceStateToProofItems = (
  id: PeerId,
  state: State,
): (PeerId | StateVersion)[] => {
  const items = [];
  entries(state).forEach(([peerId, [peerStateVersion]]) => {
    items.push(peerId, peerStateVersion);
  });
  items.push(id, id);
  return items;
};

export const checkStateProof = (
  stateVersion: Uint8Array,
  proof: Uint8Array,
  proofTarget: PeerId,
): Uint8Array | null =>
  (proof[0] === 0 &&
      merkleTreeBinary.check_proof(stateVersion, proof, proofTarget, sha1))
    ? proof.subarray(1, proofTarget.length + 1)
    : null;

const computeStateVersion = (
  id: PeerId,
  newState: State,
): ReturnType<typeof merkleTreeBinary.get_root> =>
  merkleTreeBinary.get_root(reduceStateToProofItems(id, newState), sha1);
