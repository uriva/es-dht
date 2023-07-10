import * as crypto from "https://deno.land/std@0.177.0/node/crypto.ts";

import {
  ArrayMap,
  ArraySet,
  arrayMapGet,
  entries,
  keys,
  makeMap,
  makeSet,
  mapHasArrayImmutable,
  mapRemoveArrayImmutable,
  mapSetArrayImmutable,
  mapSizeArrayImmutable,
  values,
} from "./containers.ts";
import {
  Bucket,
  bucketAdd,
  bucketAddAll,
  bucketElements,
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

export const startLookup = (d: DHT, target: PeerId): [DHT, Item[]] => {
  const { id, lookups, bucketSize, peers, latestState } = d;
  if (bucketHas(peers, target)) return [d, []];
  const bucket = getBucket(bucketSize, latestState[1], target);
  const alreadyConnected = makeSet(keys(latestState[1]));
  const parents = parenthood(latestState[1]);
  if (bucketHas(bucket, target)) {
    return [{
      ...d,
      lookups: mapSetArrayImmutable(lookups, target, [
        bucket,
        bucketSize,
        alreadyConnected,
      ]),
    }, [[
      target,
      arrayMapGet<PeerId>(parents, target),
      arrayMapGet<StateValue>(
        latestState[1],
        arrayMapGet<PeerId>(parents, target),
      )[0],
    ]]];
  }
  let modifiedBucket = bucketRemove(bucket, id);
  const maxFraction = Math.max(
    d.fractionOfNodesFromSamePeer,
    // TODO optimize
    1 / bucketElements(peers).length,
  );
  while (true) {
    const closestSoFar = closest(modifiedBucket, target, bucketSize);
    let originatedFrom = makeMap<number>([]);
    const maxCountAllowed = Math.ceil(closestSoFar.length * maxFraction);
    const state = latestState[1];
    const nodesToConnectTo = closestSoFar
      .map((closestNode: PeerId) => {
        if (mapHasArrayImmutable(parents, closestNode)) {
          const parentPeer = arrayMapGet(parents, closestNode);
          const count = mapHasArrayImmutable(originatedFrom, parentPeer)
            ? arrayMapGet(originatedFrom, parentPeer)
            : 0;
          originatedFrom = mapSetArrayImmutable(
            originatedFrom,
            parentPeer,
            count + 1,
          );
          if (count > maxCountAllowed) {
            modifiedBucket = bucketRemove(bucket, closestNode);
            return null;
          }
          const parentPeerState = arrayMapGet(state, parentPeer);
          if (parentPeerState) {
            return [closestNode, parentPeer, parentPeerState[0]];
          }
          throw "no state for parent id";
        }
        const count = mapHasArrayImmutable(originatedFrom, closestNode)
          ? arrayMapGet(originatedFrom, closestNode)
          : 0;
        originatedFrom = mapSetArrayImmutable(
          originatedFrom,
          closestNode,
          count + 1,
        );
        if (count > maxCountAllowed) {
          modifiedBucket = bucketRemove(bucket, closestNode);
        }
        return null;
      }).filter((x) => x) as Item[];
    if (nodesToConnectTo.length) {
      return [{
        ...d,
        lookups: mapSetArrayImmutable(lookups, target, [
          modifiedBucket,
          bucketSize,
          alreadyConnected,
        ]),
      }, nodesToConnectTo];
    }
  }
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

export const deletePeer = (d: DHT, peerId: PeerId): DHT => {
  const { latestState, peers, id } = d;
  if (!mapHasArrayImmutable(latestState[1], peerId)) return d;
  const newState = mapRemoveArrayImmutable(latestState[1], peerId);
  return {
    ...d,
    peers: bucketRemove(peers, peerId),
    latestState: [computeStateVersion(id, newState), newState],
    id,
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
export const commitState = (d: DHT): DHT => {
  const { cacheHistorySize, stateCache, latestState } = d;
  const [key, value] = latestState;
  if (mapHasArrayImmutable(stateCache, key)) return d;
  const newStateCache = mapSetArrayImmutable(stateCache, key, value);
  return {
    ...d,
    stateCache: mapSizeArrayImmutable(newStateCache) > cacheHistorySize
      ? mapRemoveArrayImmutable(newStateCache, keys(newStateCache)[0])
      : newStateCache,
  };
};

const getStateHelper = (
  latestState: [StateVersion, State],
  stateCache: ArrayMap<State>,
  stateVersion: StateVersion,
): [StateVersion, State] =>
  uint8ArraysEqual(stateVersion, latestState[0])
    ? latestState
    : [stateVersion, arrayMapGet(stateCache, stateVersion)];

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
