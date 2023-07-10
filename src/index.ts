import * as crypto from "https://deno.land/std@0.177.0/node/crypto.ts";

import {
  ArrayMap,
  ArraySet,
  arrayMapEntries,
  arrayMapGet,
  arrayMapHas,
  arrayMapKeys,
  arrayMapRemove,
  arrayMapSet,
  arrayMapSize,
  arrayMapValues,
  makeArrayMap,
  makeArraySet,
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
import { concatUint8Array, mapCat, uint8ArraysEqual } from "./utils.ts";

import merkleTreeBinary from "npm:merkle-tree-binary@^0.1.0";

export const sha1 = (data: any): HashedValue =>
  crypto.createHash("sha1").update(data).digest();

type PeerState = [Version, PeerId[]];
type PeerToPeerState = ArrayMap<PeerState>;
type Proof = Uint8Array;
export type Version = ReturnType<typeof merkleTreeBinary.get_root>;
export type DHT = ReturnType<typeof makeDHT>;
export type HashedValue = any;

export type LookupValue = [Bucket, number, ArraySet];

export const makeDHT = <V>(
  id: PeerId,
  bucketSize: number,
  maxCacheSize: number,
  fractionOfNodesFromSamePeer: number, // Max fraction of nodes originated from single peer allowed on lookup start, e.g. 0.2
) => ({
  data: makeArrayMap<V>([]),
  id,
  bucketSize,
  fractionOfNodesFromSamePeer,
  maxCacheSize,
  cache: makeArrayMap<PeerToPeerState>([]),
  version: computeStateVersion(id, makeArrayMap<PeerState>([])),
  state: makeArrayMap<PeerState>([]),
  peers: kBucket(id, bucketSize),
  lookups: makeArrayMap<LookupValue>([]),
});

export type PeerId = Uint8Array;
// [nodeId, parentPeerId, parentPeerStateVersion]
export type Item = [PeerId, PeerId, Version];

const parenthood = (state: PeerToPeerState) => {
  const parents: [PeerId, PeerId][] = [];
  arrayMapEntries(state).forEach(([peerId, [_, peerPeers]]) => {
    for (const peerPeerId of peerPeers) {
      parents.push([peerPeerId, peerId]);
    }
  });
  return makeArrayMap<PeerId>(parents);
};

const getBucket = (
  bucketSize: number,
  state: PeerToPeerState,
  target: PeerId,
) => {
  let bucket = bucketAddAll(kBucket(target, bucketSize), arrayMapKeys(state));
  arrayMapValues(state).forEach(([, peerPeers]) => {
    bucket = bucketAddAll(bucket, peerPeers);
  });
  return bucket;
};

export const startLookup = (d: DHT, target: PeerId): [DHT, Item[]] => {
  const { id, lookups, bucketSize, peers, state } = d;
  if (bucketHas(peers, target)) return [d, []];
  const bucket = getBucket(bucketSize, state, target);
  const alreadyConnected = makeArraySet(arrayMapKeys(state));
  const parents = parenthood(state);
  if (bucketHas(bucket, target)) {
    return [{
      ...d,
      lookups: arrayMapSet(lookups, target, [
        bucket,
        bucketSize,
        alreadyConnected,
      ]),
    }, [[
      target,
      arrayMapGet<PeerId>(parents, target),
      arrayMapGet<PeerState>(state, arrayMapGet<PeerId>(parents, target))[0],
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
    let originatedFrom = makeArrayMap<number>([]);
    const maxCountAllowed = Math.ceil(closestSoFar.length * maxFraction);
    const nodesToConnectTo = closestSoFar
      .map((closestNode: PeerId) => {
        if (arrayMapHas(parents, closestNode)) {
          const parentPeer = arrayMapGet(parents, closestNode);
          const count = arrayMapHas(originatedFrom, parentPeer)
            ? arrayMapGet(originatedFrom, parentPeer)
            : 0;
          originatedFrom = arrayMapSet(
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
        const count = arrayMapHas(originatedFrom, closestNode)
          ? arrayMapGet(originatedFrom, closestNode)
          : 0;
        originatedFrom = arrayMapSet(
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
        lookups: arrayMapSet(lookups, target, [
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
  peerStateVersion: Version,
  proof: Proof,
  neighbors: PeerId[],
): DHT | undefined => {
  const { id, peers, state, ...rest } = d;
  if (
    uint8ArraysEqual(id, peerId) ||
    !checkProofLength(id.length + 1, peerId, proof, neighbors.length)
  ) return;
  const detectedPeer = checkStateProof(peerStateVersion, proof, peerId);
  if (!detectedPeer || !uint8ArraysEqual(detectedPeer, peerId)) return;
  if (bucketHas(peers, peerId)) return d;
  const newState = arrayMapSet<PeerState>(state, peerId, [
    peerStateVersion,
    neighbors,
  ]);
  return {
    ...rest,
    id,
    peers: bucketAdd(peers, peerId),
    version: computeStateVersion(id, newState),
    state: newState,
  };
};

export const deletePeer = (d: DHT, peerId: PeerId): DHT => {
  const { state, peers, id } = d;
  if (!arrayMapHas(state, peerId)) return d;
  const newState = arrayMapRemove(state, peerId);
  return {
    ...d,
    peers: bucketRemove(peers, peerId),
    version: computeStateVersion(id, newState),
    state: newState,
    id,
  };
};

export const getState = (
  d: DHT,
  desiredVersion: Version | null,
): [Version, Proof, PeerId[]] => {
  const { version, state, cache, id } = d;
  const [version2, newState] = desiredVersion
    ? [
      desiredVersion,
      uint8ArraysEqual(desiredVersion, version)
        ? state
        : arrayMapGet(cache, desiredVersion),
    ]
    : [version, state];
  return [version2, getStateProof(d, version2, id), arrayMapKeys(newState)];
};

// Commit current state into cache.
// Needs to be called if current state was sent to any peer.
// This allows to only store useful state versions in cache known to other peers
// and discard the rest.
export const cacheState = (d: DHT): DHT => {
  const { maxCacheSize, cache, version, state } = d;
  const newCache = arrayMapSet(cache, version, state);
  return {
    ...d,
    cache: arrayMapSize(newCache) > maxCacheSize
      ? arrayMapRemove(newCache, arrayMapKeys(newCache)[0])
      : newCache,
  };
};

// Generate proof about peer in current state version.
export const getStateProof = (
  d: DHT,
  stateVersion: Version,
  peerId: PeerId,
): Proof => {
  const { version, state, cache, id } = d;
  const newState = uint8ArraysEqual(stateVersion, version)
    ? state
    : arrayMapGet(cache, stateVersion);
  return (arrayMapHas(newState, peerId) ||
      uint8ArraysEqual(peerId, id))
    ? merkleTreeBinary.get_proof(stateToProofItems(id, newState), peerId, sha1)
    : new Uint8Array(0);
};

const stateToProofItems = (
  id: PeerId,
  state: PeerToPeerState,
): (PeerId | Version)[] => {
  const items = mapCat<[PeerId, PeerState], PeerId | Version>((
    [peerId, [peerStateVersion]],
  ) => [peerId, peerStateVersion])(arrayMapEntries(state));
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
  newState: PeerToPeerState,
): Version => merkleTreeBinary.get_root(stateToProofItems(id, newState), sha1);
