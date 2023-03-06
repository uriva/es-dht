import arrayMapSet from "npm:array-map-set@^1.0.1";
import kBucketSync from "npm:k-bucket-sync@^0.1.3";
import merkleTreeBinary from "npm:merkle-tree-binary@^0.1.0";

type HashFunction = (data: any) => HashedValue;
type State = Map<PeerId, [StateVersion, PeerId[]]>;
type Proof = Uint8Array;
type StateVersion = Uint8Array;
type StateCache = ReturnType<typeof makeStateCache>;
type DHT = ReturnType<typeof DHT>;
export type HashedValue = any;

const arraysEqual = (x: Uint8Array, y: Uint8Array): boolean => {
  if (x === y) return true;
  if (x.length !== y.length) return false;
  for (let i = 0; i < x.length; ++i) {
    if (x[i] !== y[i]) return false;
  }
  return true;
};

const concat = (x: Uint8Array, y: Uint8Array): Uint8Array => {
  const result = new Uint8Array(x.length * 2);
  result.set(x);
  result.set(y, x.length);
  return result;
};

const { ArrayMap, ArraySet } = arrayMapSet;

const makeStateCache = (size: number) => ({ size, map: ArrayMap() });

const add = ({ map, size }: StateCache, key: Uint8Array, value: State) => {
  if (map.has(key)) {
    return;
  }
  map.set(key, value);
  if (map.size > size) {
    map.delete(map.keys().next().value);
  }
};

const get = ({ map }: StateCache, key: Uint8Array) => map.get(key);

export const DHT = (
  id: PeerId,
  hash: HashFunction,
  bucketSize: number,
  stateHistorySize: number, // How many versions of local history will be kept
  fractionOfNodesFromSamePeer: number, // Max fraction of nodes originated from single peer allowed on lookup start, e.g. 0.2
) => ({
  id,
  idLength: id.length,
  hash,
  bucketSize,
  fractionOfNodesFromSamePeer,
  stateCache: makeStateCache(stateHistorySize),
  latestState: makeLatestState(hash, id, new Map()),
  peers: kBucketSync(id, bucketSize),
  lookups: ArrayMap(),
});

export type PeerId = Uint8Array;
// [nodeId, parentPeerId, parentPeerStateVersion]
export type Item = [PeerId, PeerId, StateVersion];

export const startLookup = (
  { id, lookups, fractionOfNodesFromSamePeer, bucketSize, peers, latestState }:
    DHT,
  target: PeerId,
): Item[] => {
  if (peers.has(target)) return [];
  const bucket = kBucketSync(target, bucketSize);
  const parents = ArrayMap();
  if (!latestState) throw "cannot lookup if state doesn't exist";
  const state = latestState[1];
  const alreadyConnected = ArraySet();
  state.forEach(([_, peerPeers], peerId: PeerId) => {
    alreadyConnected.add(peerId);
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
  let nodesToConnectTo: Item[] = [];
  for (;;) {
    const closestSoFar = bucket.closest(target, bucketSize);
    const maxCountAllowed = Math.ceil(closestSoFar.length * maxFraction);
    nodesToConnectTo = [];
    const originatedFrom = ArrayMap();
    let retry = false;
    for (const closestNodeId of closestSoFar) {
      const parentPeerId = parents.get(closestNodeId);
      if (parentPeerId) {
        const count = originatedFrom.get(parentPeerId) || 0;
        originatedFrom.set(parentPeerId, count + 1);
        if (count > maxCountAllowed) {
          bucket.del(closestNodeId);
          retry = true;
        } else {
          const parentPeerState = state.get(parentPeerId);
          if (!parentPeerState) throw "no state for parent id";
          nodesToConnectTo.push([
            closestNodeId,
            parentPeerId,
            parentPeerState[0],
          ]);
        }
      } else {
        const count = originatedFrom.get(closestNodeId) || 0;
        originatedFrom.set(closestNodeId, count + 1);
        if (count > maxCountAllowed) {
          bucket.del(closestNodeId);
          retry = true;
        }
      }
    }
    if (!retry) break;
  }
  lookups.set(target, [bucket, bucketSize, alreadyConnected]);
  return nodesToConnectTo;
};

export const updateLookup = (
  dht: DHT,
  id: PeerId,
  nodeId: PeerId,
  // Corresponding state version for `nodeId`
  nodeStateVersion: StateVersion,
  // Peers of `nodeId` at state `nodeStateVersion` or `null` if connection to `nodeId` have failed
  nodePeers: PeerId[],
): Item[] => {
  const lookup = dht.lookups.get(id);
  if (!lookup) return [];
  const [bucket, number, alreadyConnected] = lookup;
  if (!nodePeers) {
    bucket.del(nodeId);
    return [];
  }
  alreadyConnected.add(nodeId);
  if (dht.peers.has(id) || bucket.has(id)) return [];
  const addedNodes = ArraySet();
  for (const nodePeerId of nodePeers) {
    if (!bucket.has(nodePeerId) && bucket.set(nodePeerId)) {
      addedNodes.add(nodePeerId);
    }
  }
  if (bucket.has(id)) return [[id, nodeId, nodeStateVersion]];
  bucket.del(dht.id);
  return bucket.closest(id, number).filter((peer: PeerId) =>
    addedNodes.has(peer)
  ).map((peer: PeerId) => [
    peer,
    nodeId,
    nodeStateVersion,
  ]);
};

// Returns `[id]` if node with specified ID was connected directly,
// an array of closest IDs if exact node wasn't found and `null` otherwise.
export const finishLookup = (dht: DHT, id: PeerId): PeerId[] | null => {
  const lookup = dht.lookups.get(id);
  dht.lookups.delete(id);
  if (!lookup) return null;
  const [bucket, number, alreadyConnected] = lookup;
  if (dht.peers.has(id) || alreadyConnected.has(id)) return [id];
  return bucket.closest(id, number);
};

// Returns `false` if proof is not valid, returning `true` only means there was not errors, but peer was not necessarily added to k-bucket.
export const setPeer = (
  dht: DHT,
  peerId: PeerId,
  peerStateVersion: StateVersion,
  proof: Proof,
  peerPeers: PeerId[],
): boolean => {
  if (arraysEqual(dht.id, peerId)) return false;
  const expectedNumberOfItems = peerPeers.length * 2 + 2;
  const proofBlockSize = dht.idLength + 1;
  const expectedProofHeight = Math.log2(expectedNumberOfItems);
  const proofHeight = proof.length / proofBlockSize;
  if (proofHeight !== expectedProofHeight) {
    if (proofHeight !== Math.ceil(expectedProofHeight)) return false;
    let lastBlock = peerId;
    for (
      let i = 0,
        to$ = Math.ceil(
          Math.pow(Math.log2(expectedNumberOfItems), 2) -
            expectedNumberOfItems,
        ) / 2;
      i <= to$;
      ++i
    ) {
      const block = i;
      if (
        proof[block * proofBlockSize] !== 0 ||
        !arraysEqual(
          proof.subarray(
            block * proofBlockSize + 1,
            (block + 1) * proofBlockSize,
          ),
          lastBlock,
        )
      ) {
        return false;
      }
      lastBlock = dht.hash(concat(lastBlock, lastBlock));
    }
  }
  const detectedPeerId = checkStateProof(
    dht,
    peerStateVersion,
    proof,
    peerId,
  );
  if (!detectedPeerId || !arraysEqual(detectedPeerId, peerId)) return false;
  if (dht.peers.set(peerId)) {
    const state = getStateCopy(dht);
    state.set(peerId, [peerStateVersion, peerPeers]);
    dht.latestState = makeLatestState(dht.hash, dht.id, state);
  }
  return true;
};

export const deletePeer = (dht: DHT, peerId: PeerId) => {
  const state = getStateCopy(dht);
  if (!state.has(peerId)) return;
  dht.peers.del(peerId);
  state.delete(peerId);
  dht.latestState = makeLatestState(dht.hash, dht.id, state);
};

export const getState = (
  dht: DHT,
  stateVersion: StateVersion | null,
): [StateVersion, Proof, PeerId[]] | null => {
  const result = stateVersion
    ? getStateHelper(dht, stateVersion)
    : dht.latestState;
  if (!result) return null;
  const [stateVersion2, state] = result;
  return [
    stateVersion2,
    getStateProof(dht, stateVersion2, dht.id),
    Array.from(state.keys()),
  ];
};

/**
 * Commit current state into state history, needs to be called if current state was sent to any peer.
 * This allows to only store useful state versions in cache known to other peers and discard the rest.
 */
export const commitState = (dht: DHT) => {
  if (!dht.latestState) throw "cannot commit state if it isn't there";
  const [stateVersion, state] = dht.latestState;
  add(dht.stateCache, stateVersion, state);
};

const getStateHelper = (
  { latestState, stateCache }: DHT,
  stateVersion: StateVersion,
): [StateVersion, State] | null => {
  if (latestState && arraysEqual(stateVersion, latestState[0])) {
    return latestState;
  }
  const state = get(stateCache, stateVersion);
  return state && [stateVersion, state];
};

const getStateCopy = ({ latestState }: DHT): State => {
  if (!latestState) throw "no state";
  return ArrayMap(Array.from(latestState));
};

// Generate proof about peer in current state version.
export const getStateProof = (
  dht: DHT,
  stateVersion: StateVersion,
  peerId: PeerId,
): Proof => {
  const temp = getStateHelper(dht, stateVersion);
  const state = temp && temp[1];
  return (
      !state ||
      (!state.has(peerId) && !arraysEqual(peerId, dht.id))
    )
    ? new Uint8Array(0)
    : merkleTreeBinary.get_proof(
      reduceStateToProofItems(dht.id, state),
      peerId,
      dht.hash,
    );
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
  { hash, idLength }: DHT,
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
    ? proof.subarray(1, idLength + 1)
    : null;

const makeLatestState = (
  hash: HashFunction,
  id: PeerId,
  newState: State,
): [StateVersion, State] => [
  merkleTreeBinary.get_root(
    reduceStateToProofItems(id, newState),
    hash,
  ),
  newState,
];