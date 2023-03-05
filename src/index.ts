import arrayMapSet from "npm:array-map-set@^1.0.1";
import kBucketSync from "npm:k-bucket-sync@^0.1.3";
import merkleTreeBinary from "npm:merkle-tree-binary@^0.1.0";

type State = Map<PeerId, [StateVersion, PeerId[]]>;
type Proof = Uint8Array;
type StateVersion = Uint8Array;
type StateCache = ReturnType<typeof makeStateCache>;
type DHT = ReturnType<typeof DHT>;
export type HashedValue = any;

const arraysEqual = (x: Uint8Array, y: Uint8Array): boolean => {
  if (x === y) {
    return true;
  }
  if (x.length !== y.length) {
    return false;
  }
  for (let i = 0; i < x.length; ++i) {
    if (x[i] !== y[i]) {
      return false;
    }
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
  hash: (data: any) => HashedValue,
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
  latestState: null as [StateVersion, State] | null,
  peers: kBucketSync(id, bucketSize),
  lookups: ArrayMap(),
});

export type PeerId = Uint8Array;
// [nodeId, parentPeerId, parentPeerStateVersion]
export type Item = [PeerId, PeerId, StateVersion];

export const startLookup = (
  dht: DHT,
  id: PeerId,
  // Number of nodes to be returned if exact match was not found, defaults to bucket size
  number: number | null,
): Item[] => {
  number = number || dht.bucketSize;
  if (dht.peers.has(id)) {
    return [];
  }
  const bucket = kBucketSync(id, number);
  const parents = ArrayMap();
  const lastState = getStateHelper(dht, null);
  if (!lastState) throw "cannot lookup if state doesn't exist";
  const state = lastState[1];
  const alreadyConnected = ArraySet();
  state.forEach((value, peerId: PeerId) => {
    const [_, peerPeers] = value;
    alreadyConnected.add(peerId);
    bucket.set(peerId);
    for (let i = 0; i < peerPeers.length; ++i) {
      const peerPeerId = peerPeers[i];
      if (!parents.has(peerPeerId) && bucket.set(peerPeerId)) {
        parents.set(peerPeerId, peerId);
      }
    }
  });
  if (bucket.has(id)) {
    const parentPeerState = state.get(parents.get(id));
    if (!parentPeerState) throw "id exists but no parent";
    dht.lookups.set(id, [bucket, number, alreadyConnected]);
    return [[id, parents.get(id), parentPeerState[0]]];
  } else {
    bucket.del(dht.id);
    const maxFraction = Math.max(
      dht.fractionOfNodesFromSamePeer,
      1 / dht.peers.count(),
    );
    let nodesToConnectTo: Item[] = [];
    for (;;) {
      const closestSoFar = bucket.closest(id, number);
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
      if (!retry) {
        break;
      }
    }
    dht.lookups.set(id, [bucket, number, alreadyConnected]);
    return nodesToConnectTo;
  }
};

export const updateLookup = (
  dht: DHT,
  id: PeerId,
  nodeId: PeerId,
  nodeStateVersion: StateVersion, // Corresponding state version for `nodeId`
  nodePeers: PeerId[], // Peers of `nodeId` at state `nodeStateVersion` or `null` if connection to `nodeId` have failed
): Item[] => {
  const lookup = dht.lookups.get(id);
  if (!lookup) {
    return [];
  }
  const [bucket, number, alreadyConnected] = lookup;
  if (!nodePeers) {
    bucket.del(nodeId);
    return [];
  }
  alreadyConnected.add(nodeId);
  if (dht.peers.has(id) || bucket.has(id)) {
    return [];
  }
  const addedNodes = ArraySet();
  for (let i = 0; i < nodePeers.length; ++i) {
    const nodePeerId = nodePeers[i];
    if (!bucket.has(nodePeerId) && bucket.set(nodePeerId)) {
      addedNodes.add(nodePeerId);
    }
  }
  if (bucket.has(id)) {
    return [[id, nodeId, nodeStateVersion]];
  }
  bucket.del(dht.id);
  return bucket.closest(id, number).filter((peer: PeerId) =>
    addedNodes.has(peer)
  ).map(
    (peer: PeerId) => [
      peer,
      nodeId,
      nodeStateVersion,
    ],
  );
};

/**
 * @return {Array<!Uint8Array>} `[id]` if node with specified ID was connected directly, an array of closest IDs if exact node wasn't found and `null` otherwise
 */
export const finishLookup = (dht: DHT, id: PeerId): PeerId[] | null => {
  const lookup = dht.lookups.get(id);
  dht.lookups.delete(id);
  if (!lookup) {
    return null;
  }
  const [bucket, number, alreadyConnected] = lookup;
  if (dht.peers.has(id) || alreadyConnected.has(id)) {
    return [id];
  }
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
  if (arraysEqual(dht.id, peerId)) {
    return false;
  }
  const expectedNumberOfItems = peerPeers.length * 2 + 2;
  const proofBlockSize = dht.idLength + 1;
  const expectedProofHeight = Math.log2(expectedNumberOfItems);
  const proofHeight = proof.length / proofBlockSize;
  if (proofHeight !== expectedProofHeight) {
    if (proofHeight !== Math.ceil(expectedProofHeight)) {
      return false;
    }
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
  if (!detectedPeerId || !arraysEqual(detectedPeerId, peerId)) {
    return false;
  }
  if (!dht.peers.set(peerId)) {
    return true;
  }
  const state = getStateCopy(dht, null);
  state.set(peerId, [peerStateVersion, peerPeers]);
  insertState(dht, state);
  return true;
};

export const deletePeer = (dht: DHT, peerId: PeerId) => {
  const state = getStateCopy(dht, null);
  if (!state.has(peerId)) {
    return;
  }
  dht.peers.del(peerId);
  state.delete(peerId);
  insertState(dht, state);
};

export const getState = (
  dht: DHT,
  stateVersion: StateVersion | null,
): [StateVersion, Proof, PeerId[]] | null => {
  const result = getStateHelper(dht, stateVersion);
  if (!result) {
    return null;
  }
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
  dht: DHT,
  stateVersion: StateVersion | null,
): [StateVersion, State] | null => {
  if (
    !stateVersion ||
    dht.latestState && arraysEqual(stateVersion, dht.latestState[0])
  ) {
    return dht.latestState;
  } else {
    const state = get(dht.stateCache, stateVersion);
    return state && [stateVersion, state];
  }
};

const getStateCopy = (
  dht: DHT,
  stateVersion: StateVersion | null,
): State => {
  const temp = getStateHelper(dht, stateVersion);
  return temp && temp[1] && ArrayMap(Array.from(temp[1]));
};

// Generate proof about peer in current state version.
export const getStateProof = (
  dht: DHT,
  stateVersion: StateVersion,
  peerId: PeerId,
): Proof => {
  const temp = getStateHelper(dht, stateVersion);
  const state = temp && temp[1];
  if (
    !state ||
    (!state.has(peerId) && !arraysEqual(peerId, dht.id))
  ) {
    return new Uint8Array(0);
  }
  return merkleTreeBinary.get_proof(
    reduceStateToProofItems(dht, state),
    peerId,
    dht.hash,
  );
};

const reduceStateToProofItems = (
  dht: DHT,
  state: State,
): (PeerId | StateVersion)[] => {
  const items = [];
  state.forEach(([peerStateVersion], peerId: PeerId) => {
    items.push(peerId, peerStateVersion);
  });
  items.push(dht.id, dht.id);
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

const insertState = (dht: DHT, newState: State) => {
  dht.latestState = [
    merkleTreeBinary.get_root(
      reduceStateToProofItems(dht, newState),
      dht.hash,
    ),
    newState,
  ];
};
