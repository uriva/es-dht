import arrayMapSet from "npm:array-map-set@^1.0.1";
import kBucketSync from "npm:k-bucket-sync@^0.1.3";
import merkleTreeBinary from "npm:merkle-tree-binary@^0.1.0";

const are_arrays_equal = (array1: Uint8Array, array2: Uint8Array): boolean => {
  if (array1 === array2) {
    return true;
  }
  if (array1.length !== array2.length) {
    return false;
  }
  for (let i$ = 0; i$ < array1.length; ++i$) {
    const key = i$;
    const item = array1[i$];
    if (item !== array2[key]) {
      return false;
    }
  }
  return true;
};

const concat = (array1: Uint8Array, array2: Uint8Array): Uint8Array => {
  const result = new Uint8Array(array1.length * 2);
  result.set(array1);
  result.set(array2, array1.length);
  return result;
};

const ArrayMap = arrayMapSet["ArrayMap"];
const ArraySet = arrayMapSet["ArraySet"];

const makeStateCache = (size: number) => ({ size, map: ArrayMap() });
type StateCache = ReturnType<typeof makeStateCache>;

const add = ({ map, size }: StateCache, key: Uint8Array, value: State) => {
  if (map.has(key)) {
    return;
  }
  map.set(key, value);
  if (map.size > size) {
    map["delete"](map.keys().next().value);
  }
};

const get = ({ map }: StateCache, key: Uint8Array) => map.get(key);

type DHT = ReturnType<typeof DHT>;
export type HashedValue = any;

export const DHT = (
  id: PeerId,
  hash: (data: any) => HashedValue,
  bucket_size: number,
  state_history_size: number, // How many versions of local history will be kept
  fraction_of_nodes_from_same_peer: number, // Max fraction of nodes originated from single peer allowed on lookup start, e.g. 0.2
) => ({
  id,
  id_length: id.length,
  hash,
  bucket_size,
  fraction_of_nodes_from_same_peer,
  state_cache: makeStateCache(state_history_size),
  latest_state: null as [StateVersion, State] | null,
  peers: kBucketSync(id, bucket_size),
  lookups: ArrayMap(),
});

export type PeerId = Uint8Array;
// [node_id, parent_peer_id, parent_peer_state_version]
export type Item = [PeerId, PeerId, StateVersion];

export const start_lookup = (
  dht: DHT,
  id: PeerId,
  // Number of nodes to be returned if exact match was not found, defaults to bucket size
  number: number | null,
): Item[] => {
  number = number || dht.bucket_size;
  if (dht.peers["has"](id)) {
    return [];
  }
  const bucket = kBucketSync(id, number);
  const parents = ArrayMap();
  const lastState = _get_state(dht, null);
  if (!lastState) throw "cannot lookup if state doesn't exist";
  const state = lastState[1];
  const already_connected = ArraySet();
  state.forEach((value, peer_id: PeerId) => {
    const [_, peer_peers] = value;
    already_connected.add(peer_id);
    bucket["set"](peer_id);
    for (let i = 0; i < peer_peers.length; ++i) {
      const peer_peer_id = peer_peers[i];
      if (!parents.has(peer_peer_id) && bucket["set"](peer_peer_id)) {
        parents.set(peer_peer_id, peer_id);
      }
    }
  });
  if (bucket["has"](id)) {
    const parentPeerState = state.get(parents.get(id));
    if (!parentPeerState) throw "id exists but no parent";
    dht.lookups.set(id, [bucket, number, already_connected]);
    return [[id, parents.get(id), parentPeerState[0]]];
  } else {
    bucket["del"](dht.id);
    const max_fraction = Math.max(
      dht.fraction_of_nodes_from_same_peer,
      1 / dht.peers["count"](),
    );
    let nodes_to_connect_to: Item[] = [];
    for (;;) {
      const closest_so_far = bucket["closest"](id, number);
      const closest_nodes_found = closest_so_far.length;
      const max_count_allowed = Math.ceil(closest_nodes_found * max_fraction);
      nodes_to_connect_to = [];
      const originated_from = ArrayMap();
      let retry = false;
      for (let i = 0; i < closest_so_far.length; ++i) {
        const closest_node_id = closest_so_far[i];
        const parent_peer_id = parents.get(closest_node_id);
        if (parent_peer_id) {
          const count = originated_from.get(parent_peer_id) || 0;
          originated_from.set(parent_peer_id, count + 1);
          if (count > max_count_allowed) {
            bucket["del"](closest_node_id);
            retry = true;
          } else {
            const parentPeerState = state.get(parent_peer_id);
            if (!parentPeerState) throw "no state for parent id";
            nodes_to_connect_to.push([
              closest_node_id,
              parent_peer_id,
              parentPeerState[0],
            ]);
          }
        } else {
          const count = originated_from.get(closest_node_id) || 0;
          originated_from.set(closest_node_id, count + 1);
          if (count > max_count_allowed) {
            bucket["del"](closest_node_id);
            retry = true;
          }
        }
      }
      if (!retry) {
        break;
      }
    }
    dht.lookups.set(id, [bucket, number, already_connected]);
    return nodes_to_connect_to;
  }
};

/**
 * @param {!Uint8Array}			node_state_version	Corresponding state version for `node_id`
 * @param {Array<!Uint8Array>}	node_peers			Peers of `node_id` at state `node_state_version` or `null` if connection to `node_id` have failed
 */
export const update_lookup = (
  dht: DHT,
  id: PeerId,
  node_id: PeerId,
  node_state_version: StateVersion,
  node_peers: PeerId[],
): Item[] => {
  const lookup = dht.lookups.get(id);
  if (!lookup) {
    return [];
  }
  const [bucket, number, already_connected] = lookup;
  if (!node_peers) {
    bucket["del"](node_id);
    return [];
  }
  already_connected.add(node_id);
  if (dht.peers["has"](id) || bucket["has"](id)) {
    return [];
  }
  const added_nodes = ArraySet();
  for (let i = 0; i < node_peers.length; ++i) {
    const node_peer_id = node_peers[i];
    if (!bucket["has"](node_peer_id) && bucket["set"](node_peer_id)) {
      added_nodes.add(node_peer_id);
    }
  }
  if (bucket["has"](id)) {
    return [[id, node_id, node_state_version]];
  }
  bucket["del"](dht.id);
  const closest_so_far = bucket["closest"](id, number);
  const nodes_to_connect_to: Item[] = [];
  for (let i = 0; i < closest_so_far.length; ++i) {
    const closest_node_id = closest_so_far[i];
    if (added_nodes.has(closest_node_id)) {
      nodes_to_connect_to.push([
        closest_node_id,
        node_id,
        node_state_version,
      ]);
    }
  }
  return nodes_to_connect_to;
};

/**
 * @return {Array<!Uint8Array>} `[id]` if node with specified ID was connected directly, an array of closest IDs if exact node wasn't found and `null` otherwise
 */
export const finishLookup = function (dht: DHT, id: PeerId): PeerId[] | null {
  const lookup = dht.lookups.get(id);
  dht.lookups["delete"](id);
  if (!lookup) {
    return null;
  }
  const [bucket, number, already_connected] = lookup;
  if (dht.peers["has"](id) || already_connected.has(id)) {
    return [id];
  }
  return bucket["closest"](id, number);
};

// Returns `false` if proof is not valid, returning `true` only means there was not errors, but peer was not necessarily added to k-bucket.
export const set_peer = (
  dht: DHT,
  peer_id: PeerId,
  peer_state_version: StateVersion,
  proof: Proof,
  peer_peers: PeerId[],
): boolean => {
  if (are_arrays_equal(dht.id, peer_id)) {
    return false;
  }
  const expected_number_of_items = peer_peers.length * 2 + 2;
  const proof_block_size = dht.id_length + 1;
  const expected_proof_height = Math.log2(expected_number_of_items);
  const proof_height = proof.length / proof_block_size;
  if (proof_height !== expected_proof_height) {
    if (proof_height !== Math.ceil(expected_proof_height)) {
      return false;
    }
    let last_block = peer_id;
    for (
      let i = 0,
        to$ = Math.ceil(
          Math.pow(Math.log2(expected_number_of_items), 2) -
            expected_number_of_items,
        ) / 2;
      i <= to$;
      ++i
    ) {
      const block = i;
      if (
        proof[block * proof_block_size] !== 0 ||
        !are_arrays_equal(
          proof.subarray(
            block * proof_block_size + 1,
            (block + 1) * proof_block_size,
          ),
          last_block,
        )
      ) {
        return false;
      }
      last_block = dht.hash(concat(last_block, last_block));
    }
  }
  const detected_peer_id = check_state_proof(
    dht,
    peer_state_version,
    proof,
    peer_id,
  );
  if (!detected_peer_id || !are_arrays_equal(detected_peer_id, peer_id)) {
    return false;
  }
  if (!dht.peers["set"](peer_id)) {
    return true;
  }
  const state = _get_state_copy(dht, null);
  state.set(peer_id, [peer_state_version, peer_peers]);
  insertState(dht, state);
  return true;
};

export const del_peer = (dht: DHT, peer_id: PeerId) => {
  const state = _get_state_copy(dht, null);
  if (!state.has(peer_id)) {
    return;
  }
  dht.peers["del"](peer_id);
  state["delete"](peer_id);
  insertState(dht, state);
};

export const get_state = (
  dht: DHT,
  state_version: StateVersion | null,
): [StateVersion, Proof, PeerId[]] | null => {
  const result = _get_state(dht, state_version);
  if (!result) {
    return null;
  }
  const [state_version2, state] = result;
  return [
    state_version2,
    get_state_proof(dht, state_version2, dht.id),
    Array.from(state.keys()),
  ];
};

/**
 * Commit current state into state history, needs to be called if current state was sent to any peer.
 * This allows to only store useful state versions in cache known to other peers and discard the rest.
 */
export const commit_state = (dht: DHT) => {
  if (!dht.latest_state) throw "cannot commit state if it isn't there";
  const [state_version, state] = dht.latest_state;
  add(dht.state_cache, state_version, state);
};

const _get_state = (
  dht: DHT,
  state_version: StateVersion | null,
): [StateVersion, State] | null => {
  if (
    !state_version ||
    dht.latest_state && are_arrays_equal(state_version, dht.latest_state[0])
  ) {
    return dht.latest_state;
  } else {
    const state = get(dht.state_cache, state_version);
    return state && [state_version, state];
  }
};

const _get_state_copy = (
  dht: DHT,
  state_version: StateVersion | null,
): State => {
  const temp = _get_state(dht, state_version);
  return temp && temp[1] && ArrayMap(Array.from(temp[1]));
};

// Generate proof about peer in current state version.
export const get_state_proof = (
  dht: DHT,
  state_version: StateVersion,
  peerId: PeerId,
): Proof => {
  const temp = _get_state(dht, state_version);
  const state = temp && temp[1];
  if (
    !state ||
    (!state.has(peerId) && !are_arrays_equal(peerId, dht.id))
  ) {
    return new Uint8Array(0);
  }
  return merkleTreeBinary["get_proof"](
    reduceStateToProofItems(dht, state),
    peerId,
    dht.hash,
  );
};

/**
 * @return {!Array<!Uint8Array>}
 */
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

export const check_state_proof = (
  { hash, id_length }: DHT,
  state_version: Uint8Array,
  proof: Uint8Array,
  nodeIdForProofWasGenerated: PeerId,
): Uint8Array | null =>
  (
      proof[0] === 0 &&
      merkleTreeBinary["check_proof"](
        state_version,
        proof,
        nodeIdForProofWasGenerated,
        hash,
      )
    )
    ? proof.subarray(1, id_length + 1)
    : null;

const insertState = (dht: DHT, newState: State) => {
  dht.latest_state = [
    merkleTreeBinary["get_root"](
      reduceStateToProofItems(dht, newState),
      dht.hash,
    ),
    newState,
  ];
};

type State = Map<PeerId, [StateVersion, PeerId[]]>;
type Proof = Uint8Array;
type StateVersion = Uint8Array;
