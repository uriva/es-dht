import * as crypto from "https://deno.land/std@0.177.0/node/crypto.ts";

import {
  DHT,
  HashedValue,
  Item,
  PeerId,
  check_state_proof,
  commit_state,
  del_peer,
  finishLookup,
  get_state,
  get_state_proof,
  set_peer,
  start_lookup,
  update_lookup,
} from "../src/index.ts";
import {
  assert,
  assertEquals,
  assertInstanceOf,
} from "https://deno.land/std@0.178.0/testing/asserts.ts";

import arrayMapSet from "npm:array-map-set@^1.0.1";

const sha1 = (data: any): HashedValue =>
  crypto.createHash("sha1").update(data).digest();

const makeSimpleDHT = (id: PeerId) => ({
  id,
  dht: DHT(id, sha1, 20, 1000, 0.2),
  data: arrayMapSet.ArrayMap(),
});

type SimpleDHT = ReturnType<typeof makeSimpleDHT>;

const lookup = (send: Send) => (dht: SimpleDHT, id: PeerId) => {
  _handleLookup(send)(dht, id, start_lookup(dht.dht, id, null));
  return finishLookup(dht.dht, id);
};
const _handleLookup =
  (send: Send) => (dht: SimpleDHT, id: PeerId, nodes_to_connect_to: Item[]) => {
    if (!nodes_to_connect_to.length) {
      return;
    }
    let nodes_for_next_round: Item[] = [];
    for (let i = 0; i < nodes_to_connect_to.length; ++i) {
      const [target_node_id, parent_node_id, parent_state_version] =
        nodes_to_connect_to[i];
      const target_node_state_version = check_state_proof(
        dht.dht,
        parent_state_version,
        send(parent_node_id, dht.id, "get_state_proof", [
          target_node_id,
          parent_state_version,
        ]),
        target_node_id,
      );
      if (target_node_state_version) {
        const [proof, target_node_peers] = send(
          target_node_id,
          dht.id,
          "get_state",
          target_node_state_version,
        );
        const checkResult = check_state_proof(
          dht.dht,
          target_node_state_version,
          proof,
          target_node_id,
        );
        if (checkResult && checkResult.join(",") === target_node_id.join(",")) {
          const x = update_lookup(
            dht.dht,
            id,
            target_node_id,
            target_node_state_version,
            target_node_peers,
          );
          nodes_for_next_round = nodes_for_next_round.concat(x);
        } else {
          throw new Error();
        }
      } else {
        throw new Error();
      }
    }
    _handleLookup(send)(dht, id, nodes_for_next_round);
  };

type Send = (
  recipient: PeerId,
  source: PeerId,
  command: string,
  args: any,
) => any;

const put = (send: Send) => (dht: SimpleDHT, data: any): HashedValue => {
  const infohash = sha1(data);
  dht.data.set(infohash, data);
  const ref = lookup(send)(dht, infohash);
  if (!ref) throw "missing info hash";
  for (let i = 0; i < ref.length; ++i) {
    send(ref[i], dht.id, "put", data);
  }
  return infohash;
};

const get = (send: Send) => (dht: SimpleDHT, infohash: HashedValue) => {
  if (dht.data.has(infohash)) {
    return dht.data.get(infohash);
  }
  const ref = lookup(send)(dht, infohash);
  if (!ref) throw "missing info hash";
  for (let i = 0; i < ref.length; ++i) {
    const data = send(ref[i], dht.id, "get", infohash);
    if (data) {
      return data;
    }
  }
  return null;
};

const response = (
  instance: SimpleDHT,
  source_id: PeerId,
  command: string,
  data: any,
): any => {
  switch (command) {
    case "bootstrap":
      return set_peer(instance.dht, source_id, data[0], data[1], data[2])
        ? (commit_state(instance.dht), get_state(instance.dht, null))
        : null;
    case "get":
      return instance.data.get(data) || null;
    case "put":
      instance.data.set(sha1(data), data);
      break;
    case "get_state_proof":
      return get_state_proof(instance.dht, data[1], data[0]);
    case "get_state": {
      const result = get_state(instance.dht, data);
      if (!result) throw "no state for peer";
      return result.slice(1);
    }
    case "put_state":
      set_peer(instance.dht, source_id, data[0], data[1], data[2]);
  }
};

Deno.test("es-dht", () => {
  console.log("Creating instances...");
  const instances = arrayMapSet.ArrayMap();

  const send = (
    recipient: PeerId,
    source_id: PeerId,
    command: string,
    data: any[],
  ) => response(instances.get(recipient), source_id, command, data);

  const nodes: PeerId[] = [];
  const bootsrapNodeId = crypto.randomBytes(20);
  const initial = makeSimpleDHT(bootsrapNodeId);
  instances.set(bootsrapNodeId, initial);
  for (let i = 0; i < 100; ++i) {
    const id = crypto.randomBytes(20);
    nodes.push(id);
    const x = makeSimpleDHT(id);
    instances.set(id, x);
    const firstState = get_state(x.dht, null);
    if (!firstState) throw "no last state";
    const state = send(
      bootsrapNodeId,
      x.id,
      "bootstrap",
      firstState,
    );
    commit_state(x.dht);
    if (state) {
      const [stateVersion, proof, peers] = state;
      set_peer(x.dht, bootsrapNodeId, stateVersion, proof, peers);
    }
  }
  console.log("Warm-up...");
  const alice = instances.get(nodes[Math.floor(nodes.length * Math.random())]);
  const bob = instances.get(nodes[Math.floor(nodes.length * Math.random())]);
  const carol = instances.get(nodes[Math.floor(nodes.length * Math.random())]);
  const data = crypto.randomBytes(10);
  const infohash = put(send)(alice, data);
  assert(infohash, "put succeeded");
  assertEquals(get(send)(alice, infohash), data, "get on alice succeeded");
  assertEquals(get(send)(bob, infohash), data, "get on bob succeeded");
  assertEquals(get(send)(carol, infohash), data, "get on carol succeeded");
  const lookupNodes = lookup(send)(alice, crypto.randomBytes(20));
  assert(lookupNodes);
  assert(lookupNodes.length >= 2 && lookupNodes.length <= 20);
  assertInstanceOf(lookupNodes[0], Uint8Array, "Node has correct ID type");
  assertEquals(lookupNodes[0].length, 20, "Node has correct ID length");
  const stateResult = get_state(alice.dht, null);
  if (!stateResult) throw "no last state";
  const peers = stateResult[2];
  del_peer(alice.dht, peers[peers.length - 1]);
  console.log("Peer deletion works fine");
});
