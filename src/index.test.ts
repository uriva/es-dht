import * as crypto from "https://deno.land/std@0.177.0/node/crypto.ts";

import {
  DHT,
  HashedValue,
  Item,
  PeerId,
  checkStateProof,
  commitState,
  deletePeer,
  finishLookup,
  getState,
  getStateProof,
  setPeer,
  startLookup,
  updateLookup,
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
  handleLookup(send)(dht, id, startLookup(dht.dht, id));
  return finishLookup(dht.dht, id);
};

const handleLookup =
  (send: Send) => (dht: SimpleDHT, id: PeerId, nodes_to_connect_to: Item[]) => {
    if (!nodes_to_connect_to.length) {
      return;
    }
    let nodes_for_next_round: Item[] = [];
    for (let i = 0; i < nodes_to_connect_to.length; ++i) {
      const [target_node_id, parent_node_id, parent_state_version] =
        nodes_to_connect_to[i];
      const target_node_state_version = checkStateProof(
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
        const checkResult = checkStateProof(
          dht.dht,
          target_node_state_version,
          proof,
          target_node_id,
        );
        if (checkResult && checkResult.join(",") === target_node_id.join(",")) {
          const x = updateLookup(
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
    handleLookup(send)(dht, id, nodes_for_next_round);
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
      return setPeer(instance.dht, source_id, data[0], data[1], data[2])
        ? (commitState(instance.dht), getState(instance.dht, null))
        : null;
    case "get":
      return instance.data.get(data) || null;
    case "put":
      instance.data.set(sha1(data), data);
      break;
    case "get_state_proof":
      return getStateProof(instance.dht, data[1], data[0]);
    case "get_state": {
      return getState(instance.dht, data).slice(1);
    }
    case "put_state":
      setPeer(instance.dht, source_id, data[0], data[1], data[2]);
  }
};

const randomElement = <T>(arr: T[]): T =>
  arr[Math.floor(arr.length * Math.random())];

Deno.test("es-dht", () => {
  const idToPeer = arrayMapSet.ArrayMap();
  const send = (
    recipient: PeerId,
    source_id: PeerId,
    command: string,
    data: any[],
  ) => response(idToPeer.get(recipient), source_id, command, data);
  const nodes: PeerId[] = [];
  const bootsrapNodeId = crypto.randomBytes(20);
  const initial = makeSimpleDHT(bootsrapNodeId);
  idToPeer.set(bootsrapNodeId, initial);
  for (let i = 0; i < 100; ++i) {
    const id = crypto.randomBytes(20);
    nodes.push(id);
    const x = makeSimpleDHT(id);
    idToPeer.set(id, x);
    const firstState = getState(x.dht, null);
    const state = send(
      bootsrapNodeId,
      x.id,
      "bootstrap",
      firstState,
    );
    commitState(x.dht);
    if (state) {
      const [stateVersion, proof, peers] = state;
      setPeer(x.dht, bootsrapNodeId, stateVersion, proof, peers);
    }
  }
  const alice = idToPeer.get(randomElement(nodes));
  const bob = idToPeer.get(randomElement(nodes));
  const carol = idToPeer.get(randomElement(nodes));
  const data = crypto.randomBytes(10);
  const infohash = put(send)(alice, data);
  assert(infohash);
  for (const peer of [alice, bob, carol]) {
    assertEquals(get(send)(peer, infohash), data);
  }
  const lookupNodes = lookup(send)(alice, crypto.randomBytes(20));
  assert(lookupNodes);
  assert(lookupNodes.length >= 2 && lookupNodes.length <= 20);
  assertInstanceOf(lookupNodes[0], Uint8Array);
  assertEquals(lookupNodes[0].length, 20);
  const stateResult = getState(alice.dht, null);
  const peers = stateResult[2];
  deletePeer(alice.dht, peers[peers.length - 1]);
});
