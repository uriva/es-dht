import * as crypto from "https://deno.land/std@0.177.0/node/crypto.ts";

import {
  DHT,
  HashedValue,
  Item,
  PeerId,
  StateVersion,
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
import { mapCat, randomElement, range } from "./utils.ts";

import arrayMapSet from "npm:array-map-set@^1.0.1";

const sha1 = (data: any): HashedValue =>
  crypto.createHash("sha1").update(data).digest();

const makeSimpleDHT = (id: PeerId) => ({
  id,
  dht: DHT(id, sha1, 20, 1000, 0.2),
  data: arrayMapSet.ArrayMap(),
});

type SimpleDHT = ReturnType<typeof makeSimpleDHT>;

const lookup = (send: Send) => (dht: SimpleDHT, id: PeerId): PeerId[] | null =>
  handleLookup(send)(dht, id, startLookup(dht.dht, id));

const nextNodesToConnectTo = (send: Send, id: PeerId, dht: SimpleDHT) =>
(
  [targetId, parentId, parentStateVersion]: [PeerId, PeerId, StateVersion],
) => {
  const targetStateVersion = checkStateProof(
    dht.dht,
    parentStateVersion,
    send(parentId, dht.id, "get_state_proof", [
      targetId,
      parentStateVersion,
    ]),
    targetId,
  );
  if (!targetStateVersion) throw new Error();
  const [proof, targetNodePeers] = send(
    targetId,
    dht.id,
    "get_state",
    targetStateVersion,
  );
  const checkResult = checkStateProof(
    dht.dht,
    targetStateVersion,
    proof,
    targetId,
  );
  if (checkResult && checkResult.join(",") === targetId.join(",")) {
    return updateLookup(
      dht.dht,
      id,
      targetId,
      targetStateVersion,
      targetNodePeers,
    );
  }
  throw new Error();
};

const handleLookup =
  (send: Send) =>
  (dht: SimpleDHT, id: PeerId, nodesToConnectTo: Item[]): PeerId[] | null => {
    if (!nodesToConnectTo.length) return finishLookup(dht.dht, id);
    return handleLookup(send)(
      dht,
      id,
      mapCat(nextNodesToConnectTo(send, id, dht))(nodesToConnectTo),
    );
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
  ref.forEach((element) => send(element, dht.id, "put", data));
  return infohash;
};

const get = (send: Send) => (dht: SimpleDHT, infohash: HashedValue) => {
  if (dht.data.has(infohash)) return dht.data.get(infohash);
  const ref = lookup(send)(dht, infohash);
  if (!ref) throw "missing info hash";
  for (const element of ref) {
    const data = send(element, dht.id, "get", infohash);
    if (data) return data;
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

Deno.test("es-dht", () => {
  const idToPeer = arrayMapSet.ArrayMap();
  const send = (
    recipient: PeerId,
    source_id: PeerId,
    command: string,
    data: any[],
  ) => response(idToPeer.get(recipient), source_id, command, data);
  const bootsrapPeer = crypto.randomBytes(20);
  idToPeer.set(bootsrapPeer, makeSimpleDHT(bootsrapPeer));
  const nodes = range(100).map(() => {
    const id = crypto.randomBytes(20);
    const x = makeSimpleDHT(id);
    idToPeer.set(id, x);
    const state = send(bootsrapPeer, x.id, "bootstrap", getState(x.dht, null));
    commitState(x.dht);
    if (state) {
      const [stateVersion, proof, peers] = state;
      setPeer(x.dht, bootsrapPeer, stateVersion, proof, peers);
    }
    return id;
  });
  const [alice, bob, carol] = range(3).map(() =>
    idToPeer.get(randomElement(nodes))
  );
  const data = crypto.randomBytes(10);
  const infohash = put(send)(alice, data);
  for (const peer of [alice, bob, carol]) {
    assertEquals(get(send)(peer, infohash), data);
  }
  const lookupNodes = lookup(send)(alice, crypto.randomBytes(20));
  assert(lookupNodes);
  assert(lookupNodes.length >= 2 && lookupNodes.length <= 20);
  assertInstanceOf(lookupNodes[0], Uint8Array);
  assertEquals(lookupNodes[0].length, 20);
  const peers = getState(alice.dht, null)[2];
  deletePeer(alice.dht, peers[peers.length - 1]);
});
