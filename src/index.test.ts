import * as crypto from "https://deno.land/std@0.177.0/node/crypto.ts";

import {
  ArrayMap,
  Bucket,
  DHT,
  EFFECT_commitState,
  EFFECT_deletePeer,
  EFFECT_finishLookup,
  EFFECT_setPeer,
  EFFECT_startLookup,
  EFFECT_updateLookup,
  HashFunction,
  HashedValue,
  Item,
  PeerId,
  StateVersion,
  checkStateProof,
  getState,
  getStateProof,
  makeDHT,
} from "../src/index.ts";
import {
  assert,
  assertEquals,
  assertInstanceOf,
} from "https://deno.land/std@0.178.0/testing/asserts.ts";
import {
  last,
  mapCat,
  randomElement,
  range,
  uint8ArraysEqual,
} from "./utils.ts";

const sha1 = (data: any): HashedValue =>
  crypto.createHash("sha1").update(data).digest();

const makeSimpleDHT = (id: PeerId) => makeDHT(id, sha1, 20, 1000, 0.2);

const nextNodesToConnectTo = (
  send: Send,
  infoHash: HashedValue,
  id: PeerId,
  hash: HashFunction,
  peers: Bucket,
  lookups: ReturnType<typeof ArrayMap>,
) =>
(
  [target, parentId, parentStateVersion]: [PeerId, PeerId, StateVersion],
) => {
  const targetStateVersion = checkStateProof(
    hash,
    parentStateVersion,
    send(parentId, id, "get_state_proof", [target, parentStateVersion]),
    target,
  );
  if (!targetStateVersion) throw new Error();
  const [proof, targetNodePeers] = send(
    target,
    id,
    "get_state",
    targetStateVersion,
  );
  const checkResult = checkStateProof(hash, targetStateVersion, proof, target);
  if (!checkResult || !uint8ArraysEqual(checkResult, target)) throw new Error();
  return EFFECT_updateLookup(
    peers,
    lookups,
    id,
    infoHash,
    target,
    targetStateVersion,
    targetNodePeers,
  );
};

const lookup = (send: Send) =>
(
  id: PeerId,
  hash: HashFunction,
  peers: Bucket,
  lookups: ReturnType<typeof ArrayMap>,
  infoHash: HashedValue,
  nodesToConnectTo: Item[],
): PeerId[] =>
  nodesToConnectTo.length
    ? lookup(send)(
      id,
      hash,
      peers,
      lookups,
      infoHash,
      mapCat(nextNodesToConnectTo(send, infoHash, id, hash, peers, lookups))(
        nodesToConnectTo,
      ),
    )
    : EFFECT_finishLookup(lookups, peers, infoHash);

type Send = (
  recipient: PeerId,
  source: PeerId,
  command: string,
  args: any,
) => any;

const put = (send: Send) => (dht: DHT, data: any): HashedValue => {
  const infoHash = sha1(data);
  dht.data.set(infoHash, data);
  lookup(send)(
    dht.id,
    dht.hash,
    dht.peers,
    dht.lookups,
    infoHash,
    EFFECT_startLookup(dht, infoHash),
  ).forEach((
    element,
  ) => send(element, dht.id, "put", data));
  return infoHash;
};

const get = (send: Send) => (dht: DHT, infoHash: HashedValue) => {
  if (dht.data.has(infoHash)) return dht.data.get(infoHash);
  for (
    const element of lookup(send)(
      dht.id,
      dht.hash,
      dht.peers,
      dht.lookups,
      infoHash,
      EFFECT_startLookup(dht, infoHash),
    )
  ) {
    const data = send(element, dht.id, "get", infoHash);
    if (data) return data;
  }
  return null;
};

const response = (
  instance: DHT,
  source_id: PeerId,
  command: string,
  data: any,
): any => {
  switch (command) {
    case "bootstrap":
      return EFFECT_setPeer(instance, source_id, data[0], data[1], data[2])
        ? (EFFECT_commitState(instance.stateCache, instance.latestState),
          getState(instance, null))
        : null;
    case "get":
      return instance.data.get(data) || null;
    case "put":
      instance.data.set(sha1(data), data);
      break;
    case "get_state_proof":
      return getStateProof(
        instance.latestState,
        instance.stateCache,
        instance.id,
        instance.hash,
        data[1],
        data[0],
      );
    case "get_state": {
      return getState(instance, data).slice(1);
    }
    case "put_state":
      EFFECT_setPeer(instance, source_id, data[0], data[1], data[2]);
  }
};

Deno.test("es-dht", () => {
  const idToPeer = ArrayMap();
  const send = (target: PeerId, source: PeerId, command: string, data: any[]) =>
    response(idToPeer.get(target), source, command, data);
  const bootsrapPeer = crypto.randomBytes(20);
  idToPeer.set(bootsrapPeer, makeSimpleDHT(bootsrapPeer));
  const nodes = range(100).map(() => {
    const id = crypto.randomBytes(20);
    const x = makeSimpleDHT(id);
    idToPeer.set(id, x);
    const state = send(
      bootsrapPeer,
      x.id,
      "bootstrap",
      getState(x, null),
    );
    EFFECT_commitState(x.stateCache, x.latestState);
    if (state) {
      const [stateVersion, proof, peers] = state;
      EFFECT_setPeer(x, bootsrapPeer, stateVersion, proof, peers);
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
  const infoHash = crypto.randomBytes(20);
  const lookupNodes = lookup(send)(
    alice.id,
    alice.hash,
    alice.peers,
    alice.lookups,
    infoHash,
    EFFECT_startLookup(alice, infoHash),
  );
  assert(lookupNodes);
  assert(lookupNodes.length >= 2 && lookupNodes.length <= 20);
  assertInstanceOf(lookupNodes[0], Uint8Array);
  assertEquals(lookupNodes[0].length, 20);
  EFFECT_deletePeer(alice, last(getState(alice, null)[2]));
});
