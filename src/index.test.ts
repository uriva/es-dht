import { ArrayMap, makeMap, mapGetArrayImmutable } from "./containers.ts";
import {
  DHT,
  EFFECT_commitState,
  EFFECT_finishLookup,
  EFFECT_startLookup,
  EFFECT_updateLookup,
  HashedValue,
  Item,
  LookupValue,
  PeerId,
  StateVersion,
  checkStateProof,
  deletePeer,
  getState,
  getStateProof,
  makeDHT,
  setPeer,
  sha1,
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

import { Bucket } from "./kBucket.ts";

const makeSimpleDHT = (id: PeerId) => makeDHT(id, 20, 1000, 0.2);

const nextNodesToConnectTo = (
  send: Send,
  infoHash: HashedValue,
  id: PeerId,
  peers: Bucket,
  lookups: ArrayMap<LookupValue>,
) =>
(
  [target, parentId, parentStateVersion]: [PeerId, PeerId, StateVersion],
) => {
  const targetStateVersion = checkStateProof(
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
  const checkResult = checkStateProof(targetStateVersion, proof, target);
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
  peers: Bucket,
  lookups: ArrayMap<LookupValue>,
  infoHash: HashedValue,
  nodesToConnectTo: Item[],
): PeerId[] =>
  nodesToConnectTo.length
    ? lookup(send)(
      id,
      peers,
      lookups,
      infoHash,
      mapCat(nextNodesToConnectTo(send, infoHash, id, peers, lookups))(
        nodesToConnectTo,
      ),
    )
    : EFFECT_finishLookup(lookups, peers, infoHash);

type Send = (
  recipient: PeerId,
  source: PeerId,
  command: Command,
  args: any,
) => any;

const put = (send: Send) => (dht: DHT, data: any): HashedValue => {
  const infoHash = sha1(data);
  dht.data.set(infoHash, data);
  lookup(send)(
    dht.id,
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

type Command =
  | "bootstrap"
  | "get"
  | "put"
  | "get_state_proof"
  | "get_state"
  | "put_state";

const response = (
  instance: DHT,
  source_id: PeerId,
  command: Command,
  data: any,
): any => {
  if (command === "bootstrap") {
    return setPeer(instance, source_id, data[0], data[1], data[2])
      ? (EFFECT_commitState(
        instance.cacheHistorySize,
        instance.stateCache,
        instance.latestState,
      ),
        getState(instance, null))
      : null;
  }
  if (command === "get") return instance.data.get(data) || null;
  if (command === "put") {
    instance.data.set(sha1(data), data);
  }
  if (command === "get_state_proof") {
    return getStateProof(
      instance.latestState,
      instance.stateCache,
      instance.id,
      data[1],
      data[0],
    );
  }
  if (command === "get_state") return getState(instance, data).slice(1);
  if (command === "put_state") {
    setPeer(instance, source_id, data[0], data[1], data[2]);
  }
};

Deno.test("es-dht", () => {
  const idToPeer = makeMap<DHT>([]);
  const send = (
    target: PeerId,
    source: PeerId,
    command: Command,
    data: any[],
  ) => response(mapGetArrayImmutable(idToPeer, target), source, command, data);
  const bootsrapPeer = crypto.randomBytes(20);
  idToPeer.set(bootsrapPeer, makeSimpleDHT(bootsrapPeer));
  const nodes = range(100).map(() => {
    const id = crypto.randomBytes(20);
    const x = makeSimpleDHT(id);
    idToPeer.set(id, x);
    const state = send(bootsrapPeer, id, "bootstrap", getState(x, null));
    EFFECT_commitState(x.cacheHistorySize, x.stateCache, x.latestState);
    if (state) {
      const [stateVersion, proof, peers] = state;
      setPeer(x, bootsrapPeer, stateVersion, proof, peers);
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
  deletePeer(alice, last(getState(alice, null)[2]));
});
