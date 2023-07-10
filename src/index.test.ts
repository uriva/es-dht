import {
  ArrayMap,
  makeMap,
  mapGetArrayImmutable,
  mapHasArrayImmutable,
  mapRemoveArrayImmutable,
  mapSetArrayImmutable,
  setHasArrayImmutable,
} from "./containers.ts";
import { Bucket, bucketHas, closest } from "./kBucket.ts";
import {
  DHT,
  EFFECT_updateLookup,
  HashedValue,
  Item,
  LookupValue,
  PeerId,
  StateVersion,
  checkStateProof,
  commitState,
  deletePeer,
  getState,
  getStateProof,
  makeDHT,
  setPeer,
  sha1,
  startLookup,
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
  if (!mapHasArrayImmutable(lookups, target)) return [];
  return EFFECT_updateLookup(
    peers,
    mapGetArrayImmutable(lookups, target),
    id,
    infoHash,
    target,
    targetStateVersion,
    targetNodePeers,
  );
};

// TODO propagate return value change (used to only return PeerId[])
const lookup = (send: Send) =>
(
  d: DHT,
  infoHash: HashedValue,
  nodesToConnectTo: Item[],
): [ArrayMap<LookupValue>, PeerId[]] => {
  const { id, peers, lookups } = d;
  if (nodesToConnectTo.length) {
    return lookup(send)(
      d,
      infoHash,
      mapCat(nextNodesToConnectTo(send, infoHash, id, peers, lookups))(
        nodesToConnectTo,
      ),
    );
  }
  const [bucket, number, alreadyConnected] = mapGetArrayImmutable(
    lookups,
    infoHash,
  );
  const isConnectedDirectly = bucketHas(peers, infoHash) ||
    setHasArrayImmutable(alreadyConnected, infoHash);
  return [
    mapRemoveArrayImmutable(lookups, infoHash),
    isConnectedDirectly ? [infoHash] : closest(bucket, infoHash, number),
  ];
};

type Send = (
  recipient: PeerId,
  source: PeerId,
  command: Command,
  args: any,
) => any;

// todo: propagate change in newDHT
const put = (send: Send) => (dht: DHT, data: any): HashedValue => {
  const infoHash = sha1(data);
  dht.data.set(infoHash, data);
  const [newDht, items] = startLookup(dht, infoHash);
  lookup(send)(newDht, infoHash, items).forEach((element) =>
    send(element, dht.id, "put", data)
  );
  return infoHash;
};

const get = (send: Send) => (dht: DHT, infoHash: HashedValue) => {
  if (dht.data.has(infoHash)) return dht.data.get(infoHash);
  // todo propagate change in newDht
  const [newDht, items] = startLookup(dht, infoHash);
  for (const element of lookup(send)(newDht, infoHash, items)) {
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
  let idToPeer = makeMap<DHT>([]);
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
    let x = makeSimpleDHT(id);
    const state = send(bootsrapPeer, id, "bootstrap", getState(x, null));
    x = commitState(x);
    idToPeer = mapSetArrayImmutable(idToPeer, id, x);
    if (state) {
      const [stateVersion, proof, peers] = state;
      setPeer(x, bootsrapPeer, stateVersion, proof, peers);
    }
    return id;
  });
  const [alice, bob, carol] = range(3).map(() =>
    mapGetArrayImmutable(idToPeer, randomElement(nodes))
  );
  const data = crypto.randomBytes(10);
  const infohash = put(send)(alice, data);
  for (const peer of [alice, bob, carol]) {
    assertEquals(get(send)(peer, infohash), data);
  }
  const infoHash = crypto.randomBytes(20);
  const [newAlice, items] = startLookup(alice, infoHash);
  idToPeer = mapSetArrayImmutable(idToPeer, newAlice.id, newAlice);
  const lookupNodes = lookup(send)(newAlice, infoHash, items);
  assert(lookupNodes);
  assert(lookupNodes.length >= 2 && lookupNodes.length <= 20);
  assertInstanceOf(lookupNodes[0], Uint8Array);
  assertEquals(lookupNodes[0].length, 20);
  deletePeer(newAlice, last(getState(newAlice, null)[2])); // Just check it works fine, do nothing with result.
});
