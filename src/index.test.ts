import {
  ArrayMap,
  arrayMapGet,
  arrayMapHas,
  arrayMapRemove,
  arrayMapSet,
  arraySetHas,
  makeArrayMap,
} from "./containers.ts";
import {
  checkStateProof,
  commitState,
  deletePeer,
  DHT,
  getState,
  getStateProof,
  HashedValue,
  Item,
  LookupValue,
  makeDHT,
  PeerId,
  setPeer,
  sha1,
  startLookup,
  Version,
} from "../src/index.ts";
import {
  assert,
  assertEquals,
  assertInstanceOf,
} from "https://deno.land/std@0.178.0/testing/asserts.ts";
import { bucketHas, closest } from "./kBucket.ts";
import {
  choice,
  last,
  mapCat,
  randomBytesArray,
  range,
  uint8ArraysEqual,
} from "./utils.ts";

const makeSimpleDHT = (id: PeerId) => makeDHT(id, 20, 1000, 0.2);

const nextNodesToConnectTo = (infoHash: HashedValue, d: DHT) =>
(
  [target, parentId, parentStateVersion]: [PeerId, PeerId, Version],
) => {
  const { id, peers, lookups } = d;
  const version = checkStateProof(
    parentStateVersion,
    send(parentId, id, "get_state_proof", [target, parentStateVersion]),
    target,
  );
  if (!version) throw new Error();
  const [proof, nodePeers] = send(target, id, "get_state", version);
  const checkResult = checkStateProof(version, proof, target);
  if (!checkResult || !uint8ArraysEqual(checkResult, target)) throw new Error();
  if (!arrayMapHas(lookups, target)) return [];
  const lookup = arrayMapGet(lookups, target);
  const [bucket, number, alreadyConnected] = lookup;
  if (!nodePeers.length) {
    bucket.del(target);
    return [];
  }
  lookup[2] = setAddArrayImmutable(alreadyConnected, target);
  if (bucketHas(peers, infoHash) || bucketHas(bucket, infoHash)) return [];
  const addedNodes = makeSet(
    // todo this must be split into two filters...
    nodePeers.filter((nodePeerId) =>
      !bucketHas(bucket, nodePeerId) && bucket.set(nodePeerId)
    ),
  );
  if (bucketHas(bucket, infoHash)) return [[infoHash, target, version]];
  bucket.del(id);
  return closest(bucket, infoHash, number).filter((peer: PeerId) =>
    arraySetHas(addedNodes, peer)
  ).map((peer: PeerId) => [peer, target, version]);
};

// TODO propagate return value change (used to only return PeerId[])
const lookup = (send: Send) =>
(
  d: DHT,
  infoHash: HashedValue,
  nodesToConnectTo: Item[],
): [ArrayMap<LookupValue>, PeerId[]] => {
  const { peers, lookups } = d;
  if (nodesToConnectTo.length) {
    return lookup(send)(
      d,
      infoHash,
      mapCat(nextNodesToConnectTo(send, infoHash, d))(nodesToConnectTo),
    );
  }
  const [bucket, number, alreadyConnected] = arrayMapGet(lookups, infoHash);
  const isConnectedDirectly = bucketHas(peers, infoHash) ||
    arraySetHas(alreadyConnected, infoHash);
  return [
    arrayMapRemove(lookups, infoHash),
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
const put = (dht: DHT, data: any): [Order[], HashedValue] => {
  const infoHash = sha1(data);
  dht.data.set(infoHash, data);
  const [newDht, items] = startLookup(dht, infoHash);
  return [
    lookup(newDht, infoHash, items).map((
      element,
    ) => [element, dht.id, "put", data]),
    infoHash,
  ];
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
    const result = setPeer(instance, source_id, data[0], data[1], data[2]);
    return (result)
      ? [commitState(result), getState(instance, null)]
      : [instance, null];
  }
  if (command === "get") return instance.data.get(data) || null;
  if (command === "put") {
    instance.data.set(sha1(data), data);
  }
  if (command === "get_state_proof") {
    return getStateProof(instance, data[1], data[0]);
  }
  if (command === "get_state") return getState(instance, data).slice(1);
  if (command === "put_state") {
    setPeer(instance, source_id, data[0], data[1], data[2]);
  }
};

Deno.test("es-dht", () => {
  const bootsrapPeer = randomBytesArray(20);
  let idToPeer = makeArrayMap<DHT>([[
    bootsrapPeer,
    makeSimpleDHT(bootsrapPeer),
  ]]);
  const send = (
    target: PeerId,
    source: PeerId,
    command: Command,
    data: any[],
  ) => response(arrayMapGet(idToPeer, target), source, command, data);
  const nodes = range(100).map(() => {
    const id = randomBytesArray(20);
    let x = makeSimpleDHT(id);
    const state = send(bootsrapPeer, id, "bootstrap", getState(x, null));
    x = commitState(x);
    idToPeer = arrayMapSet(idToPeer, id, x);
    if (state) {
      const [stateVersion, proof, peers] = state;
      setPeer(x, bootsrapPeer, stateVersion, proof, peers);
    }
    return id;
  });
  const [alice, bob, carol] = range(3).map(() =>
    arrayMapGet(idToPeer, choice(nodes))
  );
  const data = randomBytesArray(10);
  const totalOrders: Order[] = [];
  const [orders, infohash] = put(alice, data);
  orders.forEach((x) => totalOrders.push(x));
  while (totalOrders.length) {
    send(totalOrders.pop()).forEach((order: Order) => totalOrders.push(order));
  }
  for (const peer of [alice, bob, carol]) {
    assertEquals(get(send)(peer, infohash), data);
  }
  const infoHash = randomBytesArray(20);
  const [newAlice, items] = startLookup(alice, infoHash);
  idToPeer = arrayMapSet(idToPeer, newAlice.id, newAlice);
  const lookupNodes = lookup(send)(newAlice, infoHash, items);
  assert(lookupNodes);
  assert(lookupNodes.length >= 2 && lookupNodes.length <= 20);
  assertInstanceOf(lookupNodes[0], Uint8Array);
  assertEquals(lookupNodes[0].length, 20);
  deletePeer(newAlice, last(getState(newAlice, null)[2])); // Just check it works fine, do nothing with result.
});
