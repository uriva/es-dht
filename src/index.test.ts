import * as crypto from "https://deno.land/std@0.177.0/node/crypto.ts";

import {
  ArrayMap,
  Bucket,
  HashFunction,
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
  makeDHT,
  setPeer,
  startLookup,
  updateLookup,
} from "../src/index.ts";
import {
  assert,
  assertEquals,
  assertInstanceOf,
} from "https://deno.land/std@0.178.0/testing/asserts.ts";
import { last, mapCat, randomElement, range } from "./utils.ts";

const sha1 = (data: any): HashedValue =>
  crypto.createHash("sha1").update(data).digest();

const makeSimpleDHT = (id: PeerId) => ({
  dht: makeDHT(id, sha1, 20, 1000, 0.2),
  data: ArrayMap(),
});

type SimpleDHT = ReturnType<typeof makeSimpleDHT>;

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
    id,
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
  const checkResult = checkStateProof(
    hash,
    id,
    targetStateVersion,
    proof,
    target,
  );
  if (checkResult && checkResult.join(",") === target.join(",")) {
    return updateLookup(
      peers,
      lookups,
      id,
      infoHash,
      target,
      targetStateVersion,
      targetNodePeers,
    );
  }
  throw new Error();
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
    : finishLookup(lookups, peers, infoHash);

type Send = (
  recipient: PeerId,
  source: PeerId,
  command: string,
  args: any,
) => any;

const put = (send: Send) => (dht: SimpleDHT, data: any): HashedValue => {
  const infoHash = sha1(data);
  dht.data.set(infoHash, data);
  lookup(send)(
    dht.dht.id,
    dht.dht.hash,
    dht.dht.peers,
    dht.dht.lookups,
    infoHash,
    startLookup(dht.dht, infoHash),
  ).forEach((
    element,
  ) => send(element, dht.dht.id, "put", data));
  return infoHash;
};

const get = (send: Send) => (dht: SimpleDHT, infoHash: HashedValue) => {
  if (dht.data.has(infoHash)) return dht.data.get(infoHash);
  for (
    const element of lookup(send)(
      dht.dht.id,
      dht.dht.hash,
      dht.dht.peers,
      dht.dht.lookups,
      infoHash,
      startLookup(dht.dht, infoHash),
    )
  ) {
    const data = send(element, dht.dht.id, "get", infoHash);
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
        ? (commitState(instance.dht.stateCache, instance.dht.latestState),
          getState(instance.dht, null))
        : null;
    case "get":
      return instance.data.get(data) || null;
    case "put":
      instance.data.set(sha1(data), data);
      break;
    case "get_state_proof":
      return getStateProof(
        instance.dht.latestState,
        instance.dht.stateCache,
        instance.dht.id,
        instance.dht.hash,
        data[1],
        data[0],
      );
    case "get_state": {
      return getState(instance.dht, data).slice(1);
    }
    case "put_state":
      setPeer(instance.dht, source_id, data[0], data[1], data[2]);
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
      x.dht.id,
      "bootstrap",
      getState(x.dht, null),
    );
    commitState(x.dht.stateCache, x.dht.latestState);
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
  const infoHash = crypto.randomBytes(20);
  const lookupNodes = lookup(send)(
    alice.dht.id,
    alice.dht.hash,
    alice.dht.peers,
    alice.dht.lookups,
    infoHash,
    startLookup(alice.dht, infoHash),
  );
  assert(lookupNodes);
  assert(lookupNodes.length >= 2 && lookupNodes.length <= 20);
  assertInstanceOf(lookupNodes[0], Uint8Array);
  assertEquals(lookupNodes[0].length, 20);
  deletePeer(alice.dht, last(getState(alice.dht, null)[2]));
});
