import * as crypto from "https://deno.land/std@0.177.0/node/crypto.ts";

import { DHT } from "../src/index.ts";
import arrayMapSet from "npm:array-map-set@^1.0.1";
import test from "npm:tape@^4.9.1";

const sha1 = (data) => crypto.createHash("sha1").update(data).digest();

const makeSimpleDHT = (id: ReturnType<crypto.randomBytes>) => ({
  id,
  dht: DHT(id, sha1, 20, 1000),
  data: arrayMapSet.ArrayMap(),
});

type SimpleDHT = ReturnType<typeof makeSimpleDHT>;

const lookup = (send) => (dht: SimpleDHT, id) => {
  _handleLookup(send)(dht, id, dht.dht.start_lookup(id));
  return dht.dht.finish_lookup(id);
};
const _handleLookup = (send) => (dht, id, nodes_to_connect_to) => {
  if (!nodes_to_connect_to.length) {
    return;
  }
  let nodes_for_next_round = [];
  for (let i = 0; i < nodes_to_connect_to.length; ++i) {
    const [target_node_id, parent_node_id, parent_state_version] =
      nodes_to_connect_to[i];
    const target_node_state_version = dht.dht.check_state_proof(
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
      if (
        dht.dht
          .check_state_proof(target_node_state_version, proof, target_node_id)
          .join(",") === target_node_id.join(",")
      ) {
        nodes_for_next_round = nodes_for_next_round.concat(
          dht.dht.update_lookup(
            id,
            target_node_id,
            target_node_state_version,
            target_node_peers,
          ),
        );
      } else {
        throw new Error();
      }
    } else {
      throw new Error();
    }
  }
  _handleLookup(send)(dht, id, nodes_for_next_round);
};

const put = (send) => (dht, data) => {
  const infohash = sha1(data);
  dht.data.set(infohash, data);
  const ref = lookup(send)(dht, infohash);
  for (let i = 0; i < ref.length; ++i) {
    send(ref[i], dht.id, "put", data);
  }
  return infohash;
};

const get = (send) => (dht, infohash) => {
  if (dht.data.has(infohash)) {
    return dht.data.get(infohash);
  }
  const ref = lookup(send)(dht, infohash);
  for (let i = 0; i < ref.length; ++i) {
    const data = send(ref[i], dht.id, "get", infohash);
    if (data) {
      return data;
    }
  }
  return null;
};

const getPeers = (dht) => dht.dht.get_state()[2];

const deletePeer = (dht, peer_id) => {
  dht.dht.del_peer(peer_id);
};

const response = (instance, source_id, command, data) => {
  switch (command) {
    case "bootstrap":
      return instance.dht.set_peer(source_id, data[0], data[1], data[2])
        ? (instance.dht.commit_state(), instance.dht.get_state())
        : null;
    case "get":
      return instance.data.get(data) || null;
    case "put":
      instance.data.set(sha1(data), data);
      break;
    case "get_state_proof":
      return instance.dht.get_state_proof(data[1], data[0]);
    case "get_state":
      return instance.dht.get_state(data).slice(1);
    case "put_state":
      instance.dht.set_peer(source_id, data[0], data[1], data[2]);
  }
};

test("es-dht", (t) => {
  t.plan(8);
  console.log("Creating instances...");
  const instances = arrayMapSet.ArrayMap();

  const send = (recipient, source_id, command, data) =>
    response(instances.get(recipient), source_id, command, data);

  const nodes: ReturnType<crypto.randomBytes>[] = [];
  const bootsrapNodeId = crypto.randomBytes(20);
  const initial = makeSimpleDHT(bootsrapNodeId);
  instances.set(bootsrapNodeId, initial);
  for (let i = 0; i < 100; ++i) {
    const id = crypto.randomBytes(20);
    nodes.push(id);
    const x = makeSimpleDHT(id);
    instances.set(id, x);
    const state = send(bootsrapNodeId, x.id, "bootstrap", x.dht.get_state());
    x.dht.commit_state();
    if (state) {
      const [stateVersion, proof, peers] = state;
      x.dht.set_peer(bootsrapNodeId, stateVersion, proof, peers);
    }
  }
  console.log("Warm-up...");
  const alice = instances.get(nodes[Math.floor(nodes.length * Math.random())]);
  const bob = instances.get(nodes[Math.floor(nodes.length * Math.random())]);
  const carol = instances.get(nodes[Math.floor(nodes.length * Math.random())]);
  const data = crypto.randomBytes(10);
  const infohash = put(send)(alice, data);
  t.ok(infohash, "put succeeded");
  t.equal(get(send)(alice, infohash), data, "get on node a succeeded");
  t.equal(get(send)(bob, infohash), data, "get on node b succeeded");
  t.equal(get(send)(carol, infohash), data, "get on node c succeeded");
  const lookupNodes = lookup(send)(alice, crypto.randomBytes(20));
  t.ok(
    lookupNodes.length >= 2 && lookupNodes.length <= 20,
    "Found at most 20 nodes on random lookup, but not less than 2",
  );
  t.ok(lookupNodes[0] instanceof Uint8Array, "Node has correct ID type");
  t.equal(lookupNodes[0].length, 20, "Node has correct ID length");
  t.doesNotThrow(() => {
    const peers = getPeers(alice);
    deletePeer(alice, peers[peers.length - 1]);
  }, "Peer deletion works fine");
  instances.forEach((instance) => {
    clearInterval(instance._interval);
  });
});
