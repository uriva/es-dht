const arrayMapSet = require("array-map-set");
const crypto = require("crypto");
const lib = require("..");
const test = require("tape");
const ArrayMap = arrayMapSet.ArrayMap;
const sha1 = (data) => crypto.createHash("sha1").update(data).digest();
const random_bytes = crypto.randomBytes;
const instances = ArrayMap();
function Simple_DHT(id, bootstrap_node_id) {
  var state, state_version, proof, peers;
  bootstrap_node_id == null && (bootstrap_node_id = null);
  if (!(this instanceof Simple_DHT)) {
    return new Simple_DHT(id, bootstrap_node_id);
  }
  this._id = id;
  instances.set(id, this);
  this._dht = lib(id, sha1, 20, 1000);
  this._data = ArrayMap();
  if (bootstrap_node_id) {
    state = this._request(
      bootstrap_node_id,
      "bootstrap",
      this._dht.get_state(),
    );
    this._dht.commit_state();
    if (state) {
      (state_version = state[0]), (proof = state[1]), (peers = state[2]);
      this._dht.set_peer(bootstrap_node_id, state_version, proof, peers);
    }
  }
}
Simple_DHT.prototype = {
  lookup: function (id) {
    this._handle_lookup(id, this._dht.start_lookup(id));
    return this._dht.finish_lookup(id);
  },
  _handle_lookup: function (id, nodes_to_connect_to) {
    var nodes_for_next_round,
      i$,
      len$,
      ref$,
      target_node_id,
      parent_node_id,
      parent_state_version,
      proof,
      target_node_state_version,
      target_node_peers;
    if (!nodes_to_connect_to.length) {
      return;
    }
    nodes_for_next_round = [];
    for (i$ = 0, len$ = nodes_to_connect_to.length; i$ < len$; ++i$) {
      (ref$ = nodes_to_connect_to[i$]),
        (target_node_id = ref$[0]),
        (parent_node_id = ref$[1]),
        (parent_state_version = ref$[2]);
      proof = this._request(parent_node_id, "get_state_proof", [
        target_node_id,
        parent_state_version,
      ]);
      target_node_state_version = this._dht.check_state_proof(
        parent_state_version,
        proof,
        target_node_id,
      );
      if (target_node_state_version) {
        (ref$ = this._request(
          target_node_id,
          "get_state",
          target_node_state_version,
        )),
          (proof = ref$[0]),
          (target_node_peers = ref$[1]);
        if (
          (typeof (ref$ = this._dht.check_state_proof(
            target_node_state_version,
            proof,
            target_node_id,
          )).join == "function"
            ? ref$.join(",")
            : void 8) === target_node_id.join(",")
        ) {
          nodes_for_next_round = nodes_for_next_round.concat(
            this._dht.update_lookup(
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
    this._handle_lookup(id, nodes_for_next_round);
  },
  put: function (data) {
    var infohash, i$, ref$, len$, node;
    infohash = sha1(data);
    this._data.set(infohash, data);
    for (
      i$ = 0, len$ = (ref$ = this.lookup(infohash)).length;
      i$ < len$;
      ++i$
    ) {
      node = ref$[i$];
      this._request(node, "put", data);
    }
    return infohash;
  },
  get: function (infohash) {
    var i$, ref$, len$, node, data;
    if (this._data.has(infohash)) {
      return this._data.get(infohash);
    } else {
      for (
        i$ = 0, len$ = (ref$ = this.lookup(infohash)).length;
        i$ < len$;
        ++i$
      ) {
        node = ref$[i$];
        data = this._request(node, "get", infohash);
        if (data) {
          return data;
        }
      }
      return null;
    }
  },
  get_peers: function () {
    return this._dht.get_state()[2];
  },
  del_peer: function (peer_id) {
    this._dht.del_peer(peer_id);
  },
  destroy: function () {
    clearInterval(this._interval);
  },
  _request: function (target_id, command, data) {
    return instances.get(target_id)._response(this._id, command, data);
  },
  _response: function (source_id, command, data) {
    var state_version, proof, peers, infohash, peer_id;
    switch (command) {
      case "bootstrap":
        (state_version = data[0]), (proof = data[1]), (peers = data[2]);
        return this._dht.set_peer(source_id, state_version, proof, peers)
          ? (this._dht.commit_state(), this._dht.get_state())
          : null;
      case "get":
        return this._data.get(data) || null;
      case "put":
        infohash = sha1(data);
        this._data.set(infohash, data);
        break;
      case "get_state_proof":
        (peer_id = data[0]), (state_version = data[1]);
        return this._dht.get_state_proof(state_version, peer_id);
      case "get_state":
        return this._dht.get_state(data).slice(1);
      case "put_state":
        (state_version = data[0]), (proof = data[1]), (peers = data[2]);
        this._dht.set_peer(source_id, state_version, proof, peers);
    }
  },
};

test("es-dht", function (t) {
  var nodes,
    bootstrap_node_id,
    i$,
    _,
    id,
    node_a,
    node_b,
    node_c,
    data,
    infohash,
    lookup_nodes;
  t.plan(8);
  console.log("Creating instances...");
  nodes = [];
  bootstrap_node_id = random_bytes(20);
  Simple_DHT(bootstrap_node_id);
  for (i$ = 0; i$ < 100; ++i$) {
    _ = i$;
    id = random_bytes(20);
    nodes.push(id);
    Simple_DHT(id, bootstrap_node_id);
  }
  console.log("Warm-up...");
  node_a = instances.get(nodes[Math.floor(nodes.length * Math.random())]);
  node_b = instances.get(nodes[Math.floor(nodes.length * Math.random())]);
  node_c = instances.get(nodes[Math.floor(nodes.length * Math.random())]);
  data = random_bytes(10);
  infohash = node_a.put(data);
  t.ok(infohash, "put succeeded");
  t.equal(node_a.get(infohash), data, "get on node a succeeded");
  t.equal(node_b.get(infohash), data, "get on node b succeeded");
  t.equal(node_c.get(infohash), data, "get on node c succeeded");
  lookup_nodes = node_a.lookup(random_bytes(20));
  t.ok(
    lookup_nodes.length >= 2 && lookup_nodes.length <= 20,
    "Found at most 20 nodes on random lookup, but not less than 2",
  );
  t.ok(lookup_nodes[0] instanceof Uint8Array, "Node has correct ID type");
  t.equal(lookup_nodes[0].length, 20, "Node has correct ID length");
  t.doesNotThrow(function () {
    var ref$;
    node_a.del_peer((ref$ = node_a.get_peers())[ref$.length - 1]);
  }, "Peer deletion works fine");
  instances.forEach(function (instance) {
    instance.destroy();
  });
});
