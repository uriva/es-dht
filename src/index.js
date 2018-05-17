// Generated by LiveScript 1.5.0
/**
 * @package Entangled state DHT
 * @author  Nazar Mokrynskyi <nazar@mokrynskyi.com>
 * @license 0BSD
 */
(function(){
  /*
   * Implements version 0.1.1 of the specification
   */
  /**
   * @param {!Uint8Array}	array1
   * @param {!Uint8Array}	array2
   *
   * @return {boolean}
   */
  function are_arrays_equal(array1, array2){
    var i$, len$, key, item;
    if (array1 === array2) {
      return true;
    }
    if (array1.length !== array2.length) {
      return false;
    }
    for (i$ = 0, len$ = array1.length; i$ < len$; ++i$) {
      key = i$;
      item = array1[i$];
      if (item !== array2[key]) {
        return false;
      }
    }
    return true;
  }
  /**
   * @param {!Uint8Array} array1
   * @param {!Uint8Array} array2
   *
   * @return {!Uint8Array}
   */
  function concat(array1, array2){
    var length, x$;
    length = array1.length;
    x$ = new Uint8Array(length * 2);
    x$.set(array1);
    x$.set(array2, length);
    return x$;
  }
  function Wrapper(arrayMapSet, kBucketSync, merkleTreeBinary){
    var ArrayMap, ArraySet;
    ArrayMap = arrayMapSet['ArrayMap'];
    ArraySet = arrayMapSet['ArraySet'];
    /**
     * @constructor
     *
     * @param {number}	size
     *
     * @return {!State_cache}
     */
    function State_cache(size){
      if (!(this instanceof State_cache)) {
        return new State_cache(size);
      }
      this._size = size;
      this._map = ArrayMap();
      this._last_key = null;
    }
    State_cache.prototype = {
      /**
       * @param {!Uint8Array}	key
       * @param {!Map}		value
       */
      add: function(key, value){
        if (this._map.has(key)) {
          return;
        }
        this._map.set(key, value);
        this._last_key = key;
        if (this._map.size > this._size) {
          this._map['delete'](this._map.keys().next().value);
        }
      }
      /**
       * @param {!Uint8Array}	key
       *
       * @return {!Map}
       */,
      get: function(key){
        return this._map.get(key);
      }
      /**
       * @param {!Uint8Array}	key
       */,
      del: function(key){
        var ref$;
        this._map['delete'](key);
        if (!this._map.has(this._last_key)) {
          this._last_key = (ref$ = Array.from(this._map.keys()))[ref$.length - 1] || null;
        }
      }
      /**
       * @return {Uint8Array} `null` if there are no items
       */,
      last_key: function(){
        return this._last_key;
      }
    };
    Object.defineProperty(State_cache.prototype, 'constructor', {
      value: State_cache
    });
    /**
     * @constructor
     *
     * @param {!Uint8Array}	id									Own ID
     * @param {!Function}	hash_function						Hash function to be used for Merkle Tree
     * @param {number}		bucket_size							Size of a bucket from Kademlia design
     * @param {number}		state_history_size					How many versions of local history will be kept
     * @param {number}		fraction_of_nodes_from_same_peer	Max fraction of nodes originated from single peer allowed on lookup start
     *
     * @return {!DHT}
     */
    function DHT(id, hash_function, bucket_size, state_history_size, fraction_of_nodes_from_same_peer){
      fraction_of_nodes_from_same_peer == null && (fraction_of_nodes_from_same_peer = 0.2);
      if (!(this instanceof DHT)) {
        return new DHT(id, hash_function, bucket_size, state_history_size, fraction_of_nodes_from_same_peer);
      }
      this._id = id;
      this._id_length = id.length;
      this._hash = hash_function;
      this._bucket_size = bucket_size;
      this._fraction_of_nodes_from_same_peer = fraction_of_nodes_from_same_peer;
      this._state = State_cache(state_history_size);
      this._peers = kBucketSync(this._id, bucket_size);
      this._lookups = ArrayMap();
      this._insert_state(new Map);
    }
    DHT.prototype = {
      /**
       * @param {!Uint8Array}	id		ID if the node being searched for
       * @param {number=}		number	Number of nodes to be returned if exact match was not found, defaults to bucket size
       *
       * @return {!Array<!Array<!Uint8Array>>} Array of items, each item is an array of `Uint8Array`s `[node_id, parent_peer_id, parent_peer_state_version]`
       */
      'start_lookup': function(id, number){
        var bucket, parents, state, max_fraction, current_number, closest_so_far, closest_nodes_found, max_count_allowed, nodes_to_connect_to, connections_awaiting, originated_from, retry, i$, len$, closest_node_id, parent_peer_id, count, parent_peer_state_version;
        number == null && (number = this._bucket_size);
        if (this._peers['has'](id)) {
          return [];
        }
        bucket = kBucketSync(id, number);
        parents = ArrayMap();
        state = this._get_state();
        state.forEach(function(arg$, peer_id){
          var state_version, peer_peers, i$, len$, peer_peer_id;
          state_version = arg$[0], peer_peers = arg$[1];
          bucket.set(peer_id);
          for (i$ = 0, len$ = peer_peers.length; i$ < len$; ++i$) {
            peer_peer_id = peer_peers[i$];
            if (!parents.has(peer_peer_id) && bucket.set(peer_peer_id)) {
              parents.set(peer_peer_id, peer_id);
            }
          }
        });
        max_fraction = Math.max(this._fraction_of_nodes_from_same_peer, 1 / this._peers['count']());
        current_number = number;
        for (;;) {
          closest_so_far = bucket['closest'](id, number);
          closest_nodes_found = closest_so_far.length;
          max_count_allowed = Math.ceil(closest_nodes_found * max_fraction);
          nodes_to_connect_to = [];
          connections_awaiting = ArraySet();
          originated_from = ArrayMap();
          retry = false;
          for (i$ = 0, len$ = closest_so_far.length; i$ < len$; ++i$) {
            closest_node_id = closest_so_far[i$];
            parent_peer_id = parents.get(closest_node_id);
            if (parent_peer_id) {
              count = originated_from.get(parent_peer_id) || 0;
              originated_from.set(parent_peer_id, count + 1);
              if (count > max_count_allowed) {
                bucket.del(closest_node_id);
                retry = true;
              } else {
                parent_peer_state_version = state.get(parent_peer_id)[0];
                nodes_to_connect_to.push([closest_node_id, parent_peer_id, parent_peer_state_version]);
                connections_awaiting.add(closest_node_id);
              }
            } else {
              count = originated_from.get(closest_node_id) || 0;
              originated_from.set(closest_node_id, count + 1);
              if (count > max_count_allowed) {
                bucket.del(closest_node_id);
                retry = true;
              }
            }
          }
          if (!retry) {
            break;
          }
        }
        this._lookups.set(id, [connections_awaiting, bucket, number]);
        return nodes_to_connect_to;
      }
      /**
       * @param {!Uint8Array}			id					The same as in `start_lookup()`
       * @param {!Uint8Array}			node_id				As returned by `start_lookup()`
       * @param {!Uint8Array}			node_state_version	Corresponding state version for `node_id`
       * @param {Array<!Uint8Array>}	node_peers			Peers of `node_id` at state `node_state_version` or `null` if connection to `node_id` have failed
       *
       * @return {!Array<!Array<!Uint8Array>>} The same as in `start_lookup()`
       */,
      'update_lookup': function(id, node_id, node_state_version, node_peers){
        var lookup, connections_awaiting, bucket, number, added_nodes, i$, len$, node_peer_id, closest_so_far, nodes_to_connect_to, closest_node_id;
        lookup = this._lookups.get(id);
        if (!lookup) {
          return [];
        }
        connections_awaiting = lookup[0], bucket = lookup[1], number = lookup[2];
        connections_awaiting['delete'](node_id);
        if (!node_peers) {
          bucket.del(node_id);
          return [];
        }
        added_nodes = ArraySet();
        for (i$ = 0, len$ = node_peers.length; i$ < len$; ++i$) {
          node_peer_id = node_peers[i$];
          if (!bucket.has(node_peer_id) && bucket.set(node_peer_id)) {
            added_nodes.add(node_peer_id);
          }
        }
        closest_so_far = bucket['closest'](id, number);
        nodes_to_connect_to = [];
        for (i$ = 0, len$ = closest_so_far.length; i$ < len$; ++i$) {
          closest_node_id = closest_so_far[i$];
          if (added_nodes.has(closest_node_id)) {
            nodes_to_connect_to.push([closest_node_id, node_id, node_state_version]);
            connections_awaiting.add(closest_node_id);
          }
        }
        return nodes_to_connect_to;
      }
      /**
       * @param {!Uint8Array} id The same as in `start_lookup()`
       *
       * @return {Array<!Uint8Array>} `[id]` if node with specified ID was connected directly, an array of closest IDs if exact node wasn't found and `null` otherwise
       */,
      'finish_lookup': function(id){
        var lookup, bucket, number;
        lookup = this._lookups.get(id);
        this._lookups['delete'](id);
        if (this._peers['has'](id)) {
          return [id];
        }
        if (!lookup) {
          return null;
        }
        bucket = lookup[1], number = lookup[2];
        return bucket['closest'](id, number);
      }
      /**
       * @param {!Uint8Array}			peer_id				Id of a peer
       * @param {!Uint8Array}			peer_state_version	State version of a peer
       * @param {!Uint8Array}			proof				Proof for specified state
       * @param {!Array<!Uint8Array>}	peer_peers			Peer's peers that correspond to `state_version`
       *
       * @return {boolean} `false` if proof is not valid, returning `true` only means there was not errors, but peer was not necessarily added to k-bucket
       *                   (use `has_peer()` method if confirmation of addition to k-bucket is needed)
       */,
      'set_peer': function(peer_id, peer_state_version, proof, peer_peers){
        var expected_number_of_items, proof_block_size, expected_proof_height, proof_height, last_block, i$, to$, block, detected_peer_id, state;
        expected_number_of_items = peer_peers.length * 2 + 2;
        proof_block_size = this._id_length + 1;
        expected_proof_height = Math.log2(expected_number_of_items);
        proof_height = proof.length / proof_block_size;
        if (proof_height !== expected_proof_height) {
          if (proof_height !== Math.ceil(expected_proof_height)) {
            return false;
          }
          last_block = peer_id;
          for (i$ = 0, to$ = Math.ceil(Math.pow(Math.log2(expected_number_of_items), 2) - expected_number_of_items) / 2; i$ <= to$; ++i$) {
            block = i$;
            if (proof[block * proof_block_size] !== 0 || !are_arrays_equal(proof.subarray(block * proof_block_size + 1, (block + 1) * proof_block_size), last_block)) {
              return false;
            }
            last_block = this._hash(concat(last_block, last_block));
          }
        }
        detected_peer_id = this['check_state_proof'](peer_state_version, proof, peer_id);
        if (!detected_peer_id || !are_arrays_equal(detected_peer_id, peer_id)) {
          return false;
        }
        if (!this._peers['set'](peer_id)) {
          return true;
        }
        state = this._get_state_copy();
        state.set(peer_id, [peer_state_version, peer_peers]);
        this._insert_state(state);
        return true;
      }
      /**
       * @param {!Uint8Array} node_id
       *
       * @return {boolean} `true` if node is our peer (stored in k-bucket)
       */,
      'has_peer': function(node_id){
        return this._peers['has'](node_id);
      }
      /**
       * @param {!Uint8Array} peer_id Id of a peer
       */,
      'del_peer': function(peer_id){
        var state;
        state = this._get_state_copy();
        if (!state.has(peer_id)) {
          return;
        }
        this._peers['del'](peer_id);
        state['delete'](peer_id);
        this._insert_state(state);
      }
      /**
       * @param {Uint8Array=} state_version	Specific state version or latest if `null`
       *
       * @return {Array} `[state_version, proof, peers]` or `null` if state version not found, where `state_version` is a Merkle Tree root, `proof` is a proof
       *                 that own ID corresponds to `state_version` and `peers` is an array of peers IDs
       */,
      'get_state': function(state_version){
        var state, proof;
        state_version == null && (state_version = null);
        state_version = state_version || this._state.last_key();
        state = this._get_state(state_version);
        if (!state) {
          return null;
        }
        proof = this['get_state_proof'](state_version, this._id);
        return [state_version, proof, Array.from(state.keys())];
      }
      /**
       * @param {Uint8Array=}	state_version	Specific state version or latest if `null`
       *
       * @return {Map} `null` if state is not found
       */,
      _get_state: function(state_version){
        state_version == null && (state_version = null);
        state_version = state_version || this._state.last_key();
        if (!state_version) {
          return null;
        }
        return this._state.get(state_version);
      }
      /**
       * @param {Uint8Array=}	state_version	Specific state version or latest if `null`
       *
       * @return {Map}
       */,
      _get_state_copy: function(state_version){
        var state;
        state_version == null && (state_version = null);
        state = this._get_state(state_version);
        if (!state) {
          return null;
        }
        return ArrayMap(Array.from(state));
      }
      /**
       * Generate proof about peer in current state version
       *
       * @param {!Uint8Array} state_version	Specific state version
       * @param {!Uint8Array} peer_id			ID of peer for which to create a proof
       *
       * @return {!Uint8Array}
       */,
      'get_state_proof': function(state_version, peer_id){
        var state, items;
        state = this._get_state(state_version);
        if (!state || (!state.has(peer_id) && !are_arrays_equal(peer_id, this._id))) {
          return new Uint8Array(0);
        } else {
          items = this._reduce_state_to_proof_items(state);
          return merkleTreeBinary['get_proof'](items, peer_id, this._hash);
        }
      }
      /**
       * @param {!Map} state
       *
       * @return {!Array<!Uint8Array>}
       */,
      _reduce_state_to_proof_items: function(state){
        var items;
        items = [];
        state.forEach(function(arg$, peer_id){
          var peer_state_version;
          peer_state_version = arg$[0];
          items.push(peer_id, peer_state_version);
        });
        items.push(this._id, this._id);
        return items;
      }
      /**
       * Generate proof about peer in current state version
       *
       * @param {!Uint8Array} state_version	State version for which proof was generated
       * @param {!Uint8Array} proof			Proof itself
       * @param {!Uint8Array} node_id			Node ID for which proof was generated
       *
       * @return {Uint8Array} `state_version` of `node_id` on success or `null` otherwise
       */,
      'check_state_proof': function(state_version, proof, node_id){
        if (proof[0] === 0 && merkleTreeBinary['check_proof'](state_version, proof, node_id, this._hash)) {
          return proof.subarray(1, this._id_length + 1);
        } else {
          return null;
        }
      }
      /**
       * @param {!Map} new_state
       */,
      _insert_state: function(new_state){
        var items, state_version;
        items = this._reduce_state_to_proof_items(new_state);
        state_version = merkleTreeBinary['get_root'](items, this._hash);
        this._state.add(state_version, new_state);
      }
    };
    Object.defineProperty(DHT.prototype, 'constructor', {
      value: DHT
    });
    return DHT;
  }
  if (typeof define === 'function' && define['amd']) {
    define(['array-map-set', 'k-bucket-sync', 'merkle-tree-binary'], Wrapper);
  } else if (typeof exports === 'object') {
    module.exports = Wrapper(require('array-map-set'), require('k-bucket-sync'), require('merkle-tree-binary'));
  } else {
    this['es_dht'] = Wrapper(this['array_map_set'], this['k_bucket_sync'], this['merkle_tree_binary']);
  }
}).call(this);
