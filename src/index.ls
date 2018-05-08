/**
 * @package Entangled state DHT
 * @author  Nazar Mokrynskyi <nazar@mokrynskyi.com>
 * @license 0BSD
 */
/*
 * Implements version ? of the specification
 */
function Wrapper (array-map-set, k-bucket-sync, merkle-tree-binary)
	ArrayMap	= array-map-set['ArrayMap']
	/**
	 * @constructor
	 *
	 * @param {number}	size
	 *
	 * @return {!LRU}
	 */
	!function LRU (size)
		if !(@ instanceof LRU)
			return new LRU(size)
		@_size		= size
		@_map		= ArrayMap()
		@_last_key	= null
	LRU:: =
		/**
		 * @param {!Uint8Array}			key
		 * @param {!Array<!Uint8Array>}	value
		 */
		add : (key, value) !->
			if @_map.has(key)
				return
			@_map.set(key, value)
			@_last_key	= key
			if @_map.size > @_size
				@_map.delete(@_map.keys().next().value)
		/**
		 * @param {!Uint8Array}	key
		 *
		 * @return {!Array<!Uint8Array>}
		 */
		get : (key) ->
			@_map.get(key)
		/**
		 * @param {!Uint8Array}	key
		 */
		del : (key) !->
			@_map.delete(key)
			if !@_map.has(@_last_key)
				@_last_key = Array.from(@_map.keys())[* - 1] || null
		/**
		 * @return {Uint8Array} `null` if there are no items
		 */
		last_key : ->
			@_last_key
	/**
	 * @constructor
	 *
	 * @param {!Uint8Array}	id					Own ID
	 * @param {!Function}	hash_function		Hash function to be used for Merkle Tree
	 * @param {number}		bucket_size			Size of a bucket from Kademlia design
	 * @param {number}		state_history_size	How many versions of local history will be kept
	 *
	 * @return {!DHT}
	 */
	!function DHT (id, hash_function, bucket_size, state_history_size)
		if !(@ instanceof DHT)
			return new DHT(id, hash_function, bucket_size, state_history_size)

		@_id	= id
		@_hash	= hash_function
		@_state	= LRU(state_history_size)
		@_insert_state(new Map)
		# TODO: More stuff here

	DHT:: =
		/**
		 * @param {!Uint8Array} peer_id			Id of a peer
		 * @param {!Uint8Array} state_version	State version of a peer
		 */
		'set_peer' : (peer_id, state_version) !->
			state	= @'get_state'()[1]
			state.set(peer_id, state_version)
			@_insert_state(state)
		/**
		 * @param {!Uint8Array} peer_id Id of a peer
		 */
		'del_peer' : (peer_id) !->
			state	= @'get_state'()[1]
			if !state.has(peer_id)
				return
			state.delete(peer_id)
			@_insert_state(state)
		/**
		 * @param {Uint8Array=} state_version	Specific state version or latest if `null`
		 *
		 * @return {!Array} `[state_version, state]`, where `state_version` is a Merkle Tree root of the state and `state` is a `Map` with peers as keys and their state versions as values
		 */
		'get_state' : (state_version = null) ->
			state_version	= state_version || @_state.last_key()
			[state_version, ArrayMap(Array.from(@_state.get(version)))]
		/**
		 * Generate proof about peer in current state version
		 *
		 * @param {!Uint8Array} state_version	Specific state version
		 * @param {!Uint8Array} peer_id			ID of peer for which to create a proof
		 *
		 * @return {!Uint8Array}
		 */
		'get_state_proof' : (state_version, peer_id) ->
			state	= @_state.get(version)
			if !state || !state.has(peer_id)
				new Uint8Array(0)
			else
				items	= [].concat(...Array.from(new_state), @_id)
				proof	= merkle-tree-binary['get_proof'](items, peer_id, @_hash)
		/**
		 * Generate proof about peer in current state version
		 *
		 * @param {!Uint8Array} peer_id			ID of peer that created proof
		 * @param {!Uint8Array} proof			Proof itself
		 * @param {!Uint8Array} target_peer_id	ID of peer's peer for which proof was generated
		 *
		 * @return {Uint8Array} `state_version` of `target_peer_id` on success or `null` otherwise
		 */
		'check_state_proof' : (peer_id, proof, target_peer_id) ->
			state			= @'get_state'()[1]
			state_version	= state.get(peer_id)
			# Correct proof will always start from `0` followed by state version, since peer ID and its state version are placed one after another in Merkle Tree
			if proof[0] == 0 && merkle-tree-binary['check_proof'](state_version, proof, target_peer_id, @_hash)
				proof.subarray(1, peer_id.length + 1)
			else
				null
		/**
		 * @param {!Map}	new_state
		 */
		_insert_state : (new_state) !->
			items			= [].concat(...Array.from(new_state), @_id)
			state_version	= merkle-tree-binary['get_root'](items, @_hash)
			@_state.add(state_version, new_state)
		# TODO: Many more methods

	Object.defineProperty(DHT::, 'constructor', {value: DHT})

	DHT

if typeof define == 'function' && define['amd']
	# AMD
	define(['array-map-set', 'k-bucket-sync', 'merkle-tree-binary'], Wrapper)
else if typeof exports == 'object'
	# CommonJS
	module.exports = Wrapper(require('array-map-set'), require('k-bucket-sync'), require('merkle-tree-binary'))
else
	# Browser globals
	@'detox_transport' = Wrapper(@'array_map_set', @'k_bucket_sync', @'merkle_tree_binary')
