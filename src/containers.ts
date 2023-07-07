import { hash, Map as ImmutableMap, Set as ImmutableSet } from "npm:immutable";

const arrayToKey = (arr: Uint8Array) => arr.join(",");
const keyToArray = (key: string) => new Uint8Array(key.split(",").map(Number));
export type ArrayMap<V> = ImmutableMap<string, V>;

export const mapGetArrayImmutable = <V>(
  map: ArrayMap<V>,
  key: Uint8Array,
): V => {
  const value = map.get(arrayToKey(key));
  if (value === undefined) throw "key no found";
  return value;
};

export type ArraySet = ImmutableSet<string>;

export const setHasArrayImmutable = (set: ArraySet, arr: Uint8Array) =>
  set.has(arrayToKey(arr));
export const setAddArrayImmutable = (set: ArraySet, arr: Uint8Array) =>
  set.add(arrayToKey(arr));
export const setRemoveArrayImmutable = (set: ArraySet, arr: Uint8Array) =>
  set.remove(arrayToKey(arr));

export const mapHasArrayImmutable = <V>(
  map: ArrayMap<V>,
  key: Uint8Array,
): boolean => map.get(arrayToKey(key)) !== undefined;

export const mapSetArrayImmutable = <V>(
  map: ArrayMap<V>,
  key: Uint8Array,
  value: V,
): ArrayMap<V> => map.set(arrayToKey(key), value);

export const mapRemoveArrayImmutable = <V>(
  mapping: ArrayMap<V>,
  key: Uint8Array,
) => mapping.remove(arrayToKey(key));

export const makeMap = <V>() => ImmutableMap<string, V>();
export const makeSet = ImmutableSet<string>;

export const entries = <V>(mapping: ArrayMap<V>): [Uint8Array, V][] => {
  const result: [Uint8Array, V][] = [];
  mapping.forEach((value: V, key: string) => {
    result.push([keyToArray(key), value]);
  });
  return result;
};

export const keys = <V>(mapping: ArrayMap<V>) =>
  entries(mapping).map(([key]) => key);
export const values = <V>(mapping: ArrayMap<V>) =>
  entries(mapping).map(([, value]) => value);

export const filterSet = (set: ArraySet, logic: (x: Uint8Array) => boolean) =>
  set.filter((x) => logic(keyToArray(x)));

export const setElements = (set: ArraySet) => Array.from(set).map(keyToArray);
