import { Map as ImmutableMap, Set as ImmutableSet } from "npm:immutable";

const arrayToKey = (arr: Uint8Array) => arr.join(",");
const keyToArray = (key: string) => new Uint8Array(key.split(",").map(Number));
export type ArrayMap<V> = ImmutableMap<string, V>;

export const arrayMapGet = <V>(
  map: ArrayMap<V>,
  key: Uint8Array,
): V => {
  const value = map.get(arrayToKey(key));
  if (value === undefined) throw "key no found";
  return value;
};

export type ArraySet = ImmutableSet<string>;

export const arraySetHas = (set: ArraySet, arr: Uint8Array) =>
  set.has(arrayToKey(arr));
export const arraySetAdd = (set: ArraySet, arr: Uint8Array) =>
  set.add(arrayToKey(arr));
export const arraySetRemove = (set: ArraySet, arr: Uint8Array) =>
  set.remove(arrayToKey(arr));

export const arrayMapHas = <V>(
  map: ArrayMap<V>,
  key: Uint8Array,
): boolean => map.get(arrayToKey(key)) !== undefined;

export const arrayMapSet = <V>(
  map: ArrayMap<V>,
  key: Uint8Array,
  value: V,
): ArrayMap<V> => map.set(arrayToKey(key), value);

export const arrayMapRemove = <V>(
  mapping: ArrayMap<V>,
  key: Uint8Array,
) => mapping.remove(arrayToKey(key));

export const makeArrayMap = <V>(x: [Uint8Array, V][]) =>
  ImmutableMap<string, V>(x.map(([k, v]) => [arrayToKey(k), v]));

export const makeArraySet = (x: Uint8Array[]) =>
  ImmutableSet<string>(x.map(arrayToKey));

export const arrayMapEntries = <V>(mapping: ArrayMap<V>): [Uint8Array, V][] => {
  const result: [Uint8Array, V][] = [];
  mapping.forEach((value: V, key: string) => {
    result.push([keyToArray(key), value]);
  });
  return result;
};

export const arrayMapKeys = <V>(mapping: ArrayMap<V>) =>
  arrayMapEntries(mapping).map(([key]) => key);
export const arrayMapValues = <V>(mapping: ArrayMap<V>) =>
  arrayMapEntries(mapping).map(([, value]) => value);

export const arraySetFilter = (
  set: ArraySet,
  logic: (x: Uint8Array) => boolean,
) => set.filter((x) => logic(keyToArray(x)));

export const arraySetElements = (set: ArraySet) =>
  Array.from(set).map(keyToArray);

export const arrayMapSize = <V>(x: ArrayMap<V>) => x.size;
