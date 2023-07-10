const randomInteger = (n: number) => Math.floor(n * Math.random());

export const choice = <T>(arr: T[]): T => arr[randomInteger(arr.length)];

export const range = (n: number) => {
  const result = [];
  while (result.length != n) result.push(result.length);
  return result;
};

export const mapCat = <T, X>(f: (_: T) => X[]) => (arr: T[]): X[] => {
  const result = [];
  for (const element of arr) {
    for (const resultElement of f(element)) {
      result.push(resultElement);
    }
  }
  return result;
};

export const uint8ArraysEqual = (x: Uint8Array, y: Uint8Array): boolean => {
  if (x === y) return true;
  if (x.length !== y.length) return false;
  for (let i = 0; i < x.length; ++i) {
    if (x[i] !== y[i]) return false;
  }
  return true;
};

export const concatUint8Array = (x: Uint8Array, y: Uint8Array): Uint8Array => {
  const result = new Uint8Array(x.length * 2);
  result.set(x);
  result.set(y, x.length);
  return result;
};

export const last = <T>(arr: T[]) => arr[arr.length - 1];

export const randomBytesArray = (n: number) =>
  crypto.getRandomValues(new Uint8Array(n));
