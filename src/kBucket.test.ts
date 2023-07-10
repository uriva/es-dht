import { bucketAdd, bucketElements, kBucket } from "./kBucket.ts";

import { assertEquals } from "https://deno.land/std@0.193.0/testing/asserts.ts";
import { randomBytesArray } from "./utils.ts";

const arraySize = 2;
const k = 20;

Deno.test("adding an element places it in root node", () => {
  const element = randomBytesArray(arraySize);
  assertEquals(
    bucketElements(
      bucketAdd(kBucket(randomBytesArray(arraySize), k), element),
    ),
    [element],
  );
});

Deno.test(
  "adding an existing element does not change",
  () => {
    const element = randomBytesArray(arraySize);
    assertEquals(
      bucketElements(
        bucketAdd(
          bucketAdd(kBucket(randomBytesArray(arraySize), k), element),
          element,
        ),
      ),
      [element],
    );
  },
);

Deno.test(
  "adding contact to bucket that can't be split results in no change",
  () => {
    let bucket = kBucket(new Uint8Array([0x00, 0x00]), k);
    for (let j = 0; j < k + 1; ++j) {
      bucket = bucketAdd(bucket, new Uint8Array([0x80, j]));
    }
    assertEquals(bucketElements(bucket).length, k);
  },
);
