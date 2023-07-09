import { addToBucket, bucketElements, kBucket } from "./kBucket.ts";

import { assertEquals } from "https://deno.land/std@0.193.0/testing/asserts.ts";

Deno.test(
  "adding contact to bucket that can't be split results in no change",
  () => {
    const size = 20;
    let bucket = kBucket(new Uint8Array([0x00, 0x00]), size);
    for (let j = 0; j < size + 1; ++j) {
      bucket = addToBucket(bucket, new Uint8Array([0x80, j]));
    }
    assertEquals(bucketElements(bucket).length, size);
  },
);
