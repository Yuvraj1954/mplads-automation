import { assertEquals, assertExists } from "https://deno.land/std@0.224.0/assert/mod.ts";
import {
  validateInput,
  findNextDataset,
  computeCheckpoint,
  buildErrorResponse,
  buildCompleteResponse,
  skipEmptyDataset,
  skipBeyondEnd,
  DATASET_ORDER,
  MAX_BATCH_SIZE,
} from "./logic.ts";

// ============================================================
// Input Validation
// ============================================================

Deno.test("validateInput - missing timestamp returns error", () => {
  const result = validateInput({});
  assertEquals(result.ok, false);
  if (!result.ok) {
    assertEquals(result.status, 400);
    assertEquals(result.body.error, "Missing timestamp");
  }
});

Deno.test("validateInput - empty timestamp returns error", () => {
  const result = validateInput({ timestamp: "" });
  assertEquals(result.ok, false);
  if (!result.ok) {
    assertEquals(result.status, 400);
  }
});

Deno.test("validateInput - whitespace timestamp returns error", () => {
  const result = validateInput({ timestamp: "   " });
  assertEquals(result.ok, false);
});

Deno.test("validateInput - valid timestamp with defaults", () => {
  const result = validateInput({
    timestamp: "2026-09-03T17-50-47Z",
  });
  assertEquals(result.ok, true);
  if (result.ok) {
    assertEquals(result.value.timestamp, "2026-09-03T17-50-47Z");
    assertEquals(result.value.dataset, undefined);
    assertEquals(result.value.startPart, 1);
    assertEquals(result.value.batchSize, MAX_BATCH_SIZE);
  }
});

Deno.test("validateInput - explicit dataset and start_part", () => {
  const result = validateInput({
    timestamp: "2026-09-03T17-50-47Z",
    dataset: "works_recommended",
    start_part: 6,
  });
  assertEquals(result.ok, true);
  if (result.ok) {
    assertEquals(result.value.dataset, "works_recommended");
    assertEquals(result.value.startPart, 6);
  }
});

Deno.test("validateInput - batch_size capped at 5", () => {
  const result = validateInput({
    timestamp: "2026-09-03T17-50-47Z",
    batch_size: 100,
  });
  assertEquals(result.ok, true);
  if (result.ok) {
    assertEquals(result.value.batchSize, 5);
  }
});

Deno.test("validateInput - batch_size of 0 defaults to 5", () => {
  const result = validateInput({
    timestamp: "2026-09-03T17-50-47Z",
    batch_size: 0,
  });
  assertEquals(result.ok, true);
  if (result.ok) {
    assertEquals(result.value.batchSize, 5);
  }
});

Deno.test("validateInput - negative batch_size defaults to 5", () => {
  const result = validateInput({
    timestamp: "2026-09-03T17-50-47Z",
    batch_size: -3,
  });
  assertEquals(result.ok, true);
  if (result.ok) {
    assertEquals(result.value.batchSize, 5);
  }
});

Deno.test("validateInput - batch_size of 3 stays 3", () => {
  const result = validateInput({
    timestamp: "2026-09-03T17-50-47Z",
    batch_size: 3,
  });
  assertEquals(result.ok, true);
  if (result.ok) {
    assertEquals(result.value.batchSize, 3);
  }
});

Deno.test("validateInput - start_part floors to integer", () => {
  const result = validateInput({
    timestamp: "2026-09-03T17-50-47Z",
    start_part: 3.7,
  });
  assertEquals(result.ok, true);
  if (result.ok) {
    assertEquals(result.value.startPart, 3);
  }
});

Deno.test("validateInput - start_part of 0 defaults to 1", () => {
  const result = validateInput({
    timestamp: "2026-09-03T17-50-47Z",
    start_part: 0,
  });
  assertEquals(result.ok, true);
  if (result.ok) {
    assertEquals(result.value.startPart, 1);
  }
});

Deno.test("validateInput - negative start_part defaults to 1", () => {
  const result = validateInput({
    timestamp: "2026-09-03T17-50-47Z",
    start_part: -5,
  });
  assertEquals(result.ok, true);
  if (result.ok) {
    assertEquals(result.value.startPart, 1);
  }
});

Deno.test("validateInput - dataset trims whitespace", () => {
  const result = validateInput({
    timestamp: "2026-09-03T17-50-47Z",
    dataset: "  works_recommended  ",
  });
  assertEquals(result.ok, true);
  if (result.ok) {
    assertEquals(result.value.dataset, "works_recommended");
  }
});

Deno.test("validateInput - whitespace-only dataset is undefined", () => {
  const result = validateInput({
    timestamp: "2026-09-03T17-50-47Z",
    dataset: "   ",
  });
  assertEquals(result.ok, true);
  if (result.ok) {
    assertEquals(result.value.dataset, undefined);
  }
});

// ============================================================
// findNextDataset
// ============================================================

Deno.test("findNextDataset - returns requested if in list", () => {
  const result = findNextDataset(
    "works_recommended",
    DATASET_ORDER
  );
  assertEquals(result, "works_recommended");
});

Deno.test("findNextDataset - returns first if requested not in list", () => {
  const result = findNextDataset(
    "nonexistent_dataset",
    DATASET_ORDER
  );
  assertEquals(result, "allocated_limit");
});

Deno.test("findNextDataset - returns first if no dataset requested", () => {
  const result = findNextDataset(undefined, DATASET_ORDER);
  assertEquals(result, "allocated_limit");
});

Deno.test("findNextDataset - returns undefined if discovered is empty", () => {
  const result = findNextDataset(undefined, []);
  assertEquals(result, undefined);
});

Deno.test("findNextDataset - returns undefined if requested not in empty list", () => {
  const result = findNextDataset("works_recommended", []);
  assertEquals(result, undefined);
});

Deno.test("findNextDataset - returns last dataset if requested", () => {
  const result = findNextDataset(
    "mla_calamity",
    DATASET_ORDER
  );
  assertEquals(result, "mla_calamity");
});

// ============================================================
// computeCheckpoint — normal batch (no dataset exhaustion)
// ============================================================

Deno.test("computeCheckpoint - normal batch, dataset not exhausted", () => {
  const resp = computeCheckpoint({
    currentDataset: "works_recommended",
    allPartsCount: 20,
    startPart: 1,
    processedParts: [1, 2, 3, 4, 5],
    recordsProcessed: 10000,
    batchSize: 5,
    discovered: DATASET_ORDER,
    timestamp: "2026-09-03T17-50-47Z",
  });

  assertEquals(resp.type, "partial");
  assertEquals(resp.hasMore, true);
  assertEquals(resp.processedDataset, "works_recommended");
  assertEquals(resp.processedParts, [1, 2, 3, 4, 5]);
  assertEquals(resp.nextDataset, "works_recommended");
  assertEquals(resp.nextPart, 6);
  assertEquals(resp.partsProcessed, 5);
  assertEquals(resp.recordsProcessed, 10000);
  assertEquals(resp.failures, []);
});

Deno.test("computeCheckpoint - batch_size=3", () => {
  const resp = computeCheckpoint({
    currentDataset: "works_recommended",
    allPartsCount: 20,
    startPart: 1,
    processedParts: [1, 2, 3],
    recordsProcessed: 6000,
    batchSize: 3,
    discovered: DATASET_ORDER,
    timestamp: "2026-09-03T17-50-47Z",
  });

  assertEquals(resp.hasMore, true);
  assertEquals(resp.nextDataset, "works_recommended");
  assertEquals(resp.nextPart, 4);
  assertEquals(resp.partsProcessed, 3);
});

// ============================================================
// computeCheckpoint — dataset exhausted, advance to next
// ============================================================

Deno.test("computeCheckpoint - dataset exhausted, advance to next", () => {
  const resp = computeCheckpoint({
    currentDataset: "works_recommended",
    allPartsCount: 10,
    startPart: 6,
    processedParts: [6, 7, 8, 9, 10],
    recordsProcessed: 15000,
    batchSize: 5,
    discovered: DATASET_ORDER,
    timestamp: "2026-09-03T17-50-47Z",
  });

  assertEquals(resp.hasMore, true);
  assertEquals(resp.nextDataset, "works_sanctioned");
  assertEquals(resp.nextPart, 1);
  assertEquals(resp.partsProcessed, 5);
});

Deno.test("computeCheckpoint - last dataset exhausted, no more", () => {
  const resp = computeCheckpoint({
    currentDataset: "mla_calamity",
    allPartsCount: 3,
    startPart: 1,
    processedParts: [1, 2, 3],
    recordsProcessed: 5000,
    batchSize: 5,
    discovered: DATASET_ORDER,
    timestamp: "2026-09-03T17-50-47Z",
  });

  assertEquals(resp.hasMore, false);
  assertEquals(resp.nextDataset, undefined);
  assertEquals(resp.nextPart, undefined);
});

Deno.test("computeCheckpoint - partial dataset exhaustion (batch not full)", () => {
  const resp = computeCheckpoint({
    currentDataset: "works_recommended",
    allPartsCount: 8,
    startPart: 6,
    processedParts: [6, 7, 8],
    recordsProcessed: 3000,
    batchSize: 5,
    discovered: DATASET_ORDER,
    timestamp: "2026-09-03T17-50-47Z",
  });

  // Dataset exhausted, batch not full but we're done with this dataset
  assertEquals(resp.hasMore, true);
  assertEquals(resp.nextDataset, "works_sanctioned");
  assertEquals(resp.nextPart, 1);
  assertEquals(resp.partsProcessed, 3);
});

// ============================================================
// computeCheckpoint — zero parts processed
// ============================================================

Deno.test("computeCheckpoint - zero parts processed", () => {
  const resp = computeCheckpoint({
    currentDataset: "works_recommended",
    allPartsCount: 10,
    startPart: 1,
    processedParts: [],
    recordsProcessed: 0,
    batchSize: 5,
    discovered: DATASET_ORDER,
    timestamp: "2026-09-03T17-50-47Z",
  });

  assertEquals(resp.type, "partial");
  assertEquals(resp.hasMore, true);
  assertEquals(resp.processedParts, []);
  assertEquals(resp.partsProcessed, 0);
  assertEquals(resp.nextDataset, "works_recommended");
  assertEquals(resp.nextPart, 1);
});

// ============================================================
// buildCompleteResponse
// ============================================================

Deno.test("buildCompleteResponse - all fields correct", () => {
  const resp = buildCompleteResponse("2026-09-03T17-50-47Z");
  assertEquals(resp.type, "complete");
  assertEquals(resp.hasMore, false);
  assertEquals(resp.timestamp, "2026-09-03T17-50-47Z");
});

// ============================================================
// buildErrorResponse
// ============================================================

Deno.test("buildErrorResponse - includes failed part info", () => {
  const resp = buildErrorResponse({
    timestamp: "2026-09-03T17-50-47Z",
    failedDataset: "works_recommended",
    failedPart: 3,
    error: "Connection timeout",
    errorDetail: { message: "timeout" },
    processedParts: [1, 2],
    recordsProcessed: 4000,
  });

  assertEquals(resp.type, "error");
  assertEquals(resp.failedDataset, "works_recommended");
  assertEquals(resp.failedPart, 3);
  assertEquals(resp.error, "Connection timeout");
  assertEquals(resp.processedParts, [1, 2]);
  assertEquals(resp.recordsProcessed, 4000);
  assertEquals(resp.failures.length, 1);
  assertEquals(resp.failures[0].part, 3);
  assertEquals(resp.failures[0].error, "Connection timeout");
});

Deno.test("buildErrorResponse - no prior parts", () => {
  const resp = buildErrorResponse({
    timestamp: "2026-09-03T17-50-47Z",
    failedDataset: "works_recommended",
    failedPart: 1,
    error: "Storage error",
    errorDetail: null,
    processedParts: [],
    recordsProcessed: 0,
  });

  assertEquals(resp.processedParts, []);
  assertEquals(resp.recordsProcessed, 0);
  assertEquals(resp.failures.length, 1);
  assertEquals(resp.failures[0].part, 1);
});

// ============================================================
// skipEmptyDataset
// ============================================================

Deno.test("skipEmptyDataset - advances to next", () => {
  const result = skipEmptyDataset({
    currentDataset: "works_recommended",
    discovered: DATASET_ORDER,
  });
  assertEquals(result.skipped, true);
  assertEquals(result.nextDataset, "works_sanctioned");
});

Deno.test("skipEmptyDataset - last dataset returns undefined", () => {
  const result = skipEmptyDataset({
    currentDataset: "mla_calamity",
    discovered: DATASET_ORDER,
  });
  assertEquals(result.skipped, true);
  assertEquals(result.nextDataset, undefined);
});

// ============================================================
// skipBeyondEnd
// ============================================================

Deno.test("skipBeyondEnd - advances to next", () => {
  const result = skipBeyondEnd({
    currentDataset: "works_recommended",
    discovered: DATASET_ORDER,
  });
  assertEquals(result.skipped, true);
  assertEquals(result.nextDataset, "works_sanctioned");
});

Deno.test("skipBeyondEnd - last dataset returns undefined", () => {
  const result = skipBeyondEnd({
    currentDataset: "mla_calamity",
    discovered: DATASET_ORDER,
  });
  assertEquals(result.skipped, true);
  assertEquals(result.nextDataset, undefined);
});

// ============================================================
// Edge cases: start_part beyond final part
// ============================================================

Deno.test("computeCheckpoint - start_part beyond final part, not last dataset", () => {
  const resp = computeCheckpoint({
    currentDataset: "works_recommended",
    allPartsCount: 10,
    startPart: 15,
    processedParts: [],
    recordsProcessed: 0,
    batchSize: 5,
    discovered: DATASET_ORDER,
    timestamp: "2026-09-03T17-50-47Z",
  });

  assertEquals(resp.hasMore, true);
  assertEquals(resp.nextDataset, "works_sanctioned");
  assertEquals(resp.nextPart, 1);
  assertEquals(resp.partsProcessed, 0);
});

Deno.test("computeCheckpoint - start_part beyond final part, last dataset", () => {
  const resp = computeCheckpoint({
    currentDataset: "mla_calamity",
    allPartsCount: 5,
    startPart: 10,
    processedParts: [],
    recordsProcessed: 0,
    batchSize: 5,
    discovered: DATASET_ORDER,
    timestamp: "2026-09-03T17-50-47Z",
  });

  assertEquals(resp.hasMore, false);
  assertEquals(resp.nextDataset, undefined);
});

// ============================================================
// Dataset transition: exhaust dataset A, move to B
// ============================================================

Deno.test("computeCheckpoint - transition from allocated_limit to works_recommended", () => {
  const resp = computeCheckpoint({
    currentDataset: "allocated_limit",
    allPartsCount: 5,
    startPart: 1,
    processedParts: [1, 2, 3, 4, 5],
    recordsProcessed: 8000,
    batchSize: 5,
    discovered: DATASET_ORDER,
    timestamp: "2026-09-03T17-50-47Z",
  });

  assertEquals(resp.hasMore, true);
  assertEquals(resp.nextDataset, "works_recommended");
  assertEquals(resp.nextPart, 1);
  assertEquals(resp.processedDataset, "allocated_limit");
});

Deno.test("computeCheckpoint - transition from expenditure to calamity (last MP dataset)", () => {
  const resp = computeCheckpoint({
    currentDataset: "expenditure",
    allPartsCount: 2,
    startPart: 1,
    processedParts: [1, 2],
    recordsProcessed: 3000,
    batchSize: 5,
    discovered: DATASET_ORDER,
    timestamp: "2026-09-03T17-50-47Z",
  });

  assertEquals(resp.hasMore, true);
  assertEquals(resp.nextDataset, "calamity");
  assertEquals(resp.nextPart, 1);
});

Deno.test("computeCheckpoint - transition from mla_expenditure to mla_calamity", () => {
  const resp = computeCheckpoint({
    currentDataset: "mla_expenditure",
    allPartsCount: 3,
    startPart: 2,
    processedParts: [2, 3],
    recordsProcessed: 2000,
    batchSize: 5,
    discovered: DATASET_ORDER,
    timestamp: "2026-09-03T17-50-47Z",
  });

  assertEquals(resp.hasMore, true);
  assertEquals(resp.nextDataset, "mla_calamity");
  assertEquals(resp.nextPart, 1);
});

// ============================================================
// Idempotency: same batch invoked twice
// ============================================================

Deno.test("computeCheckpoint - same batch position returns same checkpoint", () => {
  const args = {
    currentDataset: "works_recommended",
    allPartsCount: 20,
    startPart: 6,
    processedParts: [6, 7, 8, 9, 10],
    recordsProcessed: 10000,
    batchSize: 5,
    discovered: DATASET_ORDER,
    timestamp: "2026-09-03T17-50-47Z",
  };

  const resp1 = computeCheckpoint(args);
  const resp2 = computeCheckpoint(args);

  assertEquals(resp1.nextDataset, resp2.nextDataset);
  assertEquals(resp1.nextPart, resp2.nextPart);
  assertEquals(resp1.hasMore, resp2.hasMore);
});

// ============================================================
// MAX_BATCH_SIZE constant
// ============================================================

Deno.test("MAX_BATCH_SIZE is 5", () => {
  assertEquals(MAX_BATCH_SIZE, 5);
});

Deno.test("DATASET_ORDER has 12 datasets", () => {
  assertEquals(DATASET_ORDER.length, 12);
});

Deno.test("DATASET_ORDER starts with allocated_limit", () => {
  assertEquals(DATASET_ORDER[0], "allocated_limit");
});

Deno.test("DATASET_ORDER ends with mla_calamity", () => {
  assertEquals(DATASET_ORDER[11], "mla_calamity");
});

// ============================================================
// Edge: single part dataset
// ============================================================

Deno.test("computeCheckpoint - single part dataset exhausted", () => {
  const resp = computeCheckpoint({
    currentDataset: "calamity",
    allPartsCount: 1,
    startPart: 1,
    processedParts: [1],
    recordsProcessed: 500,
    batchSize: 5,
    discovered: DATASET_ORDER,
    timestamp: "2026-09-03T17-50-47Z",
  });

  assertEquals(resp.hasMore, true);
  assertEquals(resp.nextDataset, "mla_allocated_limit");
  assertEquals(resp.nextPart, 1);
});

// ============================================================
// Edge: batch_size larger than remaining parts
// ============================================================

Deno.test("computeCheckpoint - batch_size=5 but only 2 remaining parts", () => {
  const resp = computeCheckpoint({
    currentDataset: "works_recommended",
    allPartsCount: 10,
    startPart: 9,
    processedParts: [9, 10],
    recordsProcessed: 2000,
    batchSize: 5,
    discovered: DATASET_ORDER,
    timestamp: "2026-09-03T17-50-47Z",
  });

  assertEquals(resp.hasMore, true);
  assertEquals(resp.nextDataset, "works_sanctioned");
  assertEquals(resp.nextPart, 1);
  assertEquals(resp.partsProcessed, 2);
});
