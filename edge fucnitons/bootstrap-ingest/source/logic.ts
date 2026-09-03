// Pure logic for bootstrap-ingest — testable without Supabase
export const BUCKET = "mplads-raw";

export const MP_DATASETS = [
  "allocated_limit",
  "works_recommended",
  "works_sanctioned",
  "works_completed",
  "expenditure",
  "calamity",
];

export const MLA_DATASETS = [
  "mla_allocated_limit",
  "mla_works_recommended",
  "mla_works_sanctioned",
  "mla_works_completed",
  "mla_expenditure",
  "mla_calamity",
];

export const DATASET_ORDER = [
  ...MP_DATASETS,
  ...MLA_DATASETS,
];

export const MAX_BATCH_SIZE = 5;

export interface InputParams {
  timestamp?: unknown;
  dataset?: unknown;
  start_part?: unknown;
  batch_size?: unknown;
}

export interface ValidatedInput {
  timestamp: string;
  dataset: string | undefined;
  startPart: number;
  batchSize: number;
}

export interface CheckpointResult {
  type: "partial" | "complete" | "error";
  timestamp: string;
  processedDataset?: string;
  processedParts?: number[];
  nextDataset?: string;
  nextPart?: number;
  hasMore: boolean;
  partsProcessed: number;
  recordsProcessed: number;
  failures: { part: number; error: string }[];
  failedDataset?: string;
  failedPart?: number;
  error?: string;
  errorDetail?: unknown;
}

export function validateInput(
  body: InputParams
):
  | { ok: true; value: ValidatedInput }
  | { ok: false; status: number; body: Record<string, unknown> } {
  const timestamp = body?.timestamp;
  if (
    typeof timestamp !== "string" ||
    timestamp.trim() === ""
  ) {
    return {
      ok: false,
      status: 400,
      body: {
        success: false,
        error: "Missing timestamp",
      },
    };
  }

  const dataset =
    typeof body?.dataset === "string" &&
    body.dataset.trim() !== ""
      ? body.dataset.trim()
      : undefined;

  let startPart =
    typeof body?.start_part === "number" &&
    body.start_part >= 1
      ? Math.floor(body.start_part)
      : 1;

  let batchSize =
    typeof body?.batch_size === "number" &&
    body.batch_size >= 1
      ? Math.floor(body.batch_size)
      : MAX_BATCH_SIZE;

  if (batchSize > MAX_BATCH_SIZE) {
    batchSize = MAX_BATCH_SIZE;
  }

  return {
    ok: true,
    value: { timestamp, dataset, startPart, batchSize },
  };
}

export function findNextDataset(
  fromDataset: string | undefined,
  discovered: string[]
): string | undefined {
  if (fromDataset) {
    const idx = discovered.indexOf(fromDataset);
    if (idx !== -1) return fromDataset;
  }
  return discovered.length > 0 ? discovered[0] : undefined;
}

export function computeCheckpoint(args: {
  currentDataset: string;
  allPartsCount: number;
  startPart: number;
  processedParts: number[];
  recordsProcessed: number;
  batchSize: number;
  discovered: string[];
  timestamp: string;
}): CheckpointResult {
  const {
    currentDataset,
    allPartsCount,
    startPart,
    processedParts,
    recordsProcessed,
    batchSize,
    discovered,
    timestamp,
  } = args;

  if (processedParts.length === 0) {
    // start_part beyond final part or dataset empty — advance
    if (startPart > allPartsCount || allPartsCount === 0) {
      const nextIdx =
        discovered.indexOf(currentDataset) + 1;
      const nextDataset =
        nextIdx < discovered.length
          ? discovered[nextIdx]
          : undefined;
      return {
        type: "partial",
        timestamp,
        processedDataset: currentDataset,
        processedParts: [],
        nextDataset,
        nextPart: nextDataset ? 1 : undefined,
        hasMore: nextDataset !== undefined,
        partsProcessed: 0,
        recordsProcessed: 0,
        failures: [],
      };
    }
    return {
      type: "partial",
      timestamp,
      processedDataset: currentDataset,
      processedParts: [],
      nextDataset: currentDataset,
      nextPart: startPart,
      hasMore: true,
      partsProcessed: 0,
      recordsProcessed: 0,
      failures: [],
    };
  }

  const lastPartNumber =
    processedParts[processedParts.length - 1];
  const datasetExhausted =
    lastPartNumber >= allPartsCount;
  const batchFull =
    processedParts.length >= batchSize;

  let nextDataset: string | undefined;
  let nextPart: number | undefined;
  let hasMore: boolean;

  if (!datasetExhausted && batchFull) {
    nextDataset = currentDataset;
    nextPart = lastPartNumber + 1;
    hasMore = true;
  } else if (datasetExhausted) {
    const nextIdx =
      discovered.indexOf(currentDataset) + 1;
    if (nextIdx < discovered.length) {
      nextDataset = discovered[nextIdx];
      nextPart = 1;
      hasMore = true;
    } else {
      hasMore = false;
    }
  } else {
    nextDataset = currentDataset;
    nextPart = lastPartNumber + 1;
    hasMore = true;
  }

  return {
    type: "partial",
    timestamp,
    processedDataset: currentDataset,
    processedParts,
    nextDataset,
    nextPart,
    hasMore,
    partsProcessed: processedParts.length,
    recordsProcessed,
    failures: [],
  };
}

export function buildErrorResponse(args: {
  timestamp: string;
  failedDataset: string;
  failedPart: number;
  error: string;
  errorDetail: unknown;
  processedParts: number[];
  recordsProcessed: number;
}): CheckpointResult {
  return {
    type: "error",
    timestamp: args.timestamp,
    failedDataset: args.failedDataset,
    failedPart: args.failedPart,
    error: args.error,
    errorDetail: args.errorDetail,
    processedDataset: args.failedDataset,
    processedParts: args.processedParts,
    hasMore: false,
    partsProcessed: args.processedParts.length,
    recordsProcessed: args.recordsProcessed,
    failures: [
      {
        part: args.failedPart,
        error: args.error,
      },
    ],
  };
}

export function buildCompleteResponse(
  timestamp: string
): CheckpointResult {
  return {
    type: "complete",
    timestamp,
    hasMore: false,
    partsProcessed: 0,
    recordsProcessed: 0,
    failures: [],
  };
}

export function skipEmptyDataset(args: {
  currentDataset: string;
  discovered: string[];
}): {
  skipped: boolean;
  nextDataset: string | undefined;
} {
  const nextIdx =
    args.discovered.indexOf(args.currentDataset) + 1;
  if (nextIdx < args.discovered.length) {
    return {
      skipped: true,
      nextDataset: args.discovered[nextIdx],
    };
  }
  return { skipped: true, nextDataset: undefined };
}

export function skipBeyondEnd(args: {
  currentDataset: string;
  discovered: string[];
}): {
  skipped: boolean;
  nextDataset: string | undefined;
} {
  const nextIdx =
    args.discovered.indexOf(args.currentDataset) + 1;
  if (nextIdx < args.discovered.length) {
    return {
      skipped: true,
      nextDataset: args.discovered[nextIdx],
    };
  }
  return { skipped: true, nextDataset: undefined };
}
