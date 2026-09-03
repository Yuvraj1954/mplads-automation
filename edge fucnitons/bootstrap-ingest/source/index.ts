import { serve } from "https://deno.land/std@0.224.0/http/server.ts";
import { createClient } from "https://esm.sh/@supabase/supabase-js@2";

// ============================================================
// Constants
// ============================================================

const BUCKET = "mplads-raw";

const MP_DATASETS = [
  "allocated_limit",
  "works_recommended",
  "works_sanctioned",
  "works_completed",
  "expenditure",
  "calamity",
];

const MLA_DATASETS = [
  "mla_allocated_limit",
  "mla_works_recommended",
  "mla_works_sanctioned",
  "mla_works_completed",
  "mla_expenditure",
  "mla_calamity",
];

const DATASET_ORDER = [...MP_DATASETS, ...MLA_DATASETS];

const MAX_BATCH_SIZE = 5;

// ============================================================
// Types
// ============================================================

interface InputParams {
  timestamp?: unknown;
  dataset?: unknown;
  start_part?: unknown;
  batch_size?: unknown;
}

interface ValidatedInput {
  timestamp: string;
  dataset: string | undefined;
  startPart: number;
  batchSize: number;
}

interface CheckpointResult {
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

// ============================================================
// Pure logic — testable without Supabase
// ============================================================

function validateInput(
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
      body: { success: false, error: "Missing timestamp" },
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

function findNextDataset(
  fromDataset: string | undefined,
  discovered: string[]
): string | undefined {
  if (fromDataset) {
    const idx = discovered.indexOf(fromDataset);
    if (idx !== -1) return fromDataset;
  }
  return discovered.length > 0 ? discovered[0] : undefined;
}

function computeCheckpoint(args: {
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

function buildErrorResponse(args: {
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
      { part: args.failedPart, error: args.error },
    ],
  };
}

function buildCompleteResponse(
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

function respToWire(
  resp: CheckpointResult
): Record<string, unknown> {
  if (resp.type === "complete") {
    return {
      success: true,
      status: "complete",
      timestamp: resp.timestamp,
      has_more: false,
    };
  }

  if (resp.type === "error") {
    return {
      success: false,
      status: "error",
      timestamp: resp.timestamp,
      failed_dataset: resp.failedDataset,
      failed_part: resp.failedPart,
      error: resp.error,
      error_detail: resp.errorDetail,
      processed_dataset: resp.processedDataset,
      processed_parts: resp.processedParts,
      records_processed: resp.recordsProcessed,
    };
  }

  return {
    success: true,
    status: "partial",
    timestamp: resp.timestamp,
    processed_dataset: resp.processedDataset,
    processed_parts: resp.processedParts,
    next_dataset: resp.nextDataset,
    next_part: resp.nextPart,
    has_more: resp.hasMore,
    parts_processed: resp.partsProcessed,
    records_processed: resp.recordsProcessed,
    failures: resp.failures,
  };
}

// ============================================================
// Helpers
// ============================================================

function errorMessage(error: unknown): string {
  if (error instanceof Error) return error.message;
  return String(error);
}

function fullError(error: unknown): Record<string, unknown> {
  if (error instanceof Error) {
    return {
      name: error.name,
      message: error.message,
      stack: error.stack,
    };
  }
  if (typeof error === "object" && error !== null) {
    return error as Record<string, unknown>;
  }
  return { raw: String(error) };
}

// ============================================================
// Supabase client
// ============================================================

const SUPABASE_URL = Deno.env.get("SUPABASE_URL");
const SUPABASE_SERVICE_ROLE_KEY = Deno.env.get(
  "SUPABASE_SERVICE_ROLE_KEY"
);

if (!SUPABASE_URL || !SUPABASE_SERVICE_ROLE_KEY) {
  console.error(
    "FATAL: Missing required environment variables"
  );
}

const supabase = createClient(
  SUPABASE_URL ?? "",
  SUPABASE_SERVICE_ROLE_KEY ?? ""
);

// ============================================================
// Storage helpers
// ============================================================

async function listNdjsonParts(
  timestamp: string,
  dataset: string
): Promise<string[]> {
  const prefix = `${timestamp}/${dataset}`;
  let offset = 0;
  const ndjsonFiles: string[] = [];

  while (true) {
    const listResult = await supabase.storage
      .from(BUCKET)
      .list(prefix, {
        limit: 100,
        offset,
        sortBy: { column: "name", order: "asc" },
      });

    if (listResult?.error) {
      throw new Error(
        `Storage listing failed for ${dataset}: ${errorMessage(listResult.error)}`
      );
    }

    const files = listResult?.data;
    if (!files || files.length === 0) break;

    for (const item of files) {
      if (item.name.endsWith(".ndjson")) {
        ndjsonFiles.push(item.name);
      }
    }

    if (files.length < 100) break;
    offset += 100;
  }

  ndjsonFiles.sort();
  return ndjsonFiles;
}

async function discoverDatasets(
  timestamp: string
): Promise<string[]> {
  const discovered: string[] = [];

  for (const dataset of DATASET_ORDER) {
    const prefix = `${timestamp}/${dataset}`;
    try {
      const listResult = await supabase.storage
        .from(BUCKET)
        .list(prefix, {
          limit: 1,
          sortBy: { column: "name", order: "asc" },
        });

      if (listResult?.error) {
        console.warn(
          `[DISCOVER] list(${prefix}) error:`,
          errorMessage(listResult.error)
        );
        continue;
      }

      if (listResult?.data && listResult.data.length > 0) {
        discovered.push(dataset);
      }
    } catch (listErr) {
      console.warn(
        `[DISCOVER] list(${prefix}) THREW:`,
        fullError(listErr)
      );
      continue;
    }
  }

  return discovered;
}

// ============================================================
// MAIN
// ============================================================

serve(async (req) => {
  const startTime = Date.now();
  console.log(`\n${"=".repeat(60)}`);
  console.log(`REQUEST: ${req.method} ${req.url}`);
  console.log(`${"=".repeat(60)}`);

  try {
    // --------------------------------------------------
    // STEP 1: Parse & validate request body
    // --------------------------------------------------
    let body: Record<string, unknown>;
    try {
      body = await req.json();
    } catch (parseErr) {
      return new Response(
        JSON.stringify({
          success: false,
          error: `Failed to parse request body: ${errorMessage(parseErr)}`,
        }),
        { status: 400, headers: { "Content-Type": "application/json" } }
      );
    }

    const validated = validateInput(body);
    if (!validated.ok) {
      return new Response(
        JSON.stringify(validated.body),
        {
          status: validated.status,
          headers: { "Content-Type": "application/json" },
        }
      );
    }

    const {
      timestamp,
      dataset: requestedDataset,
      startPart,
      batchSize,
    } = validated.value;

    console.log(
      `[STEP 1] timestamp=${timestamp} dataset=${requestedDataset ?? "(auto)"} start=${startPart} batch=${batchSize}`
    );

    // --------------------------------------------------
    // STEP 2: Validate completion marker
    // --------------------------------------------------
    const markerPath = `${timestamp}/_COMPLETE.json`;
    let markerFile: Blob | null = null;

    try {
      const downloadResult = await supabase.storage
        .from(BUCKET)
        .download(markerPath);
      markerFile = downloadResult?.data ?? null;

      if (downloadResult?.error || !markerFile) {
        return new Response(
          JSON.stringify({
            success: false,
            error: `Completion marker not found: ${markerPath}`,
          }),
          {
            status: 400,
            headers: { "Content-Type": "application/json" },
          }
        );
      }
    } catch (downloadErr) {
      return new Response(
        JSON.stringify({
          success: false,
          error: `Failed to download completion marker: ${errorMessage(downloadErr)}`,
        }),
        { status: 500, headers: { "Content-Type": "application/json" } }
      );
    }

    let marker: Record<string, unknown>;
    try {
      marker = JSON.parse(await markerFile.text());
    } catch {
      return new Response(
        JSON.stringify({
          success: false,
          error: "Completion marker is not valid JSON",
        }),
        { status: 400, headers: { "Content-Type": "application/json" } }
      );
    }

    if (marker.status !== "complete") {
      return new Response(
        JSON.stringify({
          success: false,
          error: `Snapshot status is "${marker.status}", expected "complete"`,
        }),
        { status: 400, headers: { "Content-Type": "application/json" } }
      );
    }

    console.log("[STEP 2] Completion marker verified");

    // --------------------------------------------------
    // STEP 3: Discover datasets
    // --------------------------------------------------
    const discovered = await discoverDatasets(timestamp);
    console.log(
      `[STEP 3] Discovered ${discovered.length} datasets:`,
      discovered.join(", ")
    );

    if (discovered.length === 0) {
      return new Response(
        JSON.stringify({
          success: false,
          error: "No datasets found in snapshot",
        }),
        { status: 400, headers: { "Content-Type": "application/json" } }
      );
    }

    // --------------------------------------------------
    // STEP 4: Resolve starting dataset
    // --------------------------------------------------
    let currentDataset = findNextDataset(
      requestedDataset,
      discovered
    );

    if (!currentDataset) {
      const resp = buildCompleteResponse(timestamp);
      return new Response(
        JSON.stringify(respToWire(resp)),
        {
          status: 200,
          headers: { "Content-Type": "application/json" },
        }
      );
    }

    let startPartFinal = startPart;

    if (
      requestedDataset &&
      discovered.indexOf(requestedDataset) === -1
    ) {
      startPartFinal = 1;
    }

    // --------------------------------------------------
    // STEP 5: Process parts from one dataset
    // --------------------------------------------------
    let allParts: string[];
    try {
      allParts = await listNdjsonParts(
        timestamp,
        currentDataset
      );
    } catch (listErr) {
      return new Response(
        JSON.stringify(
          respToWire(
            buildErrorResponse({
              timestamp,
              failedDataset: currentDataset,
              failedPart: 0,
              error: errorMessage(listErr),
              errorDetail: fullError(listErr),
              processedParts: [],
              recordsProcessed: 0,
            })
          )
        ),
        {
          status: 500,
          headers: { "Content-Type": "application/json" },
        }
      );
    }

    if (allParts.length === 0) {
      const nextIdx =
        discovered.indexOf(currentDataset) + 1;
      const nextDataset =
        nextIdx < discovered.length
          ? discovered[nextIdx]
          : undefined;

      if (!nextDataset) {
        const resp = buildCompleteResponse(timestamp);
        return new Response(
          JSON.stringify(respToWire(resp)),
          {
            status: 200,
            headers: { "Content-Type": "application/json" },
          }
        );
      }

      const resp = computeCheckpoint({
        currentDataset,
        allPartsCount: 0,
        startPart: 1,
        processedParts: [],
        recordsProcessed: 0,
        batchSize,
        discovered,
        timestamp,
      });
      resp.nextDataset = nextDataset;
      resp.nextPart = 1;
      resp.hasMore = true;
      return new Response(
        JSON.stringify(respToWire(resp)),
        {
          status: 200,
          headers: { "Content-Type": "application/json" },
        }
      );
    }

    if (startPartFinal > allParts.length) {
      const nextIdx =
        discovered.indexOf(currentDataset) + 1;
      const nextDataset =
        nextIdx < discovered.length
          ? discovered[nextIdx]
          : undefined;

      if (!nextDataset) {
        const resp = buildCompleteResponse(timestamp);
        return new Response(
          JSON.stringify(respToWire(resp)),
          {
            status: 200,
            headers: { "Content-Type": "application/json" },
          }
        );
      }

      const resp = computeCheckpoint({
        currentDataset,
        allPartsCount: allParts.length,
        startPart: startPartFinal,
        processedParts: [],
        recordsProcessed: 0,
        batchSize,
        discovered,
        timestamp,
      });
      resp.nextDataset = nextDataset;
      resp.nextPart = 1;
      resp.hasMore = true;
      return new Response(
        JSON.stringify(respToWire(resp)),
        {
          status: 200,
          headers: { "Content-Type": "application/json" },
        }
      );
    }

    const remainingInDataset =
      allParts.length - startPartFinal + 1;
    const partsToProcess = Math.min(
      batchSize,
      remainingInDataset
    );

    const processedParts: number[] = [];
    let recordsProcessed = 0;

    for (let i = 0; i < partsToProcess; i++) {
      const partIndex = startPartFinal - 1 + i;
      const fileName = allParts[partIndex];
      const filePath = `${timestamp}/${currentDataset}/${fileName}`;
      const partNumber = partIndex + 1;

      console.log(
        `[BATCH] (${i + 1}/${partsToProcess}) Invoking ingest-mplads-part for ${filePath}`
      );

      try {
        const response = await supabase.functions.invoke(
          "ingest-mplads-part",
          { body: { path: filePath } }
        );

        if (response?.error) {
          throw new Error(errorMessage(response.error));
        }

        const result = response?.data as
          | Record<string, unknown>
          | null;
        const records = Number(result?.records ?? 0);
        recordsProcessed += records;
        processedParts.push(partNumber);

        console.log(
          `[BATCH] Part ${partNumber}: ${records} records`
        );
      } catch (error) {
        console.error(
          `[BATCH] Part ${partNumber} failed:`,
          errorMessage(error)
        );

        const resp = buildErrorResponse({
          timestamp,
          failedDataset: currentDataset,
          failedPart: partNumber,
          error: errorMessage(error),
          errorDetail: fullError(error),
          processedParts,
          recordsProcessed,
        });

        return new Response(
          JSON.stringify(respToWire(resp)),
          {
            status: 500,
            headers: { "Content-Type": "application/json" },
          }
        );
      }
    }

    // --------------------------------------------------
    // STEP 6: Build checkpoint
    // --------------------------------------------------
    const resp = computeCheckpoint({
      currentDataset,
      allPartsCount: allParts.length,
      startPart: startPartFinal,
      processedParts,
      recordsProcessed,
      batchSize,
      discovered,
      timestamp,
    });

    console.log(
      `[BATCH] Complete: ${processedParts.length} parts, ${recordsProcessed} records, has_more=${resp.hasMore}`
    );

    return new Response(
      JSON.stringify(respToWire(resp), null, 2),
      {
        status: 200,
        headers: { "Content-Type": "application/json" },
      }
    );
  } catch (error) {
    const durationMs = Date.now() - startTime;
    console.error(
      `\n[FATAL] Error after ${durationMs}ms:`,
      fullError(error)
    );

    return new Response(
      JSON.stringify(
        {
          success: false,
          error: errorMessage(error),
          error_detail: fullError(error),
          duration_ms: durationMs,
        },
        null,
        2
      ),
      {
        status: 500,
        headers: { "Content-Type": "application/json" },
      }
    );
  }
});
