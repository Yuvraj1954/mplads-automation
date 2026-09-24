import { serve } from "https://deno.land/std@0.224.0/http/server.ts";
import { createClient } from "https://esm.sh/@supabase/supabase-js@2";

const SUPABASE_URL = Deno.env.get("SUPABASE_URL")!;
const SUPABASE_SERVICE_ROLE_KEY =
  Deno.env.get("SUPABASE_SERVICE_ROLE_KEY")!;

const supabase = createClient(
  SUPABASE_URL,
  SUPABASE_SERVICE_ROLE_KEY
);

const BUCKET = "mplads-raw";
const DB_BATCH_SIZE = 500;

// ============================================================
// HELPERS
// ============================================================

/**
 * Normalize a member name for identity matching.
 *
 * Mirrors analysis/snapshot_loader.py:normalize_member_name() exactly.
 * Strips: parenthetical suffixes, common Indian honorific prefixes,
 * collapses whitespace, and uppercases for case-insensitive comparison.
 *
 * This ensures "Shri Parimal Nathwani (2026-32)" and
 * "Parimal Nathwani (2026-32)" resolve to the same canonical identity.
 */
function normalizeMemberName(name: string | null): string {
  if (!name) {
    return "";
  }
  let n = name.trim();
  // Remove parenthetical suffixes like (2024-30)
  n = n.replace(/\s*\(.*?\)\s*$/, "");
  // Remove common title prefixes
  n = n.replace(
    /^(Shri|Smt\.?|Dr\.?|Mrs\.?|Ms\.?|Late)\s+/i,
    ""
  );
  // Uppercase for comparison
  n = n.toUpperCase();
  // Normalize whitespace
  n = n.replace(/\s+/g, " ").trim();
  return n;
}

/**
 * Build a canonical identity key for MP/MLA resolution.
 *
 * Format: "<normalized_name>|<constituency_id>"
 * Uses normalizeMemberName() so honorific/casing variations
 * resolve to the same identity.
 */
function memberKey(
  name: string | null,
  constituencyId: number | null
): string {
  return `${normalizeMemberName(name)}|${
    constituencyId ?? ""
  }`;
}

function text(value: unknown): string | null {
  if (value === null || value === undefined) {
    return null;
  }

  const v = String(value)
    .replace(/\t/g, " ")
    .trim();

  return v === "" ? null : v;
}

function num(value: unknown): number | null {
  if (value === null || value === undefined || value === "") {
    return null;
  }

  const n = Number(value);

  return Number.isFinite(n) ? n : null;
}

function parseDate(value: unknown): string | null {
  if (!value) {
    return null;
  }

  const raw = String(value).trim();

  // Already PostgreSQL-friendly
  if (/^\d{4}-\d{2}-\d{2}$/.test(raw)) {
    return raw;
  }

  // Example:
  // 08-Jul-2024
  // 05-Sep-2024
  const match = raw.match(
    /^(\d{1,2})-([A-Za-z]{3})-(\d{4})$/
  );

  if (match) {
    const months: Record<string, string> = {
      Jan: "01",
      Feb: "02",
      Mar: "03",
      Apr: "04",
      May: "05",
      Jun: "06",
      Jul: "07",
      Aug: "08",
      Sep: "09",
      Oct: "10",
      Nov: "11",
      Dec: "12",
    };

    const month = months[match[2]];

    if (month) {
      return `${match[3]}-${month}-${match[1].padStart(2, "0")}`;
    }
  }

  // Example:
  // Jun 4, 2024 12:00:00 AM
  const parsed = new Date(raw);

  if (!Number.isNaN(parsed.getTime())) {
    return parsed.toISOString().slice(0, 10);
  }

  return null;
}

// ============================================================
// EXPENDITURE FINGERPRINT
// ============================================================

function fingerprintPart(
  value: unknown
): string {
  if (
    value === null ||
    value === undefined
  ) {
    return "<NULL>";
  }

  return String(value);
}

function fingerprintAmount(
  value: number | null
): string {
  if (value === null) {
    return "<NULL>";
  }

  /*
   * Must match the SQL fingerprint created
   * during the database migration.
   *
   * Examples:
   * 1017636.00 -> 1017636
   * 2500.50    -> 2500.5
   * 1000.00    -> 1000
   */
  return value
    .toFixed(2)
    .replace(/0+$/, "")
    .replace(/\.$/, "");
}

/*
 * IMPORTANT:
 *
 * This fingerprint is generated from RAW
 * government-source values, before any
 * state/constituency/MP/work DB ID resolution.
 *
 * This keeps the identity stable even if
 * internal DB IDs change.
 *
 * Identity fields:
 *   WORK_RECOMMENDATION_DTL_ID
 *   WORK_ID
 *   EXPENDITURE_DATE
 *   VENDOR_NAME
 *   WORK_STATUS
 *   FUND_DISBURSED_AMT
 *   STATE_NAME
 *   CONSTITUENCY
 *   MP_NAME
 */
async function expenditureFingerprint(
  record: Record<string, unknown>
): Promise<string> {

  const canonicalValue = [
    fingerprintPart(
      num(
        record.WORK_RECOMMENDATION_DTL_ID
      )
    ),

    fingerprintPart(
      text(record.WORK_ID)
    ),

    fingerprintPart(
      parseDate(
        record.EXPENDITURE_DATE
      )
    ),

    fingerprintPart(
      text(record.VENDOR_NAME)
    ),

    fingerprintPart(
      text(record.WORK_STATUS)
    ),

    fingerprintAmount(
      num(
        record.FUND_DISBURSED_AMT
      )
    ),

    fingerprintPart(
      text(record.STATE_NAME)
    ),

    fingerprintPart(
      text(record.CONSTITUENCY)
    ),

    fingerprintPart(
      text(record.MP_NAME)
    ),
  ].join("|");

  const data =
    new TextEncoder().encode(
      canonicalValue
    );

  const hash =
    await crypto.subtle.digest(
      "SHA-256",
      data
    );

  return Array.from(
    new Uint8Array(hash)
  )
    .map(
      byte =>
        byte
          .toString(16)
          .padStart(2, "0")
    )
    .join("");
}

function datasetFromPath(path: string): string {
  const parts = path.split("/");

  if (parts.length < 3) {
    throw new Error(
      "Invalid path. Expected timestamp/dataset/part.ndjson"
    );
  }

  return parts[1];
}

function errorDetails(error: unknown) {
  if (error instanceof Error) {
    return {
      name: error.name,
      message: error.message,
      stack: error.stack,
    };
  }

  return error;
}

// ============================================================
// GENERIC BATCH UPSERT
// ============================================================

async function upsertBatches(
  table: string,
  rows: Record<string, unknown>[],
  conflict?: string
) {
  if (rows.length === 0) {
    return [];
  }

  const output: Record<string, unknown>[] = [];

  for (
    let i = 0;
    i < rows.length;
    i += DB_BATCH_SIZE
  ) {
    let batch = rows.slice(
      i,
      i + DB_BATCH_SIZE
    );

    /*
     * When using a conflict key, remove
     * duplicate keys inside the same
     * request batch.
     *
     * This correctly handles both
     * single-column conflicts like
     * "source_record_key" and composite
     * conflicts like
     * "mp_id,consent_date,calamity_name".
     */
    if (conflict) {
      const conflictColumns =
        conflict
          .split(",")
          .map(
            column =>
              column.trim()
          );

      const uniqueRows =
        new Map<
          string,
          Record<string, unknown>
        >();

      for (const row of batch) {
        const key =
          JSON.stringify(
            conflictColumns.map(
              column =>
                row[column] ??
                null
            )
          );

        uniqueRows.set(
          key,
          row
        );
      }

      batch = [
        ...uniqueRows.values()
      ];
    }

    const options = conflict
      ? { onConflict: conflict }
      : undefined;

    const { data, error } =
      await supabase
        .from(table)
        .upsert(
          batch,
          options
        )
        .select();

    if (error) {
      throw new Error(
        `${table} upsert failed: ${JSON.stringify(error)}`
      );
    }

    if (data) {
      output.push(...data);
    }
  }

  return output;
}

// ============================================================
// STATE RESOLUTION
// ============================================================

async function resolveStates(
  records: Record<string, unknown>[]
) {
  const stateNames = [
    ...new Set(
      records
        .map(r => text(r.STATE_NAME))
        .filter(
          (x): x is string => Boolean(x)
        )
    ),
  ];

  const result = new Map<string, number>();

  if (stateNames.length === 0) {
    return result;
  }

  const { data, error } =
    await supabase
      .from("states")
      .select("state_id,state_name")
      .in("state_name", stateNames);

  if (error) {
    throw new Error(
      `State lookup failed: ${JSON.stringify(error)}`
    );
  }

  for (const row of data ?? []) {
    result.set(
      String(row.state_name)
        .trim()
        .toLowerCase(),
      Number(row.state_id)
    );
  }

  const missing = stateNames.filter(
    name =>
      !result.has(
        name.trim().toLowerCase()
      )
  );

  if (missing.length > 0) {
    const rows = missing.map(
      state_name => ({
        state_name,
      })
    );

    const inserted =
      await upsertBatches(
        "states",
        rows,
        "state_name"
      );

    for (const row of inserted) {
      result.set(
        String(row.state_name)
          .trim()
          .toLowerCase(),
        Number(row.state_id)
      );
    }
  }

  return result;
}

// ============================================================
// CONSTITUENCY RESOLUTION
// ============================================================

async function resolveConstituencies(
  records: Record<string, unknown>[],
  states: Map<string, number>
) {
  const result = new Map<string, number>();

  const wanted = new Map<
    string,
    {
      state_id: number;
      constituency_name: string;
    }
  >();

  for (const record of records) {
    const stateName =
      text(record.STATE_NAME);

    const constituencyName =
      text(record.CONSTITUENCY);

    if (
      !stateName ||
      !constituencyName
    ) {
      continue;
    }

    const stateId =
      states.get(
        stateName.toLowerCase()
      );

    if (!stateId) {
      continue;
    }

    const key =
      `${stateId}|${constituencyName.toLowerCase()}`;

    wanted.set(key, {
      state_id: stateId,
      constituency_name:
        constituencyName,
    });
  }

  if (wanted.size === 0) {
    return result;
  }

  const stateIds = [
    ...new Set(
      [...wanted.values()]
        .map(x => x.state_id)
    ),
  ];

  const { data, error } =
    await supabase
      .from("constituencies")
      .select(
        "constituency_id,state_id,constituency_name"
      )
      .in("state_id", stateIds);

  if (error) {
    throw new Error(
      `Constituency lookup failed: ${JSON.stringify(error)}`
    );
  }

  for (const row of data ?? []) {
    const key =
      `${row.state_id}|${String(
        row.constituency_name
      ).trim().toLowerCase()}`;

    result.set(
      key,
      Number(row.constituency_id)
    );
  }

  const missing: Record<string, unknown>[] = [];

  for (const [key, value] of wanted) {
    if (!result.has(key)) {
      missing.push({
        state_id: value.state_id,
        constituency_name:
          value.constituency_name,
      });
    }
  }

  if (missing.length > 0) {
    const inserted =
      await upsertBatches(
        "constituencies",
        missing,
        "state_id,constituency_name"
      );

    for (const row of inserted) {
      const key =
        `${row.state_id}|${String(
          row.constituency_name
        ).trim().toLowerCase()}`;

      result.set(
        key,
        Number(row.constituency_id)
      );
    }
  }

  return result;
}

// ============================================================
// MP RESOLUTION
// ============================================================

async function resolveMps(
  records: Record<string, unknown>[],
  states: Map<string, number>,
  constituencies: Map<string, number>
) {
  const result = new Map<string, number>();

  // Get existing MPs.
  const { data, error } =
    await supabase
      .from("mps")
      .select(
        "mp_id,mp_name,constituency_id"
      );

  if (error) {
    throw new Error(
      `MP lookup failed: ${JSON.stringify(error)}`
    );
  }

  for (const row of data ?? []) {
    const key = memberKey(
      row.mp_name,
      row.constituency_id
    );

    result.set(
      key,
      Number(row.mp_id)
    );
  }

  const newMps: Record<string, unknown>[] = [];

  const pendingKeys = new Set<string>();

  for (const record of records) {
    const mpName =
      text(record.MP_NAME);

    const stateName =
      text(record.STATE_NAME);

    const constituencyName =
      text(record.CONSTITUENCY);

    if (!mpName) {
      continue;
    }

    let constituencyId:
      number | null = null;

    if (
      stateName &&
      constituencyName
    ) {
      const stateId =
        states.get(
          stateName.toLowerCase()
        );

      if (stateId) {
        constituencyId =
          constituencies.get(
            `${stateId}|${constituencyName.toLowerCase()}`
          ) ?? null;
      }
    }

    const key = memberKey(
      mpName,
      constituencyId
    );

    if (
      !result.has(key) &&
      !pendingKeys.has(key)
    ) {
      pendingKeys.add(key);

      newMps.push({
        mp_name: mpName,

        house_name:
          text(record.HOUSE_NAME),

        constituency_id:
          constituencyId,

        tenure:
          text(record.TENURE),

        tenure_start_date:
          parseDate(
            record.TENURE_START_DATE
          ),

        tenure_end_date:
          parseDate(
            record.TENURE_END_DATE
          ),
      });
    }
  }

  // Insert missing MPs.
  for (
    let i = 0;
    i < newMps.length;
    i += DB_BATCH_SIZE
  ) {
    const batch =
      newMps.slice(
        i,
        i + DB_BATCH_SIZE
      );

    const { data: inserted, error } =
      await supabase
        .from("mps")
        .insert(batch)
        .select();

    if (error) {
      throw new Error(
        `MP insert failed: ${JSON.stringify(error)}`
      );
    }

    for (const row of inserted ?? []) {
      const key = memberKey(
        row.mp_name,
        row.constituency_id
      );

      result.set(
        key,
        Number(row.mp_id)
      );
    }
  }

  return result;
}

// ============================================================
// GET WORK IDS
// ============================================================

async function getWorkMap(
  dtlIds: number[]
) {
  const result = new Map<number, number>();

  const uniqueIds = [
    ...new Set(dtlIds),
  ];

  for (
    let i = 0;
    i < uniqueIds.length;
    i += 500
  ) {
    const batch =
      uniqueIds.slice(
        i,
        i + 500
      );

    const { data, error } =
      await supabase
        .from("works")
        .select(
          "work_id,work_recommendation_dtl_id"
        )
        .in(
          "work_recommendation_dtl_id",
          batch
        );

    if (error) {
      throw new Error(
        `Work lookup failed: ${JSON.stringify(error)}`
      );
    }

    for (const row of data ?? []) {
      result.set(
        Number(
          row.work_recommendation_dtl_id
        ),
        Number(row.work_id)
      );
    }
  }

  return result;
}

// ============================================================
// RECOMMENDED
// ============================================================

async function ingestRecommended(
  records: Record<string, unknown>[]
) {
  const states =
    await resolveStates(records);

  const constituencies =
    await resolveConstituencies(
      records,
      states
    );

  const mps =
    await resolveMps(
      records,
      states,
      constituencies
    );

  const works = [];

  for (const record of records) {
    const dtlId =
      num(
        record.WORK_RECOMMENDATION_DTL_ID
      );

    if (dtlId === null) {
      continue;
    }

    const stateName =
      text(record.STATE_NAME);

    const constituencyName =
      text(record.CONSTITUENCY);

    let constituencyId:
      number | null = null;

    if (
      stateName &&
      constituencyName
    ) {
      const stateId =
        states.get(
          stateName.toLowerCase()
        );

      if (stateId) {
        constituencyId =
          constituencies.get(
            `${stateId}|${constituencyName.toLowerCase()}`
          ) ?? null;
      }
    }

    const mpName =
      text(record.MP_NAME);

    let mpId:
      number | null = null;

    if (mpName) {
      mpId =
        mps.get(
          memberKey(mpName, constituencyId)
        ) ?? null;
    }

    /*
     * ACTIVITY_NAME contains:
     *
     * WS/MP620/2024-2025/133166-Construction...
     *
     * Preserve the whole government source value.
     */
    works.push({
      work_recommendation_dtl_id:
        dtlId,

      source_work_id:
        text(record.ACTIVITY_NAME),

      mp_id:
        mpId,

      constituency_id:
        constituencyId,

      work_category:
        text(record.WORK_CATEGORY),

      activity_name:
        text(record.ACTIVITY_NAME),

      work_description:
        text(record.WORK_DESCRIPTION),

      updated_at:
        new Date().toISOString(),
    });
  }

  await upsertBatches(
    "works",
    works,
    "work_recommendation_dtl_id"
  );

  const workMap =
    await getWorkMap(
      works.map(
        w =>
          Number(
            w.work_recommendation_dtl_id
          )
      )
    );

  const recommendations = [];
  const sanctions = [];

  for (const record of records) {
    const dtlId =
      num(
        record.WORK_RECOMMENDATION_DTL_ID
      );

    if (dtlId === null) {
      continue;
    }

    const workId =
      workMap.get(dtlId);

    if (!workId) {
      continue;
    }

    recommendations.push({
      work_id:
        workId,

      recommendation_date:
        parseDate(
          record.RECOMMENDATION_DATE
        ),

      recommended_amount:
        num(
          record.RECOMMENDED_AMOUNT
        ),

      sanction_date:
        parseDate(
          record.SANCTION_DATE
        ),

      letter_no:
        text(record.LETTER_NO),

      flag:
        num(record.FLAG),
    });

    /*
     * Your recommended JSON contains
     * sanction fields too.
     */
    if (
      record.SANCTION_DATE !== undefined ||
      record.SANCTION_AMOUNT !== undefined ||
      record.WORK_STAGE !== undefined ||
      record.FILE_STATUS !== undefined ||
      record.ATTACH_ID !== undefined
    ) {
      sanctions.push({
        work_id:
          workId,

        recommended_date:
          parseDate(
            record.RECOMMENDATION_DATE
          ),

        sanction_date:
          parseDate(
            record.SANCTION_DATE
          ),

        sanction_amount:
          num(
            record.SANCTION_AMOUNT
          ),

        work_stage:
          text(record.WORK_STAGE),

        file_status:
          record.FILE_STATUS === undefined
            ? null
            : Boolean(record.FILE_STATUS),

        attach_id:
          num(record.ATTACH_ID),
      });
    }
  }

  await upsertBatches(
    "work_recommendations",
    recommendations,
    "work_id"
  );

  if (sanctions.length > 0) {
    await upsertBatches(
      "work_sanctions",
      sanctions,
      "work_id"
    );
  }

  return {
    dataset: "works_recommended",
    records: records.length,
    works: works.length,
    recommendations:
      recommendations.length,
    sanctions:
      sanctions.length,
    states:
      states.size,
  };
}

// ============================================================
// SANCTIONED
// ============================================================

async function ingestSanctioned(
  records: Record<string, unknown>[]
) {
  /*
   * The sanctioned data uses the same
   * WORK_RECOMMENDATION_DTL_ID identity.
   */

  const states =
    await resolveStates(records);

  const constituencies =
    await resolveConstituencies(
      records,
      states
    );

  const mps =
    await resolveMps(
      records,
      states,
      constituencies
    );

  const works = [];

  for (const record of records) {
    const dtlId =
      num(
        record.WORK_RECOMMENDATION_DTL_ID
      );

    if (dtlId === null) {
      continue;
    }

    const stateName =
      text(record.STATE_NAME);

    const constituencyName =
      text(record.CONSTITUENCY);

    let constituencyId:
      number | null = null;

    if (
      stateName &&
      constituencyName
    ) {
      const stateId =
        states.get(
          stateName.toLowerCase()
        );

      if (stateId) {
        constituencyId =
          constituencies.get(
            `${stateId}|${constituencyName.toLowerCase()}`
          ) ?? null;
      }
    }

    const mpName =
      text(record.MP_NAME);

    const mpId =
      mpName
        ? mps.get(
            memberKey(mpName, constituencyId)
          ) ?? null
        : null;

    works.push({
      work_recommendation_dtl_id:
        dtlId,

      source_work_id:
        text(record.ACTIVITY_NAME),

      mp_id:
        mpId,

      constituency_id:
        constituencyId,

      work_category:
        text(record.WORK_CATEGORY),

      activity_name:
        text(record.ACTIVITY_NAME),

      work_description:
        text(record.WORK_DESCRIPTION),

      updated_at:
        new Date().toISOString(),
    });
  }

  if (works.length > 0) {
    await upsertBatches(
      "works",
      works,
      "work_recommendation_dtl_id"
    );
  }

  const workMap =
    await getWorkMap(
      works.map(
        w =>
          Number(
            w.work_recommendation_dtl_id
          )
      )
    );

  const sanctions = [];

  for (const record of records) {
    const dtlId =
      num(
        record.WORK_RECOMMENDATION_DTL_ID
      );

    if (dtlId === null) {
      continue;
    }

    const workId =
      workMap.get(dtlId);

    if (!workId) {
      continue;
    }

    sanctions.push({
      work_id:
        workId,

      recommended_date:
        parseDate(
          record.RECOMMENDATION_DATE
        ),

      sanction_date:
        parseDate(
          record.SANCTION_DATE
        ),

      sanction_amount:
        num(
          record.SANCTION_AMOUNT
        ),

      work_stage:
        text(record.WORK_STAGE),

      file_status:
        record.FILE_STATUS === undefined
          ? null
          : Boolean(record.FILE_STATUS),

      attach_id:
        num(record.ATTACH_ID),
    });
  }

  if (sanctions.length > 0) {
    await upsertBatches(
      "work_sanctions",
      sanctions,
      "work_id"
    );
  }

  return {
    dataset: "works_sanctioned",
    records: records.length,
    works: works.length,
    sanctions:
      sanctions.length,
  };
}

// ============================================================
// COMPLETED
// ============================================================

async function ingestCompleted(
  records: Record<string, unknown>[]
) {
  const dtlIds = records
    .map(r =>
      num(
        r.WORK_RECOMMENDATION_DTL_ID
      )
    )
    .filter(
      (x): x is number =>
        x !== null
    );

  const workMap =
    await getWorkMap(dtlIds);

  const completions = [];

  let linked = 0;

  for (const record of records) {
    const dtlId =
      num(
        record.WORK_RECOMMENDATION_DTL_ID
      );

    if (dtlId === null) {
      continue;
    }

    const workId =
      workMap.get(dtlId);

    if (!workId) {
      throw new Error(
        `Unmatched work for WORK_RECOMMENDATION_DTL_ID=${dtlId}`
      );
    }

    linked++;

    completions.push({
      work_id:
        workId,

      completion_date:
        parseDate(
          record.ACTUAL_END_DATE
        ),

      amount_disbursed:
        num(
          record.ACTUAL_AMOUNT
        ),

      image: null,
    });
  }

  if (completions.length > 0) {
    await upsertBatches(
      "work_completions",
      completions,
      "work_id"
    );
  }

  return {
    dataset: "works_completed",
    records: records.length,
    completed:
      completions.length,
    linked,
    unmatched:
      records.length - linked,
  };
}

// ============================================================
// EXPENDITURE
// ============================================================

async function ingestExpenditure(
  records: Record<string, unknown>[]
) {
  /*
   * Resolve states.
   */
  const states =
    await resolveStates(records);

  /*
   * Resolve constituencies.
   */
  const constituencies =
    await resolveConstituencies(
      records,
      states
    );

  /*
   * Resolve works using
   * WORK_RECOMMENDATION_DTL_ID.
   */
  const dtlIds = records
    .map(r =>
      num(
        r.WORK_RECOMMENDATION_DTL_ID
      )
    )
    .filter(
      (x): x is number =>
        x !== null
    );

  const workMap =
    await getWorkMap(dtlIds);

  /*
   * Resolve MPs.
   */
  const mps =
    await resolveMps(
      records,
      states,
      constituencies
    );

  const expenditures: Record<
    string,
    unknown
  >[] = [];

  let linked = 0;

  for (const record of records) {

    // --------------------------------------------------------
    // WORK
    // --------------------------------------------------------

    const dtlId =
      num(
        record.WORK_RECOMMENDATION_DTL_ID
      );

    let workId:
      number | null = null;

    if (dtlId !== null) {
      workId =
        workMap.get(dtlId) ?? null;
    }

    if (
      dtlId !== null &&
      workId === null
    ) {
      throw new Error(
        `Unmatched work for expenditure WORK_RECOMMENDATION_DTL_ID=${dtlId}`
      );
    }


    // --------------------------------------------------------
    // STATE
    // --------------------------------------------------------

    const stateName =
      text(record.STATE_NAME);


    // --------------------------------------------------------
    // CONSTITUENCY
    // --------------------------------------------------------

    const constituencyName =
      text(record.CONSTITUENCY);

    let constituencyId:
      number | null = null;

    if (
      stateName &&
      constituencyName
    ) {

      const stateId =
        states.get(
          stateName.toLowerCase()
        );

      if (stateId) {

        constituencyId =
          constituencies.get(
            `${stateId}|${constituencyName.toLowerCase()}`
          ) ?? null;
      }
    }


    // --------------------------------------------------------
    // MP
    // --------------------------------------------------------

    const mpName =
      text(record.MP_NAME);

    const mpId =
      mpName
        ? mps.get(
            memberKey(mpName, constituencyId)
          ) ?? null
        : null;


    if (workId !== null) {
      linked++;
    }


    // --------------------------------------------------------
    // BUILD DATABASE ROW
    // --------------------------------------------------------

    const expenditure = {
      work_id:
        workId,

      source_work_id:
        text(record.WORK_ID),

      expenditure_date:
        parseDate(
          record.EXPENDITURE_DATE
        ),

      vendor_name:
        text(record.VENDOR_NAME),

      payment_status:
        text(record.WORK_STATUS),

      fund_disbursed_amount:
        num(
          record.FUND_DISBURSED_AMT
        ),

      state_id:
        stateName
          ? states.get(
              stateName.toLowerCase()
            ) ?? null
          : null,

      constituency_id:
        constituencyId,

      mp_id:
        mpId,

      /*
       * Fingerprint uses the raw source
       * record, not the resolved DB row.
       */
      source_record_key:
        "",
    };


    // --------------------------------------------------------
    // CREATE DETERMINISTIC IDENTITY
    // FROM RAW SOURCE VALUES
    // --------------------------------------------------------

    expenditure.source_record_key =
      await expenditureFingerprint(
        record
      );


    expenditures.push(
      expenditure
    );
  }


  // ----------------------------------------------------------
  // UPSERT
  // ----------------------------------------------------------

  if (
    expenditures.length > 0
  ) {

    /*
     * IMPORTANT:
     *
     * source_record_key is now the
     * unique identity of an expenditure.
     *
     * Same expenditure:
     *     -> UPDATE
     *
     * New expenditure:
     *     -> INSERT
     */
    await upsertBatches(
      "work_expenditures",
      expenditures,
      "source_record_key"
    );
  }


  return {
    dataset:
      "expenditure",

    records:
      records.length,

    expenditures:
      expenditures.length,

    linked,

    unmatched:
      records.length - linked,
  };
}

// ============================================================
// ALLOCATED LIMIT
// ============================================================

async function ingestAllocated(
  records: Record<string, unknown>[]
) {
  const states =
    await resolveStates(records);

  const constituencies =
    await resolveConstituencies(
      records,
      states
    );

  const mps =
    await resolveMps(
      records,
      states,
      constituencies
    );

  const allocations = [];

  for (const record of records) {
    const mpName =
      text(record.MP_NAME);

    if (!mpName) {
      continue;
    }

    const stateName =
      text(record.STATE_NAME);

    const constituencyName =
      text(record.CONSTITUENCY);

    let constituencyId:
      number | null = null;

    if (
      stateName &&
      constituencyName
    ) {
      const stateId =
        states.get(
          stateName.toLowerCase()
        );

      if (stateId) {
        constituencyId =
          constituencies.get(
            `${stateId}|${constituencyName.toLowerCase()}`
          ) ?? null;
      }
    }

    const mpId =
      mps.get(
        memberKey(mpName, constituencyId)
      ) ?? null;

    if (!mpId) {
      continue;
    }

    allocations.push({
      mp_id:
        mpId,

      allocated_amount:
        num(
          record.ALLOCATED_AMT
        ),

      source_sno:
        num(record.Sno),

      source_tenure:
        text(record.TENURE),

      source_house_of_parliament:
        num(
          record.HOUSE_OF_PARLIAMENT
        ),

      source_tenure_start_date:
        parseDate(
          record.TENURE_START_DATE
        ),

      source_tenure_end_date:
        parseDate(
          record.TENURE_END_DATE
        ),
    });
  }

  if (allocations.length > 0) {
    await upsertBatches(
      "mp_allocations",
      allocations,
      "mp_id"
    );
  }

  return {
    dataset: "allocated_limit",
    records: records.length,
    allocations:
      allocations.length,
  };
}

// ============================================================
// CALAMITY
// ============================================================

async function ingestCalamity(
  records: Record<string, unknown>[]
) {
  /*
   * Calamity data has no STATE_NAME
   * or constituency.
   *
   * MP_NAME is resolved against the
   * existing MPs table.
   */

  const {
    data: existingMps,
    error,
  } =
    await supabase
      .from("mps")
      .select(
        "mp_id,mp_name"
      );

  if (error) {
    throw new Error(
      `MP lookup failed: ${JSON.stringify(error)}`
    );
  }


  // ----------------------------------------------------------
  // BUILD MP LOOKUP
  // ----------------------------------------------------------

  const mpMap =
    new Map<string, number>();

  for (
    const row of existingMps ?? []
  ) {

    mpMap.set(
      String(row.mp_name)
        .trim()
        .toLowerCase(),

      Number(
        row.mp_id
      )
    );
  }


  // ----------------------------------------------------------
  // BUILD CALAMITIES
  // ----------------------------------------------------------

  const calamities: Record<
    string,
    unknown
  >[] = [];

  let linked = 0;

for (
  const record of records
) {

  const mpName =
    text(record.MP_NAME);

  const calamityType =
    text(record.TYPE);

  const calamityName =
    text(record.CALAMITY_NAME);

  const consentDate =
    parseDate(record.CRT_DT);

  const consentAmount =
    num(record.CONSENTED_AMOUNT);

  const sourceSno =
    num(record.Sno);

  /*
   * Skip completely empty source records.
   *
   * These contain no meaningful calamity information
   * and should not create a database row.
   */
  if (
    !mpName &&
    !calamityType &&
    !calamityName &&
    !consentDate &&
    consentAmount === null &&
    sourceSno === null
  ) {
    continue;
  }

  const mpId =
    mpName
      ? mpMap.get(
          mpName.toLowerCase()
        ) ?? null
      : null;

  if (
    mpId !== null
  ) {
    linked++;
  }

  calamities.push({
    mp_id:
      mpId,

    calamity_type:
      calamityType,

    calamity_name:
      calamityName,

    consent_date:
      consentDate,

    consent_amount:
      consentAmount,

    source_sno:
      sourceSno,
  });
}
  // ----------------------------------------------------------
  // UPSERT
  // ----------------------------------------------------------

  if (
    calamities.length > 0
  ) {

    /*
     * Calamity identity:
     *
     *     mp_id
     *     +
     *     consent_date
     *     +
     *     calamity_name
     *
     * Same calamity:
     *     -> UPDATE
     *
     * New calamity:
     *     -> INSERT
     */
    await upsertBatches(
      "calamities",
      calamities,
      "mp_id,consent_date,calamity_name"
    );
  }


  return {
    dataset:
      "calamity",

    records:
      records.length,

    calamities:
      calamities.length,

    linked_mps:
      linked,
  };
}

// ============================================================
// MLA RESOLUTION
// ============================================================

async function resolveMlas(
  records: Record<string, unknown>[],
  states: Map<string, number>,
  constituencies: Map<string, number>
) {
  const result = new Map<string, number>();

  const { data, error } =
    await supabase
      .from("mlas")
      .select(
        "mla_id,mla_name,constituency_id"
      );

  if (error) {
    throw new Error(
      `MLA lookup failed: ${JSON.stringify(error)}`
    );
  }

  for (const row of data ?? []) {
    const key = memberKey(
      row.mla_name,
      row.constituency_id
    );

    result.set(
      key,
      Number(row.mla_id)
    );
  }

  const newMlas: Record<string, unknown>[] = [];

  const pendingKeys = new Set<string>();

  for (const record of records) {
    const mlaName =
      text(record.MP_NAME);

    const stateName =
      text(record.STATE_NAME);

    const constituencyName =
      text(record.CONSTITUENCY);

    if (!mlaName) {
      continue;
    }

    let constituencyId:
      number | null = null;

    if (
      stateName &&
      constituencyName
    ) {
      const stateId =
        states.get(
          stateName.toLowerCase()
        );

      if (stateId) {
        constituencyId =
          constituencies.get(
            `${stateId}|${constituencyName.toLowerCase()}`
          ) ?? null;
      }
    }

    const key = memberKey(
      mlaName,
      constituencyId
    );

    if (
      !result.has(key) &&
      !pendingKeys.has(key)
    ) {
      pendingKeys.add(key);

      newMlas.push({
        mla_name: mlaName,

        house_name:
          text(record.HOUSE_NAME),

        constituency_id:
          constituencyId,

        tenure:
          text(record.TENURE),

        tenure_start_date:
          parseDate(
            record.TENURE_START_DATE
          ),

        tenure_end_date:
          parseDate(
            record.TENURE_END_DATE
          ),
      });
    }
  }

  for (
    let i = 0;
    i < newMlas.length;
    i += DB_BATCH_SIZE
  ) {
    const batch =
      newMlas.slice(
        i,
        i + DB_BATCH_SIZE
      );

    const { data: inserted, error } =
      await supabase
        .from("mlas")
        .insert(batch)
        .select();

    if (error) {
      throw new Error(
        `MLA insert failed: ${JSON.stringify(error)}`
      );
    }

    for (const row of inserted ?? []) {
      const key = memberKey(
        row.mla_name,
        row.constituency_id
      );

      result.set(
        key,
        Number(row.mla_id)
      );
    }
  }

  return result;
}

// ============================================================
// MLA ALLOCATED LIMIT
// ============================================================

async function ingestMlaAllocated(
  records: Record<string, unknown>[]
) {
  const states =
    await resolveStates(records);

  const constituencies =
    await resolveConstituencies(
      records,
      states
    );

  const mlas =
    await resolveMlas(
      records,
      states,
      constituencies
    );

  const allocations = [];

  for (const record of records) {
    const mlaName =
      text(record.MP_NAME);

    if (!mlaName) {
      continue;
    }

    const stateName =
      text(record.STATE_NAME);

    const constituencyName =
      text(record.CONSTITUENCY);

    let constituencyId:
      number | null = null;

    if (
      stateName &&
      constituencyName
    ) {
      const stateId =
        states.get(
          stateName.toLowerCase()
        );

      if (stateId) {
        constituencyId =
          constituencies.get(
            `${stateId}|${constituencyName.toLowerCase()}`
          ) ?? null;
      }
    }

    const mlaId =
      mlas.get(
        memberKey(mlaName, constituencyId)
      ) ?? null;

    if (!mlaId) {
      continue;
    }

    allocations.push({
      mla_id:
        mlaId,

      allocated_amount:
        num(
          record.ALLOCATED_AMT
        ),

      source_sno:
        num(record.Sno),

      source_tenure:
        text(record.TENURE),

      source_house_of_parliament:
        num(
          record.HOUSE_OF_PARLIAMENT
        ),

      source_tenure_start_date:
        parseDate(
          record.TENURE_START_DATE
        ),

      source_tenure_end_date:
        parseDate(
          record.TENURE_END_DATE
        ),
    });
  }

  if (allocations.length > 0) {
    await upsertBatches(
      "mla_allocations",
      allocations,
      "mla_id"
    );
  }

  return {
    dataset: "mla_allocated_limit",
    records: records.length,
    allocations:
      allocations.length,
  };
}

// ============================================================
// MLA RECOMMENDED
// ============================================================

async function ingestMlaRecommended(
  records: Record<string, unknown>[]
) {
  const states =
    await resolveStates(records);

  const constituencies =
    await resolveConstituencies(
      records,
      states
    );

  const mlas =
    await resolveMlas(
      records,
      states,
      constituencies
    );

  const works = [];

  for (const record of records) {
    const dtlId =
      num(
        record.WORK_RECOMMENDATION_DTL_ID
      );

    if (dtlId === null) {
      continue;
    }

    const stateName =
      text(record.STATE_NAME);

    const constituencyName =
      text(record.CONSTITUENCY);

    let constituencyId:
      number | null = null;

    if (
      stateName &&
      constituencyName
    ) {
      const stateId =
        states.get(
          stateName.toLowerCase()
        );

      if (stateId) {
        constituencyId =
          constituencies.get(
            `${stateId}|${constituencyName.toLowerCase()}`
          ) ?? null;
      }
    }

    const mlaName =
      text(record.MP_NAME);

    let mlaId:
      number | null = null;

    if (mlaName) {
      mlaId =
        mlas.get(
          memberKey(mlaName, constituencyId)
        ) ?? null;
    }

    works.push({
      work_recommendation_dtl_id:
        dtlId,

      source_work_id:
        text(record.ACTIVITY_NAME),

      mla_id:
        mlaId,

      constituency_id:
        constituencyId,

      work_category:
        text(record.WORK_CATEGORY),

      activity_name:
        text(record.ACTIVITY_NAME),

      work_description:
        text(record.WORK_DESCRIPTION),

      updated_at:
        new Date().toISOString(),
    });
  }

  await upsertBatches(
    "mla_works",
    works,
    "work_recommendation_dtl_id,mla_id"
  );

  const workMap =
    await getMlaWorkMap(
      works.map(
        w =>
          Number(
            w.work_recommendation_dtl_id
          )
      )
    );

  const recommendations = [];
  const sanctions = [];

  for (const record of records) {
    const dtlId =
      num(
        record.WORK_RECOMMENDATION_DTL_ID
      );

    if (dtlId === null) {
      continue;
    }

    const workId =
      workMap.get(dtlId);

    if (!workId) {
      continue;
    }

    recommendations.push({
      work_id:
        workId,

      recommendation_date:
        parseDate(
          record.RECOMMENDATION_DATE
        ),

      recommended_amount:
        num(
          record.RECOMMENDED_AMOUNT
        ),

      sanction_date:
        parseDate(
          record.SANCTION_DATE
        ),

      letter_no:
        text(record.LETTER_NO),

      flag:
        num(record.FLAG),
    });

    if (
      record.SANCTION_DATE !== undefined ||
      record.SANCTION_AMOUNT !== undefined ||
      record.WORK_STAGE !== undefined ||
      record.FILE_STATUS !== undefined ||
      record.ATTACH_ID !== undefined
    ) {
      sanctions.push({
        work_id:
          workId,

        recommended_date:
          parseDate(
            record.RECOMMENDATION_DATE
          ),

        sanction_date:
          parseDate(
            record.SANCTION_DATE
          ),

        sanction_amount:
          num(
            record.SANCTION_AMOUNT
          ),

        work_stage:
          text(record.WORK_STAGE),

        file_status:
          record.FILE_STATUS === undefined
            ? null
            : Boolean(record.FILE_STATUS),

        attach_id:
          num(record.ATTACH_ID),
      });
    }
  }

  await upsertBatches(
    "mla_work_recommendations",
    recommendations,
    "work_id"
  );

  if (sanctions.length > 0) {
    await upsertBatches(
      "mla_work_sanctions",
      sanctions,
      "work_id"
    );
  }

  return {
    dataset: "mla_works_recommended",
    records: records.length,
    works: works.length,
    recommendations:
      recommendations.length,
    sanctions:
      sanctions.length,
    states:
      states.size,
  };
}

// ============================================================
// GET MLA WORK IDS
// ============================================================

async function getMlaWorkMap(
  dtlIds: number[]
) {
  const result = new Map<number, number>();

  const uniqueIds = [
    ...new Set(dtlIds),
  ];

  for (
    let i = 0;
    i < uniqueIds.length;
    i += 500
  ) {
    const batch =
      uniqueIds.slice(
        i,
        i + 500
      );

    const { data, error } =
      await supabase
        .from("mla_works")
        .select(
          "work_id,work_recommendation_dtl_id"
        )
        .in(
          "work_recommendation_dtl_id",
          batch
        );

    if (error) {
      throw new Error(
        `MLA work lookup failed: ${JSON.stringify(error)}`
      );
    }

    for (const row of data ?? []) {
      result.set(
        Number(
          row.work_recommendation_dtl_id
        ),
        Number(row.work_id)
      );
    }
  }

  return result;
}

// ============================================================
// MLA SANCTIONED
// ============================================================

async function ingestMlaSanctioned(
  records: Record<string, unknown>[]
) {
  const states =
    await resolveStates(records);

  const constituencies =
    await resolveConstituencies(
      records,
      states
    );

  const mlas =
    await resolveMlas(
      records,
      states,
      constituencies
    );

  const works = [];

  for (const record of records) {
    const dtlId =
      num(
        record.WORK_RECOMMENDATION_DTL_ID
      );

    if (dtlId === null) {
      continue;
    }

    const stateName =
      text(record.STATE_NAME);

    const constituencyName =
      text(record.CONSTITUENCY);

    let constituencyId:
      number | null = null;

    if (
      stateName &&
      constituencyName
    ) {
      const stateId =
        states.get(
          stateName.toLowerCase()
        );

      if (stateId) {
        constituencyId =
          constituencies.get(
            `${stateId}|${constituencyName.toLowerCase()}`
          ) ?? null;
      }
    }

    const mlaName =
      text(record.MP_NAME);

    const mlaId =
      mlaName
        ? mlas.get(
            memberKey(mlaName, constituencyId)
          ) ?? null
        : null;

    works.push({
      work_recommendation_dtl_id:
        dtlId,

      source_work_id:
        text(record.ACTIVITY_NAME),

      mla_id:
        mlaId,

      constituency_id:
        constituencyId,

      work_category:
        text(record.WORK_CATEGORY),

      activity_name:
        text(record.ACTIVITY_NAME),

      work_description:
        text(record.WORK_DESCRIPTION),

      updated_at:
        new Date().toISOString(),
    });
  }

  if (works.length > 0) {
    await upsertBatches(
      "mla_works",
      works,
      "work_recommendation_dtl_id,mla_id"
    );
  }

  const workMap =
    await getMlaWorkMap(
      works.map(
        w =>
          Number(
            w.work_recommendation_dtl_id
          )
      )
    );

  const sanctions = [];

  for (const record of records) {
    const dtlId =
      num(
        record.WORK_RECOMMENDATION_DTL_ID
      );

    if (dtlId === null) {
      continue;
    }

    const workId =
      workMap.get(dtlId);

    if (!workId) {
      continue;
    }

    sanctions.push({
      work_id:
        workId,

      recommended_date:
        parseDate(
          record.RECOMMENDATION_DATE
        ),

      sanction_date:
        parseDate(
          record.SANCTION_DATE
        ),

      sanction_amount:
        num(
          record.SANCTION_AMOUNT
        ),

      work_stage:
        text(record.WORK_STAGE),

      file_status:
        record.FILE_STATUS === undefined
          ? null
          : Boolean(record.FILE_STATUS),

      attach_id:
        num(record.ATTACH_ID),
    });
  }

  if (sanctions.length > 0) {
    await upsertBatches(
      "mla_work_sanctions",
      sanctions,
      "work_id"
    );
  }

  return {
    dataset: "mla_works_sanctioned",
    records: records.length,
    works: works.length,
    sanctions:
      sanctions.length,
  };
}

// ============================================================
// MLA COMPLETED
// ============================================================

async function ingestMlaCompleted(
  records: Record<string, unknown>[]
) {
  const dtlIds = records
    .map(r =>
      num(
        r.WORK_RECOMMENDATION_DTL_ID
      )
    )
    .filter(
      (x): x is number =>
        x !== null
    );

  const workMap =
    await getMlaWorkMap(dtlIds);

  const completions = [];

  let linked = 0;

  for (const record of records) {
    const dtlId =
      num(
        record.WORK_RECOMMENDATION_DTL_ID
      );

    if (dtlId === null) {
      continue;
    }

    const workId =
      workMap.get(dtlId);

    if (!workId) {
      throw new Error(
        `Unmatched MLA work for WORK_RECOMMENDATION_DTL_ID=${dtlId}`
      );
    }

    linked++;

    completions.push({
      work_id:
        workId,

      completion_date:
        parseDate(
          record.ACTUAL_END_DATE
        ),

      amount_disbursed:
        num(
          record.ACTUAL_AMOUNT
        ),

      image: null,
    });
  }

  if (completions.length > 0) {
    await upsertBatches(
      "mla_work_completions",
      completions,
      "work_id"
    );
  }

  return {
    dataset: "mla_works_completed",
    records: records.length,
    completed:
      completions.length,
    linked,
    unmatched:
      records.length - linked,
  };
}

// ============================================================
// MLA EXPENDITURE
// ============================================================

async function ingestMlaExpenditure(
  records: Record<string, unknown>[]
) {
  const states =
    await resolveStates(records);

  const constituencies =
    await resolveConstituencies(
      records,
      states
    );

  const dtlIds = records
    .map(r =>
      num(
        r.WORK_RECOMMENDATION_DTL_ID
      )
    )
    .filter(
      (x): x is number =>
        x !== null
    );

  const workMap =
    await getMlaWorkMap(dtlIds);

  const mlas =
    await resolveMlas(
      records,
      states,
      constituencies
    );

  const expenditures: Record<
    string,
    unknown
  >[] = [];

  let linked = 0;

  for (const record of records) {

    const dtlId =
      num(
        record.WORK_RECOMMENDATION_DTL_ID
      );

    let workId:
      number | null = null;

    if (dtlId !== null) {
      workId =
        workMap.get(dtlId) ?? null;
    }

    if (
      dtlId !== null &&
      workId === null
    ) {
      throw new Error(
        `Unmatched MLA work for expenditure WORK_RECOMMENDATION_DTL_ID=${dtlId}`
      );
    }

    const stateName =
      text(record.STATE_NAME);

    const constituencyName =
      text(record.CONSTITUENCY);

    let constituencyId:
      number | null = null;

    if (
      stateName &&
      constituencyName
    ) {

      const stateId =
        states.get(
          stateName.toLowerCase()
        );

      if (stateId) {

        constituencyId =
          constituencies.get(
            `${stateId}|${constituencyName.toLowerCase()}`
          ) ?? null;
      }
    }

    const mlaName =
      text(record.MP_NAME);

    const mlaId =
      mlaName
        ? mlas.get(
            memberKey(mlaName, constituencyId)
          ) ?? null
        : null;

    if (workId !== null) {
      linked++;
    }

    const expenditure = {
      work_id:
        workId,

      source_work_id:
        text(record.WORK_ID),

      expenditure_date:
        parseDate(
          record.EXPENDITURE_DATE
        ),

      vendor_name:
        text(record.VENDOR_NAME),

      payment_status:
        text(record.WORK_STATUS),

      fund_disbursed_amount:
        num(
          record.FUND_DISBURSED_AMT
        ),

      state_id:
        stateName
          ? states.get(
              stateName.toLowerCase()
            ) ?? null
          : null,

      constituency_id:
        constituencyId,

      mla_id:
        mlaId,

      source_record_key:
        "",
    };

    expenditure.source_record_key =
      await expenditureFingerprint(
        record
      );

    expenditures.push(
      expenditure
    );
  }

  if (
    expenditures.length > 0
  ) {
    await upsertBatches(
      "mla_work_expenditures",
      expenditures,
      "source_record_key"
    );
  }

  return {
    dataset:
      "mla_expenditure",

    records:
      records.length,

    expenditures:
      expenditures.length,

    linked,

    unmatched:
      records.length - linked,
  };
}

// ============================================================
// MLA CALAMITY
// ============================================================

async function ingestMlaCalamity(
  records: Record<string, unknown>[]
) {
  const {
    data: existingMlas,
    error,
  } =
    await supabase
      .from("mlas")
      .select(
        "mla_id,mla_name"
      );

  if (error) {
    throw new Error(
      `MLA lookup failed: ${JSON.stringify(error)}`
    );
  }

  const mlaMap =
    new Map<string, number>();

  for (
    const row of existingMlas ?? []
  ) {

    mlaMap.set(
      String(row.mla_name)
        .trim()
        .toLowerCase(),

      Number(
        row.mla_id
      )
    );
  }

  const calamities: Record<
    string,
    unknown
  >[] = [];

  let linked = 0;

for (
  const record of records
) {

  const mlaName =
    text(record.MP_NAME);

  const calamityType =
    text(record.TYPE);

  const calamityName =
    text(record.CALAMITY_NAME);

  const consentDate =
    parseDate(record.CRT_DT);

  const consentAmount =
    num(record.CONSENTED_AMOUNT);

  const sourceSno =
    num(record.Sno);

  if (
    !mlaName &&
    !calamityType &&
    !calamityName &&
    !consentDate &&
    consentAmount === null &&
    sourceSno === null
  ) {
    continue;
  }

  const mlaId =
    mlaName
      ? mlaMap.get(
          mlaName.toLowerCase()
        ) ?? null
      : null;

  if (
    mlaId !== null
  ) {
    linked++;
  }

  calamities.push({
    mla_id:
      mlaId,

    calamity_type:
      calamityType,

    calamity_name:
      calamityName,

    consent_date:
      consentDate,

    consent_amount:
      consentAmount,

    source_sno:
      sourceSno,
  });
}

  if (
    calamities.length > 0
  ) {
    await upsertBatches(
      "mla_calamities",
      calamities,
      "mla_id,consent_date,calamity_name"
    );
  }

  return {
    dataset:
      "mla_calamity",

    records:
      records.length,

    calamities:
      calamities.length,

    linked_mlas:
      linked,
  };
}

// ============================================================
// MAIN EDGE FUNCTION
// ============================================================

serve(async (req) => {
  try {
    if (req.method !== "POST") {
      return new Response(
        JSON.stringify({
          success: false,
          error:
            "POST request required.",
        }),
        {
          status: 405,
          headers: {
            "Content-Type":
              "application/json",
          },
        }
      );
    }

    const body =
      await req.json();

    const path =
      body?.path;

    if (
      typeof path !== "string" ||
      path.trim() === ""
    ) {
      return new Response(
        JSON.stringify({
          success: false,
          error:
            "Missing Storage path.",
        }),
        {
          status: 400,
          headers: {
            "Content-Type":
              "application/json",
          },
        }
      );
    }

    console.log(
      `Starting ingestion: ${path}`
    );

    const dataset =
      datasetFromPath(path);

    console.log(
      `Dataset: ${dataset}`
    );

    // --------------------------------------------------------
    // Download ONE NDJSON file
    // --------------------------------------------------------

    const {
      data: file,
      error: downloadError,
    } =
      await supabase.storage
        .from(BUCKET)
        .download(path);

    if (
      downloadError ||
      !file
    ) {
      throw new Error(
        `Storage download failed: ${JSON.stringify(
          downloadError
        )}`
      );
    }

    console.log(
      `Downloaded ${file.size} bytes`
    );

    // --------------------------------------------------------
    // Parse NDJSON
    // --------------------------------------------------------

    const content =
      await file.text();

    const lines =
      content
        .split(/\r?\n/)
        .map(line => line.trim())
        .filter(Boolean);

    const parsedRecords:
      Record<string, unknown>[] = [];

    for (
      let i = 0;
      i < lines.length;
      i++
    ) {
      try {
        parsedRecords.push(
          JSON.parse(lines[i])
        );
      } catch (error) {
        throw new Error(
          `Invalid JSON on line ${
            i + 1
          }: ${JSON.stringify(
            errorDetails(error)
          )}`
        );
      }
    }

    console.log(
      `Parsed ${parsedRecords.length} records`
    );

    // --------------------------------------------------------
    // Filter government summary rows
    // (e.g. {"Total_Amt": ...})
    // --------------------------------------------------------

    const records =
      parsedRecords.filter(
        record =>
          !(
            "Total_Amt" in record
          )
      );

    if (
      records.length !==
      parsedRecords.length
    ) {
      console.log(
        `Filtered ${
          parsedRecords.length -
          records.length
        } summary row(s)`
      );
    }

    // --------------------------------------------------------
    // Dataset dispatcher
    // --------------------------------------------------------

    let result;

    switch (dataset) {
      case "works_recommended":
        result =
          await ingestRecommended(
            records
          );
        break;

      case "works_sanctioned":
        result =
          await ingestSanctioned(
            records
          );
        break;

      case "works_completed":
        result =
          await ingestCompleted(
            records
          );
        break;

      case "expenditure":
        result =
          await ingestExpenditure(
            records
          );
        break;

      case "allocated_limit":
        result =
          await ingestAllocated(
            records
          );
        break;

      case "calamity":
        result =
          await ingestCalamity(
            records
          );
        break;

      case "mla_works_recommended":
        result =
          await ingestMlaRecommended(
            records
          );
        break;

      case "mla_works_sanctioned":
        result =
          await ingestMlaSanctioned(
            records
          );
        break;

      case "mla_works_completed":
        result =
          await ingestMlaCompleted(
            records
          );
        break;

      case "mla_expenditure":
        result =
          await ingestMlaExpenditure(
            records
          );
        break;

      case "mla_allocated_limit":
        result =
          await ingestMlaAllocated(
            records
          );
        break;

      case "mla_calamity":
        result =
          await ingestMlaCalamity(
            records
          );
        break;

      default:
        throw new Error(
          `Unknown dataset folder: ${dataset}`
        );
    }

    console.log(
      "INGESTION COMPLETE",
      result
    );

    return new Response(
      JSON.stringify(
        {
          success: true,
          path,
          dataset,
          ...result,
        },
        null,
        2
      ),
      {
        status: 200,
        headers: {
          "Content-Type":
            "application/json",
        },
      }
    );

  } catch (error) {
    console.error(
      "INGESTION ERROR:",
      error
    );

    return new Response(
      JSON.stringify(
        {
          success: false,
          error:
            errorDetails(error),
        },
        null,
        2
      ),
      {
        status: 500,
        headers: {
          "Content-Type":
            "application/json",
        },
      }
    );
  }
});