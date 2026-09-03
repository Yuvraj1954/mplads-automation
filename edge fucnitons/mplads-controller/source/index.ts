import { serve } from "https://deno.land/std@0.224.0/http/server.ts";
import { createClient } from "https://esm.sh/@supabase/supabase-js@2";

const SUPABASE_URL = Deno.env.get("SUPABASE_URL")!;
const SERVICE_ROLE_KEY =
  Deno.env.get("SUPABASE_SERVICE_ROLE_KEY")!;

const supabase = createClient(
  SUPABASE_URL,
  SERVICE_ROLE_KEY
);

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

const DATASET_ORDER = [
  ...MP_DATASETS,
  ...MLA_DATASETS,
];

serve(async (req) => {
  try {
    if (req.method !== "POST") {
      return new Response(
        JSON.stringify({
          success: false,
          error: "POST required",
        }),
        {
          status: 405,
          headers: {
            "Content-Type": "application/json",
          },
        }
      );
    }

    const body = await req.json();

    const runId =
      body?.run_id ||
      body?.timestamp;

    if (!runId) {
      return new Response(
        JSON.stringify({
          success: false,
          error:
            "Missing run_id/timestamp",
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
      `Creating ingestion jobs for ${runId}`
    );

    let totalJobs = 0;

    const datasetResults = [];

    for (const dataset of DATASET_ORDER) {
      const prefix =
        `${runId}/${dataset}`;

      let offset = 0;

      const files: string[] = [];

      while (true) {
        const {
          data,
          error,
        } =
          await supabase.storage
            .from(BUCKET)
            .list(prefix, {
              limit: 100,
              offset,
              sortBy: {
                column: "name",
                order: "asc",
              },
            });

        if (error) {
          throw new Error(
            `Storage listing failed for ${dataset}: ${JSON.stringify(error)}`
          );
        }

        if (!data || data.length === 0) {
          break;
        }

        for (const item of data) {
          if (
            item.name.endsWith(
              ".ndjson"
            )
          ) {
            files.push(item.name);
          }
        }

        if (data.length < 100) {
          break;
        }

        offset += 100;
      }

      const jobs = files
        .sort()
        .map((file, index) => ({
          run_id: runId,
          dataset,
          file_path:
            `${prefix}/${file}`,
          part_number: index + 1,
          status: "pending",
        }));

      if (jobs.length > 0) {
        const {
          error,
        } =
          await supabase
            .from("ingestion_jobs")
            .upsert(
              jobs,
              {
                onConflict:
                  "run_id,file_path",
                ignoreDuplicates: true,
              }
            );

        if (error) {
          throw new Error(
            `Job creation failed for ${dataset}: ${JSON.stringify(error)}`
          );
        }
      }

      totalJobs += jobs.length;

      datasetResults.push({
        dataset,
        parts: jobs.length,
      });
    }

    return new Response(
      JSON.stringify(
        {
          success: true,
          run_id: runId,
          total_jobs: totalJobs,
          datasets:
            datasetResults,
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
    console.error(error);

    return new Response(
      JSON.stringify(
        {
          success: false,
          error:
            error instanceof Error
              ? error.message
              : String(error),
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