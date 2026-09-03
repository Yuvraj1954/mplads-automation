import { serve } from "https://deno.land/std@0.224.0/http/server.ts";
import { createClient } from "https://esm.sh/@supabase/supabase-js@2";

const SUPABASE_URL = Deno.env.get("SUPABASE_URL")!;
const SERVICE_ROLE_KEY =
  Deno.env.get("SUPABASE_SERVICE_ROLE_KEY")!;

const supabase = createClient(
  SUPABASE_URL,
  SERVICE_ROLE_KEY
);

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

function errorMessage(error: unknown): string {
  if (error instanceof Error) {
    return error.message;
  }

  return JSON.stringify(error);
}

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
      `Worker started for ${runId}`
    );

    // ========================================================
    // FIND THE FIRST DATASET THAT STILL HAS WORK
    // ========================================================

    let selectedJob: any = null;

    for (const dataset of DATASET_ORDER) {

      const { data, error } =
        await supabase
          .from("ingestion_jobs")
          .select("*")
          .eq("run_id", runId)
          .eq("dataset", dataset)
          .eq("status", "pending")
          .order("part_number", {
            ascending: true,
          })
          .limit(1)
          .maybeSingle();

      if (error) {
        throw new Error(
          `Job lookup failed: ${JSON.stringify(error)}`
        );
      }

      if (data) {
        selectedJob = data;
        break;
      }
    }

    // ========================================================
    // NOTHING LEFT
    // ========================================================

    if (!selectedJob) {

      const { count, error } =
        await supabase
          .from("ingestion_jobs")
          .select(
            "*",
            {
              count: "exact",
              head: true,
            }
          )
          .eq("run_id", runId)
          .eq("status", "completed");

      if (error) {
        throw new Error(
          `Completion count failed: ${JSON.stringify(error)}`
        );
      }

      return new Response(
        JSON.stringify({
          success: true,
          message:
            "All ingestion jobs completed.",
          run_id: runId,
          completed:
            count ?? 0,
        }),
        {
          status: 200,
          headers: {
            "Content-Type":
              "application/json",
          },
        }
      );
    }

    // ========================================================
    // CLAIM JOB
    // ========================================================

    const jobId =
      selectedJob.job_id;

    const attempts =
      Number(
        selectedJob.attempts ?? 0
      ) + 1;

    const { data: claimed, error: claimError } =
      await supabase
        .from("ingestion_jobs")
        .update({
          status: "processing",
          attempts,
          started_at:
            new Date().toISOString(),
          error_message: null,
        })
        .eq("job_id", jobId)
        .eq("status", "pending")
        .select()
        .maybeSingle();

    if (claimError) {
      throw new Error(
        `Failed claiming job: ${JSON.stringify(claimError)}`
      );
    }

    // Another worker may have claimed it.
    if (!claimed) {
      return new Response(
        JSON.stringify({
          success: true,
          message:
            "Job was already claimed. Try again.",
        }),
        {
          status: 200,
          headers: {
            "Content-Type":
              "application/json",
          },
        }
      );
    }

    console.log(
      `Processing job ${jobId}`
    );

    console.log(
      `Dataset: ${claimed.dataset}`
    );

    console.log(
      `Part: ${claimed.part_number}`
    );

    console.log(
      `File: ${claimed.file_path}`
    );

    // ========================================================
    // CALL THE EXISTING WORKING INGESTOR
    // ========================================================

    const { data: result, error: invokeError } =
      await supabase.functions.invoke(
        "ingest-mplads-part",
        {
          body: {
            path:
              claimed.file_path,
          },
        }
      );

    // ========================================================
    // INGESTOR ERROR
    // ========================================================

    if (invokeError) {

      await supabase
        .from("ingestion_jobs")
        .update({
          status: "failed",

          error_message:
            errorMessage(
              invokeError
            ),

          completed_at:
            new Date().toISOString(),
        })
        .eq(
          "job_id",
          jobId
        );

      throw new Error(
        `Part ingestion failed: ${errorMessage(
          invokeError
        )}`
      );
    }

    // The ingestion function can return
    // success:false inside its response.
    if (
      result &&
      result.success === false
    ) {

      const message =
        result.error
          ? JSON.stringify(
              result.error
            )
          : "Ingestion returned success=false";

      await supabase
        .from("ingestion_jobs")
        .update({
          status: "failed",

          error_message:
            message,

          completed_at:
            new Date().toISOString(),
        })
        .eq(
          "job_id",
          jobId
        );

      throw new Error(
        message
      );
    }

    // ========================================================
    // MARK COMPLETED
    // ========================================================

    let recordsProcessed = 0;

    if (
      result &&
      typeof result.records ===
        "number"
    ) {
      recordsProcessed =
        result.records;
    }

    const { error: completeError } =
      await supabase
        .from("ingestion_jobs")
        .update({
          status: "completed",

          records_processed:
            recordsProcessed,

          completed_at:
            new Date().toISOString(),

          error_message: null,
        })
        .eq(
          "job_id",
          jobId
        );

    if (completeError) {
      throw new Error(
        `Failed marking job complete: ${JSON.stringify(
          completeError
        )}`
      );
    }

    // ========================================================
    // PROGRESS
    // ========================================================

    const { count: completedCount } =
      await supabase
        .from("ingestion_jobs")
        .select(
          "*",
          {
            count: "exact",
            head: true,
          }
        )
        .eq(
          "run_id",
          runId
        )
        .eq(
          "status",
          "completed"
        );

    const { count: totalCount } =
      await supabase
        .from("ingestion_jobs")
        .select(
          "*",
          {
            count: "exact",
            head: true,
          }
        )
        .eq(
          "run_id",
          runId
        );

    return new Response(
      JSON.stringify(
        {
          success: true,

          run_id:
            runId,

          job_id:
            jobId,

          dataset:
            claimed.dataset,

          part_number:
            claimed.part_number,

          file_path:
            claimed.file_path,

          records:
            recordsProcessed,

          progress: {
            completed:
              completedCount ?? 0,

            total:
              totalCount ?? 0,

            remaining:
              (totalCount ?? 0) -
              (completedCount ?? 0),
          },

          ingestion_result:
            result,
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
      "WORKER ERROR",
      error
    );

    return new Response(
      JSON.stringify({
        success: false,
        error:
          errorMessage(error),
      }),
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