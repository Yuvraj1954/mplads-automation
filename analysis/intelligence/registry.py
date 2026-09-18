"""Model registry persistence (DB2 model_registry table)."""

import json
from datetime import datetime, timezone

import asyncpg


async def upsert_registry(db, name, version, model_type, training_date,
                          training_observations, features, target,
                          validation_method, metrics, threshold,
                          calibration, status, data_version):
    await db.execute(
        """
        INSERT INTO public.model_registry
            (model_name, model_version, model_type, training_date,
             training_observations, features, target, validation_method,
             metrics, threshold, calibration, status, data_version)
        VALUES ($1,$2,$3,$4,$5,$6::jsonb,$7,$8,$9::jsonb,$10::jsonb,$11,$12,$13)
        ON CONFLICT (model_name) DO UPDATE SET
            model_type = EXCLUDED.model_type,
            training_date = EXCLUDED.training_date,
            training_observations = EXCLUDED.training_observations,
            features = EXCLUDED.features,
            target = EXCLUDED.target,
            validation_method = EXCLUDED.validation_method,
            metrics = EXCLUDED.metrics,
            threshold = EXCLUDED.threshold,
            calibration = EXCLUDED.calibration,
            status = EXCLUDED.status,
            data_version = EXCLUDED.data_version
        """,
        name, version, model_type, training_date,
        training_observations, json.dumps(features), target, validation_method,
        json.dumps(metrics), json.dumps(threshold or {}), calibration, status, data_version,
    )
