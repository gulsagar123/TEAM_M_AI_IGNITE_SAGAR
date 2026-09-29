# =============================================================================
# 05_run_pipeline.py
# Revenue Leakage Detection & Prediction — Home Health
# Purpose: Master orchestrator — runs the full pipeline end to end
#
# Run this single file on Databricks to execute all steps in order:
#   Step 1 → config.py                    (configuration)
#   Step 2 → 01_bronze_reference_data.py  (patients, clinicians, payers, services)
#   Step 3 → 02_bronze_transactional_data.py (episodes, visits, charges, claims ...)
#   Step 4 → 03_silver_layer.py           (dimensions + fact tables)
#   Step 5 → 04_gold_leakage_detection.py (leakage views + appeals analysis)
# =============================================================================

from pyspark.sql import functions as F

# Import all modules
from config import (
    CATALOG, BRONZE_SCHEMA, SILVER_SCHEMA, GOLD_SCHEMA,
    print_config
)
from bronze_reference_data import (
    generate_patients, generate_clinicians,
    generate_payers, generate_service_types
)
from bronze_transactional_data import (
    generate_episodes, generate_authorizations,
    generate_visits_and_charges, generate_claims_and_remittances,
    generate_dme, generate_diagnosis, generate_admissions, generate_appeals
)
from silver_layer import (
    create_silver_schemas, create_dim_patient,
    create_dim_clinician, create_dim_payer,
    create_fact_visit, create_fact_claim
)
from gold_leakage_detection import (
    create_gold_schema, create_vw_missed_charges,
    create_vw_undercoding, create_vw_authorization_lapses,
    create_vw_unbilled_dme_and_denials, create_claims_appeals_analysis
)


def setup_catalog(spark):
    spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG}")
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {BRONZE_SCHEMA}")
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {SILVER_SCHEMA}")
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {GOLD_SCHEMA}")
    print(f"✓ Catalog and schemas ready under: {CATALOG}")


def run_pipeline(spark):
    print("=" * 70)
    print("  REVENUE LEAKAGE DETECTION & PREDICTION — HOME HEALTH")
    print("  Full Pipeline Run")
    print("=" * 70)

    # ── Config ──────────────────────────────────────────────────────────────
    print_config()
    setup_catalog(spark)

    # ── Step 1: Bronze Reference Data ───────────────────────────────────────
    print("\n[STEP 1/4] Generating Bronze Reference Data...")
    df_patient   = generate_patients(spark)
    df_clinician = generate_clinicians(spark)
    df_payer     = generate_payers(spark)
    df_service   = generate_service_types(spark)

    patient_ids   = [r.patient_id      for r in df_patient.select('patient_id').collect()]
    payer_ids     = [r.payer_id        for r in df_payer.select('payer_id').collect()]
    service_ids   = [r.service_type_id for r in df_service.select('service_type_id').collect()]
    clinician_ids = [r.clinician_id    for r in df_clinician.select('clinician_id').collect()]

    # ── Step 2: Bronze Transactional Data ───────────────────────────────────
    print("\n[STEP 2/4] Generating Bronze Transactional Data...")
    df_episode  = generate_episodes(spark, patient_ids, payer_ids)
    episode_ids = [r.episode_id for r in df_episode.select('episode_id').collect()]

    generate_authorizations(spark, df_episode, episode_ids, service_ids)
    df_visit, df_charge = generate_visits_and_charges(spark, episode_ids, clinician_ids, service_ids)
    df_claim, df_remit  = generate_claims_and_remittances(spark, df_charge, episode_ids, payer_ids)

    generate_dme(spark, episode_ids)
    generate_diagnosis(spark, df_episode, episode_ids)
    generate_admissions(spark, df_episode, episode_ids)
    generate_appeals(spark, df_remit)

    # ── Step 3: Silver Layer ─────────────────────────────────────────────────
    print("\n[STEP 3/4] Building Silver Layer...")
    create_silver_schemas(spark)
    create_dim_patient(spark)
    create_dim_clinician(spark)
    create_dim_payer(spark)
    create_fact_visit(spark)
    create_fact_claim(spark)

    # ── Step 4: Gold Layer ───────────────────────────────────────────────────
    print("\n[STEP 4/4] Building Gold Layer — Leakage Detection...")
    create_gold_schema(spark)
    create_vw_missed_charges(spark)
    create_vw_undercoding(spark)
    create_vw_authorization_lapses(spark)
    create_vw_unbilled_dme_and_denials(spark)
    create_claims_appeals_analysis(spark)

    # ── Summary ──────────────────────────────────────────────────────────────
    print("\n" + "=" * 70)
    print("  PIPELINE COMPLETE — LEAKAGE SUMMARY")
    print("=" * 70)

    missed  = spark.sql(f"SELECT COUNT(*) FROM {GOLD_SCHEMA}.vw_missed_charges").collect()[0][0]
    underc  = spark.sql(f"SELECT COUNT(*) FROM {GOLD_SCHEMA}.vw_undercoding").collect()[0][0]
    auth    = spark.sql(f"SELECT COUNT(*) FROM {GOLD_SCHEMA}.vw_authorization_lapses").collect()[0][0]
    dme     = spark.sql(f"SELECT COUNT(*) FROM {GOLD_SCHEMA}.vw_unbilled_dme").collect()[0][0]
    appeals = spark.sql(f"""
        SELECT COUNT(*) FROM {GOLD_SCHEMA}.claims_appeals_analysis
        WHERE missed_opportunity_flag = 1
    """).collect()[0][0]

    print(f"  1. Missed Charges detected      : {missed}")
    print(f"  2. Undercoding cases detected   : {underc}")
    print(f"  3. Authorization lapses detected: {auth}")
    print(f"  4. Unbilled DME detected        : {dme}")
    print(f"  5. Missed appeal opportunities  : {appeals}")
    print("\n  Review Gold layer tables/views for full details.")
    print("=" * 70)


# ---------------------------------------------------------------------------
# Entry point (runs on Databricks where `spark` is pre-defined)
# ---------------------------------------------------------------------------
run_pipeline(spark)
