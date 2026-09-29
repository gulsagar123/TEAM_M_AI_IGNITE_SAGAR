# =============================================================================
# 03_silver_layer.py
# Revenue Leakage Detection & Prediction — Home Health
# Purpose: Build Silver layer — cleansed dimensional and fact models
#          from Bronze tables using Spark SQL
# Run order: 3 of 5  (run after 02_bronze_transactional_data.py)
# =============================================================================

from config import BRONZE_SCHEMA, SILVER_SCHEMA


def create_silver_schemas(spark):
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {SILVER_SCHEMA}")
    print(f"✓ Silver schema ready: {SILVER_SCHEMA}")


# ---------------------------------------------------------------------------
# Dimension Tables
# ---------------------------------------------------------------------------
def create_dim_patient(spark):
    spark.sql(f"""
        CREATE OR REPLACE TABLE {SILVER_SCHEMA}.dim_patient AS
        SELECT
            patient_id,
            source_patient_id,
            patient_mrn,
            date_of_birth,
            gender,
            first_name,
            last_name,
            CONCAT(first_name, ' ', last_name)           AS full_name,
            address_line_1,
            city,
            state,
            zip_code,
            phone_number,
            DATEDIFF(CURRENT_DATE(), date_of_birth) / 365 AS age_years,
            ingestion_timestamp                           AS silver_load_timestamp
        FROM {BRONZE_SCHEMA}.bronze_patient
    """)
    count = spark.sql(f"SELECT COUNT(*) FROM {SILVER_SCHEMA}.dim_patient").collect()[0][0]
    print(f"✓ dim_patient created — {count} rows")


def create_dim_clinician(spark):
    spark.sql(f"""
        CREATE OR REPLACE TABLE {SILVER_SCHEMA}.dim_clinician AS
        SELECT
            clinician_id,
            source_clinician_id,
            first_name,
            last_name,
            CONCAT(first_name, ' ', last_name) AS full_name,
            clinician_type,
            license_number,
            hire_date,
            termination_date,
            CASE
                WHEN termination_date IS NULL THEN 'Active'
                WHEN termination_date > CURRENT_DATE() THEN 'Active'
                ELSE 'Terminated'
            END AS employment_status,
            ingestion_timestamp AS silver_load_timestamp
        FROM {BRONZE_SCHEMA}.bronze_clinician
    """)
    count = spark.sql(f"SELECT COUNT(*) FROM {SILVER_SCHEMA}.dim_clinician").collect()[0][0]
    print(f"✓ dim_clinician created — {count} rows")


def create_dim_payer(spark):
    spark.sql(f"""
        CREATE OR REPLACE TABLE {SILVER_SCHEMA}.dim_payer AS
        SELECT
            payer_id,
            source_payer_id,
            payer_name,
            payer_type,
            payer_identifier,
            ingestion_timestamp AS silver_load_timestamp
        FROM {BRONZE_SCHEMA}.bronze_payer
    """)
    count = spark.sql(f"SELECT COUNT(*) FROM {SILVER_SCHEMA}.dim_payer").collect()[0][0]
    print(f"✓ dim_payer created — {count} rows")


# ---------------------------------------------------------------------------
# Fact Tables
# ---------------------------------------------------------------------------
def create_fact_visit(spark):
    spark.sql(f"""
        CREATE OR REPLACE TABLE {SILVER_SCHEMA}.fact_visit AS
        SELECT
            v.visit_id,
            v.episode_id,
            v.clinician_id,
            v.service_type_id,
            v.visit_date,
            v.visit_status,
            e.patient_id,
            e.payer_id,
            c.charge_id,
            c.cpt_code,
            c.charge_amount,
            c.charge_status,
            CASE WHEN c.charge_id IS NULL AND v.visit_status = 'Completed'
                 THEN 1 ELSE 0
            END AS missed_charge_flag,
            v.ingestion_timestamp AS silver_load_timestamp
        FROM {BRONZE_SCHEMA}.bronze_visit v
        LEFT JOIN {BRONZE_SCHEMA}.bronze_episode e
            ON v.episode_id = e.episode_id
        LEFT JOIN {BRONZE_SCHEMA}.bronze_charge c
            ON v.visit_id = c.visit_id
    """)
    count = spark.sql(f"SELECT COUNT(*) FROM {SILVER_SCHEMA}.fact_visit").collect()[0][0]
    print(f"✓ fact_visit created — {count} rows")


def create_fact_claim(spark):
    spark.sql(f"""
        CREATE OR REPLACE TABLE {SILVER_SCHEMA}.fact_claim AS
        SELECT
            cl.claim_id,
            cl.episode_id,
            cl.payer_id,
            cl.claim_date,
            cl.submission_date,
            cl.claim_amount,
            cl.claim_status,
            r.remittance_id,
            r.remittance_date,
            r.paid_amount,
            r.adjustment_amount,
            r.denial_reason_code,
            CASE WHEN r.denial_reason_code IS NOT NULL THEN 1 ELSE 0 END AS denial_flag,
            cl.claim_amount - COALESCE(r.paid_amount, 0)                 AS revenue_loss,
            cl.ingestion_timestamp AS silver_load_timestamp
        FROM {BRONZE_SCHEMA}.bronze_claim cl
        LEFT JOIN {BRONZE_SCHEMA}.bronze_remittance r
            ON cl.claim_id = r.claim_id
    """)
    count = spark.sql(f"SELECT COUNT(*) FROM {SILVER_SCHEMA}.fact_claim").collect()[0][0]
    print(f"✓ fact_claim created — {count} rows")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    create_silver_schemas(spark)
    create_dim_patient(spark)
    create_dim_clinician(spark)
    create_dim_payer(spark)
    create_fact_visit(spark)
    create_fact_claim(spark)
    print("\n=== Silver Layer Build Complete ===")
