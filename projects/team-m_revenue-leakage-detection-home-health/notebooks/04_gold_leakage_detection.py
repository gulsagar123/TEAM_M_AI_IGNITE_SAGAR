# =============================================================================
# 04_gold_leakage_detection.py
# Revenue Leakage Detection & Prediction — Home Health
# Purpose: Build Gold layer — leakage detection views and analytics tables
#          (Missed Charges, Undercoding, Auth Lapses, Unbilled DME,
#           Payer Denials, Claims Appeals Analysis)
# Run order: 4 of 5  (run after 03_silver_layer.py)
# =============================================================================

from config import BRONZE_SCHEMA, GOLD_SCHEMA


def create_gold_schema(spark):
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {GOLD_SCHEMA}")
    print(f"✓ Gold schema ready: {GOLD_SCHEMA}")


# ---------------------------------------------------------------------------
# 1. Missed Charges Detection View
# ---------------------------------------------------------------------------
def create_vw_missed_charges(spark):
    spark.sql(f"""
        CREATE OR REPLACE VIEW {GOLD_SCHEMA}.vw_missed_charges AS
        SELECT
            v.visit_id,
            v.episode_id,
            v.patient_id,
            v.clinician_id,
            v.service_type_id,
            v.visit_date,
            st.service_name,
            'MISSED_CHARGE'  AS leakage_type,
            'HIGH'           AS urgency,
            150.0            AS estimated_revenue_at_risk
        FROM hh_revenue_leakage.bronze.bronze_visit v
        INNER JOIN hh_revenue_leakage.bronze.bronze_episode e
            ON v.episode_id = e.episode_id
        INNER JOIN hh_revenue_leakage.bronze.bronze_service_type st
            ON v.service_type_id = st.service_type_id
        LEFT JOIN hh_revenue_leakage.bronze.bronze_charge c
            ON v.visit_id = c.visit_id
        WHERE v.visit_status = 'Completed'
          AND c.charge_id IS NULL
          AND v.visit_date >= DATE_ADD(CURRENT_DATE(), -180)
    """)
    count = spark.sql(f"SELECT COUNT(*) FROM {GOLD_SCHEMA}.vw_missed_charges").collect()[0][0]
    print(f"✓ vw_missed_charges — {count} missed charge records detected")


# ---------------------------------------------------------------------------
# 2. Undercoding Detection View
# ---------------------------------------------------------------------------
def create_vw_undercoding(spark):
    spark.sql(f"""
        CREATE OR REPLACE VIEW {GOLD_SCHEMA}.vw_undercoding AS
        SELECT
            v.visit_id,
            v.episode_id,
            e.patient_id,
            e.payer_id,
            v.visit_date,
            c.cpt_code              AS billed_cpt_code,
            c.charge_amount         AS billed_amount,
            CASE
                WHEN c.cpt_code IN ('99347','99348') THEN 'LOW'
                WHEN c.cpt_code IN ('99349','99350') THEN 'MEDIUM'
                WHEN c.cpt_code IN ('G0154','G0155') THEN 'HIGH'
            END                     AS billed_complexity,
            'UNDERCODING'           AS leakage_type,
            'MEDIUM'                AS urgency,
            (250.0 - c.charge_amount) AS estimated_revenue_gap
        FROM hh_revenue_leakage.bronze.bronze_visit v
        INNER JOIN hh_revenue_leakage.bronze.bronze_episode e
            ON v.episode_id = e.episode_id
        INNER JOIN hh_revenue_leakage.bronze.bronze_charge c
            ON v.visit_id = c.visit_id
        WHERE v.visit_status  = 'Completed'
          AND c.cpt_code IN ('99349', '99350')   -- Billed at medium
          AND v.visit_date >= DATE_ADD(CURRENT_DATE(), -180)
    """)
    count = spark.sql(f"SELECT COUNT(*) FROM {GOLD_SCHEMA}.vw_undercoding").collect()[0][0]
    print(f"✓ vw_undercoding — {count} potential undercoding records detected")


# ---------------------------------------------------------------------------
# 3. Authorization Lapses Detection View
# ---------------------------------------------------------------------------
def create_vw_authorization_lapses(spark):
    spark.sql(f"""
        CREATE OR REPLACE VIEW {GOLD_SCHEMA}.vw_authorization_lapses AS
        SELECT
            v.visit_id,
            v.episode_id,
            v.service_type_id,
            v.visit_date,
            e.patient_id,
            e.payer_id,
            st.service_name,
            a.authorization_id,
            a.authorization_start_date,
            a.authorization_end_date,
            CASE
                WHEN a.authorization_id IS NULL            THEN 'NO_AUTH_FOUND'
                WHEN v.visit_date < a.authorization_start_date THEN 'BEFORE_AUTH_START'
                WHEN v.visit_date > a.authorization_end_date   THEN 'AFTER_AUTH_END'
                ELSE 'VALID'
            END AS auth_status,
            CASE
                WHEN a.authorization_id IS NULL OR v.visit_date > a.authorization_end_date
                THEN 'HIGH' ELSE 'MEDIUM'
            END AS urgency,
            'AUTHORIZATION_LAPSE' AS leakage_type,
            c.charge_amount       AS at_risk_amount
        FROM hh_revenue_leakage.bronze.bronze_visit v
        INNER JOIN hh_revenue_leakage.bronze.bronze_episode e
            ON v.episode_id = e.episode_id
        INNER JOIN hh_revenue_leakage.bronze.bronze_service_type st
            ON v.service_type_id = st.service_type_id
        LEFT JOIN hh_revenue_leakage.bronze.bronze_authorization a
            ON v.episode_id = a.episode_id
           AND v.service_type_id = a.service_type_id
           AND v.visit_date BETWEEN a.authorization_start_date AND a.authorization_end_date
        LEFT JOIN hh_revenue_leakage.bronze.bronze_charge c
            ON v.visit_id = c.visit_id
        WHERE v.visit_status = 'Completed'
          AND (a.authorization_id IS NULL
               OR v.visit_date > a.authorization_end_date
               OR v.visit_date < a.authorization_start_date)
          AND v.visit_date >= DATE_ADD(CURRENT_DATE(), -180)
    """)
    count = spark.sql(f"SELECT COUNT(*) FROM {GOLD_SCHEMA}.vw_authorization_lapses").collect()[0][0]
    print(f"✓ vw_authorization_lapses — {count} authorization lapse records detected")


# ---------------------------------------------------------------------------
# 4. Unbilled DME + Payer Denial Detection View
# ---------------------------------------------------------------------------
def create_vw_unbilled_dme_and_denials(spark):
    # Unbilled DME
    spark.sql(f"""
        CREATE OR REPLACE VIEW {GOLD_SCHEMA}.vw_unbilled_dme AS
        SELECT
            d.dme_id,
            d.episode_id,
            d.dme_description,
            d.delivery_date,
            d.dme_cost,
            'DME_NOT_BILLED' AS leakage_type,
            'MEDIUM'         AS urgency,
            d.dme_cost       AS revenue_at_risk
        FROM hh_revenue_leakage.bronze.bronze_dme d
        WHERE d.billed_flag = FALSE
    """)
    count = spark.sql(f"SELECT COUNT(*) FROM {GOLD_SCHEMA}.vw_unbilled_dme").collect()[0][0]
    print(f"✓ vw_unbilled_dme — {count} unbilled DME records detected")

    # Payer denial analysis
    spark.sql(f"""
        CREATE OR REPLACE VIEW {GOLD_SCHEMA}.vw_payer_denial_analysis AS
        SELECT
            'PAYER_DENIAL_ANALYSIS'                                     AS analysis_type,
            p.payer_name,
            COUNT(cl.claim_id)                                          AS total_claims,
            SUM(CASE WHEN r.denial_reason_code IS NOT NULL THEN 1 ELSE 0 END) AS denied_claims,
            ROUND(
                SUM(CASE WHEN r.denial_reason_code IS NOT NULL THEN 1 ELSE 0 END)
                / COUNT(cl.claim_id) * 100, 2
            )                                                           AS denial_rate_pct,
            ROUND(SUM(cl.claim_amount), 2)                              AS total_billed_amount,
            ROUND(SUM(COALESCE(r.paid_amount, 0)), 2)                   AS total_paid_amount,
            ROUND(SUM(cl.claim_amount) - SUM(COALESCE(r.paid_amount, 0)), 2) AS total_revenue_loss
        FROM hh_revenue_leakage.bronze.bronze_claim cl
        INNER JOIN hh_revenue_leakage.bronze.bronze_payer p
            ON cl.payer_id = p.payer_id
        LEFT JOIN hh_revenue_leakage.bronze.bronze_remittance r
            ON cl.claim_id = r.claim_id
        GROUP BY p.payer_name
        ORDER BY total_revenue_loss DESC
    """)
    print(f"✓ vw_payer_denial_analysis created")


# ---------------------------------------------------------------------------
# 5. Gold Table: Claims Appeals Analysis
# ---------------------------------------------------------------------------
def create_claims_appeals_analysis(spark):
    import numpy as np
    from datetime import datetime, timedelta
    from pyspark.sql import functions as F
    from pyspark.sql.types import *

    denied_claims_df = spark.sql(f"""
        SELECT
            cl.claim_id,
            cl.episode_id,
            cl.payer_id,
            ep.patient_id,
            cl.claim_date,
            cl.claim_amount,
            COALESCE(r.paid_amount, 0)       AS initial_paid_amount,
            cl.claim_amount - COALESCE(r.paid_amount, 0) AS denied_amount,
            r.denial_reason_code,
            a.appeal_id,
            a.appeal_date,
            a.appeal_status
        FROM hh_revenue_leakage.bronze.bronze_claim cl
        INNER JOIN hh_revenue_leakage.bronze.bronze_remittance r
            ON cl.claim_id = r.claim_id
           AND r.denial_reason_code IS NOT NULL
        LEFT JOIN hh_revenue_leakage.bronze.bronze_episode ep
            ON cl.episode_id = ep.episode_id
        LEFT JOIN hh_revenue_leakage.bronze.bronze_appeal a
            ON cl.claim_id = a.claim_id
    """)

    rows = denied_claims_df.collect()
    appeals_analysis_data = []

    for row in rows:
        claim_id      = row['claim_id']
        episode_id    = row['episode_id']
        patient_id    = row['patient_id']
        payer_id      = row['payer_id']
        claim_date    = row['claim_date']
        claim_amount  = row['claim_amount']
        initial_paid  = row['initial_paid_amount']
        denied_amount = row['denied_amount']
        appeal_id     = row['appeal_id']
        appeal_date   = row['appeal_date']
        appeal_status = row['appeal_status'] if row['appeal_status'] else None

        was_appealed = 1 if appeal_id else 0

        if was_appealed:
            if appeal_status == 'Approved':
                appeal_outcome    = 'Approved'
                appeal_successful = 1
                recovered_amount  = denied_amount * np.random.uniform(0.6, 0.8)
            elif appeal_status == 'Pending':
                appeal_outcome    = 'Pending'
                appeal_successful = None
                recovered_amount  = 0
            else:
                appeal_outcome    = 'Denied'
                appeal_successful = 0
                recovered_amount  = 0

            if appeal_status in ['Approved', 'Denied']:
                days_to_resolution = int(np.random.uniform(30, 90))
                resolution_date    = appeal_date + timedelta(days=days_to_resolution) if appeal_date else None
            else:
                days_to_resolution = None
                resolution_date    = None

            recovery_rate       = (recovered_amount / denied_amount * 100) if denied_amount > 0 else 0
            missed_opportunity  = 0
        else:
            days_to_resolution  = None
            resolution_date     = None
            appeal_successful   = None
            recovery_rate       = 0
            recovered_amount    = 0
            missed_opportunity  = 1
            appeal_status       = 'Not Appealed'
            appeal_outcome      = 'Missed Opportunity'

        appeals_analysis_data.append((
            claim_id, episode_id, patient_id, payer_id,
            claim_date, claim_amount, initial_paid, denied_amount,
            was_appealed, appeal_id, appeal_date, appeal_status,
            appeal_outcome, resolution_date, days_to_resolution,
            appeal_successful, float(recovered_amount),
            float(recovery_rate), missed_opportunity, datetime.now()
        ))

    schema = StructType([
        StructField("claim_id",               StringType(),    False),
        StructField("episode_id",             StringType(),    False),
        StructField("patient_id",             StringType(),    True),
        StructField("payer_id",               StringType(),    True),
        StructField("claim_date",             DateType(),      True),
        StructField("claim_amount",           FloatType(),     True),
        StructField("initial_paid_amount",    FloatType(),     True),
        StructField("denied_amount",          FloatType(),     True),
        StructField("was_appealed",           IntegerType(),   True),
        StructField("appeal_id",              StringType(),    True),
        StructField("appeal_date",            DateType(),      True),
        StructField("appeal_status",          StringType(),    True),
        StructField("appeal_outcome",         StringType(),    True),
        StructField("resolution_date",        DateType(),      True),
        StructField("days_to_resolution",     IntegerType(),   True),
        StructField("appeal_successful",      IntegerType(),   True),
        StructField("recovered_amount",       FloatType(),     True),
        StructField("recovery_rate_pct",      FloatType(),     True),
        StructField("missed_opportunity_flag", IntegerType(),  True),
        StructField("analysis_date",          TimestampType(), True),
    ])

    df_appeals = spark.createDataFrame(appeals_analysis_data, schema=schema)
    df_appeals.write.mode("overwrite").saveAsTable(f"{GOLD_SCHEMA}.claims_appeals_analysis")

    total_denied    = df_appeals.agg(F.sum("denied_amount")).collect()[0][0]
    total_recovered = df_appeals.agg(F.sum("recovered_amount")).collect()[0][0]
    total_missed    = df_appeals.filter(F.col("missed_opportunity_flag") == 1).agg(F.sum("denied_amount")).collect()[0][0]

    print(f"\n{'='*80}")
    print("✓ CLAIMS APPEALS ANALYSIS TABLE CREATED SUCCESSFULLY")
    print(f"{'='*80}")
    print(f"  Total Denied Amount            : ${total_denied:,.2f}")
    print(f"  Total Recovered                : ${total_recovered:,.2f}")
    print(f"  Recovery Rate                  : {(total_recovered/total_denied*100):.1f}%")
    print(f"  Missed Opportunity (Not Appealed): ${total_missed:,.2f}")
    print(f"  Potential Additional Recovery  : ${total_missed * 0.6:,.2f} (assuming 60% success rate)")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    create_gold_schema(spark)
    create_vw_missed_charges(spark)
    create_vw_undercoding(spark)
    create_vw_authorization_lapses(spark)
    create_vw_unbilled_dme_and_denials(spark)
    create_claims_appeals_analysis(spark)
    print("\n=== Gold Layer Leakage Detection Complete ===")
