# =============================================================================
# 02_bronze_transactional_data.py
# Revenue Leakage Detection & Prediction — Home Health
# Purpose: Generate Bronze layer transactional data
#          (Episodes, Authorizations, Visits, Charges, Claims,
#           Remittances, DME, Diagnosis, Admissions, Appeals)
# Run order: 2 of 5  (run after 01_bronze_reference_data.py)
# =============================================================================

import random
from datetime import datetime, timedelta
from pyspark.sql import functions as F
from pyspark.sql.types import *
from config import (
    BRONZE_SCHEMA, START_DATE, END_DATE,
    MISSED_CHARGE_RATE, UNDERCODED_RATE, AUTH_LAPSE_RATE,
    DME_NOT_BILLED_RATE, DENIAL_RATE,
    FIRST_NAMES, LAST_NAMES, DME_TYPES, ICD10_CODES,
    CPT_LOW, CPT_MEDIUM, CPT_HIGH
)


# ---------------------------------------------------------------------------
# 1. Episodes (100 records)
# ---------------------------------------------------------------------------
def generate_episodes(spark, patient_ids, payer_ids):
    episode_data = []
    for i in range(100):
        episode_id  = f"EP{str(i+1).zfill(6)}"
        patient_id  = random.choice(patient_ids)
        payer_id    = random.choice(payer_ids)
        admit_date  = START_DATE + timedelta(days=random.randint(0, 150))
        discharge_date = admit_date + timedelta(days=random.randint(30, 60))
        status = 'Active' if random.random() < 0.8 else 'Closed'
        actual_discharge = discharge_date if status == 'Closed' else None

        episode_data.append((
            episode_id, episode_id, patient_id, payer_id,
            admit_date.date(), discharge_date.date(),
            actual_discharge.date() if actual_discharge else None,
            status,
            random.choice(['Surgical Recovery', 'Chronic Disease Management',
                           'Wound Care', 'Cardiac Care', 'Diabetes Management']),
            datetime.now()
        ))

    schema = StructType([
        StructField("episode_id",              StringType(),    False),
        StructField("source_episode_id",       StringType(),    True),
        StructField("patient_id",              StringType(),    False),
        StructField("payer_id",                StringType(),    False),
        StructField("admit_date",              DateType(),      True),
        StructField("expected_discharge_date", DateType(),      True),
        StructField("actual_discharge_date",   DateType(),      True),
        StructField("episode_status",          StringType(),    True),
        StructField("primary_diagnosis",       StringType(),    True),
        StructField("ingestion_timestamp",     TimestampType(), True),
    ])

    df = spark.createDataFrame(episode_data, schema=schema)
    spark.sql(f"DROP TABLE IF EXISTS {BRONZE_SCHEMA}.bronze_episode")
    df.write.saveAsTable(f"{BRONZE_SCHEMA}.bronze_episode")
    print(f"✓ Generated {df.count()} episodes")
    return df


# ---------------------------------------------------------------------------
# 2. Authorizations (80 records — some expired to create lapse scenarios)
# ---------------------------------------------------------------------------
def generate_authorizations(spark, df_episode, episode_ids, service_ids):
    auth_data = []
    for i in range(80):
        auth_id    = f"AUTH{str(i+1).zfill(6)}"
        episode_id = random.choice(episode_ids)
        service_id = random.choice(service_ids)
        ep_admit   = df_episode.filter(F.col('episode_id') == episode_id).select('admit_date').first()[0]
        auth_start = ep_admit
        auth_days  = 30 if random.random() < 0.9 else 60
        auth_end   = auth_start + timedelta(days=auth_days)

        # 5% will have short/expired authorizations
        if random.random() < AUTH_LAPSE_RATE:
            auth_end = auth_start + timedelta(days=random.randint(5, 15))

        auth_data.append((
            auth_id, auth_id, episode_id, service_id,
            auth_start, auth_end,
            random.randint(8, 30), 'Approved', datetime.now()
        ))

    schema = StructType([
        StructField("authorization_id",         StringType(),    False),
        StructField("source_authorization_id",  StringType(),    True),
        StructField("episode_id",               StringType(),    False),
        StructField("service_type_id",          StringType(),    False),
        StructField("authorization_start_date", DateType(),      True),
        StructField("authorization_end_date",   DateType(),      True),
        StructField("authorized_visit_count",   IntegerType(),   True),
        StructField("authorization_status",     StringType(),    True),
        StructField("ingestion_timestamp",      TimestampType(), True),
    ])

    df = spark.createDataFrame(auth_data, schema=schema)
    spark.sql(f"DROP TABLE IF EXISTS {BRONZE_SCHEMA}.bronze_authorization")
    df.write.saveAsTable(f"{BRONZE_SCHEMA}.bronze_authorization")
    print(f"✓ Generated {df.count()} authorizations")
    return df


# ---------------------------------------------------------------------------
# 3. Visits (500 records) + Charges (with missed charge & undercoding scenarios)
# ---------------------------------------------------------------------------
def generate_visits_and_charges(spark, episode_ids, clinician_ids, service_ids):
    # --- Visits ---
    visit_data = []
    visits_for_missing_charges = set()
    docs_for_undercoding = set()

    for i in range(500):
        visit_id   = f"VIS{str(i+1).zfill(6)}"
        episode_id = random.choice(episode_ids)
        visit_date = (START_DATE + timedelta(days=random.randint(0, 179))).date()
        status     = random.choice(['Completed', 'Completed', 'Completed', 'Scheduled', 'Cancelled'])

        # Inject missed charge scenario
        if status == 'Completed' and random.random() < MISSED_CHARGE_RATE:
            visits_for_missing_charges.add(visit_id)

        # Inject undercoding scenario
        if random.random() < UNDERCODED_RATE:
            docs_for_undercoding.add(visit_id)

        visit_data.append((
            visit_id, visit_id, episode_id,
            random.choice(clinician_ids),
            random.choice(service_ids),
            visit_date, status, datetime.now()
        ))

    visit_schema = StructType([
        StructField("visit_id",            StringType(),    False),
        StructField("source_visit_id",     StringType(),    True),
        StructField("episode_id",          StringType(),    False),
        StructField("clinician_id",        StringType(),    True),
        StructField("service_type_id",     StringType(),    True),
        StructField("visit_date",          DateType(),      True),
        StructField("visit_status",        StringType(),    True),
        StructField("ingestion_timestamp", TimestampType(), True),
    ])

    df_visit = spark.createDataFrame(visit_data, schema=visit_schema)
    spark.sql(f"DROP TABLE IF EXISTS {BRONZE_SCHEMA}.bronze_visit")
    df_visit.write.saveAsTable(f"{BRONZE_SCHEMA}.bronze_visit")
    print(f"✓ Generated {df_visit.count()} visits")

    # --- Charges (skip visits with intentional missing charges) ---
    visit_ids_all = [row.visit_id for row in df_visit.select('visit_id').collect()]
    charge_data = []

    for visit_id in visit_ids_all:
        if visit_id in visits_for_missing_charges:
            continue

        charge_id = f"CHG{visit_id[3:]}"

        if visit_id in docs_for_undercoding:
            cpt_code = random.choice(CPT_MEDIUM)
            amount   = random.uniform(120.0, 180.0)
        else:
            roll = random.random()
            if roll < 0.35:
                cpt_code = random.choice(CPT_LOW);    amount = random.uniform(80.0,  120.0)
            elif roll < 0.70:
                cpt_code = random.choice(CPT_MEDIUM); amount = random.uniform(120.0, 180.0)
            else:
                cpt_code = random.choice(CPT_HIGH);   amount = random.uniform(180.0, 250.0)

        charge_date = datetime.now() - timedelta(days=random.randint(0, 150))
        charge_data.append((
            charge_id, charge_id, visit_id, None,
            cpt_code, 1, amount, charge_date,
            random.choice(['Posted', 'Pending', 'Billed']),
            datetime.now()
        ))

    charge_schema = StructType([
        StructField("charge_id",           StringType(),    False),
        StructField("source_charge_id",    StringType(),    True),
        StructField("visit_id",            StringType(),    True),
        StructField("episode_id",          StringType(),    True),
        StructField("cpt_code",            StringType(),    True),
        StructField("charge_quantity",     IntegerType(),   True),
        StructField("charge_amount",       DoubleType(),    True),
        StructField("charge_date",         TimestampType(), True),
        StructField("charge_status",       StringType(),    True),
        StructField("ingestion_timestamp", TimestampType(), True),
    ])

    df_charge = spark.createDataFrame(charge_data, schema=charge_schema)
    spark.sql(f"DROP TABLE IF EXISTS {BRONZE_SCHEMA}.bronze_charge")
    df_charge.write.saveAsTable(f"{BRONZE_SCHEMA}.bronze_charge")
    print(f"✓ Generated {df_charge.count()} charges")
    print(f"  - {len(visits_for_missing_charges)} visits intentionally have NO charges (missed charge scenario)")
    print(f"  - {len(docs_for_undercoding)} charges are undercoded (medium CPT for high complexity)")
    return df_visit, df_charge


# ---------------------------------------------------------------------------
# 4. Claims (300) + Remittances (250, with denial scenarios)
# ---------------------------------------------------------------------------
def generate_claims_and_remittances(spark, df_charge, episode_ids, payer_ids):
    charge_ids = [row.charge_id for row in df_charge.select('charge_id').collect()]

    # --- Claims ---
    claim_data = []
    charge_idx = 0
    for i in range(300):
        claim_id    = f"CLM{str(i+1).zfill(6)}"
        episode_id  = random.choice(episode_ids)
        payer_id    = random.choice(payer_ids)
        num_charges = random.randint(1, 3)
        claim_amount = 0

        for _ in range(num_charges):
            if charge_idx < len(charge_ids):
                amt = df_charge.filter(F.col('charge_id') == charge_ids[charge_idx]).select('charge_amount').first()
                if amt:
                    claim_amount += amt[0]
                charge_idx += 1

        claim_date      = datetime.now() - timedelta(days=random.randint(0, 120))
        submission_date = claim_date + timedelta(days=random.randint(1, 7))

        claim_data.append((
            claim_id, claim_id, episode_id, payer_id,
            claim_date.date(), submission_date.date(), claim_amount,
            random.choice(['Submitted', 'Pending', 'Paid', 'Denied', 'Adjusted']),
            datetime.now()
        ))

    claim_schema = StructType([
        StructField("claim_id",            StringType(),    False),
        StructField("source_claim_id",     StringType(),    True),
        StructField("episode_id",          StringType(),    False),
        StructField("payer_id",            StringType(),    False),
        StructField("claim_date",          DateType(),      True),
        StructField("submission_date",     DateType(),      True),
        StructField("claim_amount",        DoubleType(),    True),
        StructField("claim_status",        StringType(),    True),
        StructField("ingestion_timestamp", TimestampType(), True),
    ])

    df_claim = spark.createDataFrame(claim_data, schema=claim_schema)
    spark.sql(f"DROP TABLE IF EXISTS {BRONZE_SCHEMA}.bronze_claim")
    df_claim.write.saveAsTable(f"{BRONZE_SCHEMA}.bronze_claim")
    print(f"✓ Generated {df_claim.count()} claims")

    # --- Remittances (12% denials) ---
    claim_ids     = [row.claim_id for row in df_claim.select('claim_id').collect()]
    remit_data    = []
    denials_count = 0

    for i in range(min(250, len(claim_ids))):
        remit_id  = f"REM{str(i+1).zfill(6)}"
        claim_id  = claim_ids[i]
        claim_amt = df_claim.filter(F.col('claim_id') == claim_id).select('claim_amount').first()[0]

        if random.random() < DENIAL_RATE:
            if random.random() < 0.5:
                paid_amount       = 0.0
                adjustment_amount = claim_amt
                denial_code       = random.choice(['CO-16', 'CO-97', 'CO-109', 'PR-1'])
            else:
                paid_amount       = claim_amt * random.uniform(0.50, 0.85)
                adjustment_amount = claim_amt - paid_amount
                denial_code       = random.choice(['CO-45', 'CO-16'])
            denials_count += 1
        else:
            paid_amount       = claim_amt
            adjustment_amount = 0.0
            denial_code       = None

        remit_date = datetime.now() - timedelta(days=random.randint(0, 90))
        remit_data.append((
            remit_id, remit_id, claim_id, remit_date.date(),
            paid_amount, adjustment_amount, denial_code, datetime.now()
        ))

    remit_schema = StructType([
        StructField("remittance_id",        StringType(),    False),
        StructField("source_remittance_id", StringType(),    True),
        StructField("claim_id",             StringType(),    False),
        StructField("remittance_date",      DateType(),      True),
        StructField("paid_amount",          DoubleType(),    True),
        StructField("adjustment_amount",    DoubleType(),    True),
        StructField("denial_reason_code",   StringType(),    True),
        StructField("ingestion_timestamp",  TimestampType(), True),
    ])

    df_remit = spark.createDataFrame(remit_data, schema=remit_schema)
    spark.sql(f"DROP TABLE IF EXISTS {BRONZE_SCHEMA}.bronze_remittance")
    df_remit.write.saveAsTable(f"{BRONZE_SCHEMA}.bronze_remittance")
    print(f"✓ Generated {df_remit.count()} remittances")
    print(f"  - {denials_count} remittances have denials or adjustments ({DENIAL_RATE*100}%)")
    return df_claim, df_remit


# ---------------------------------------------------------------------------
# 5. DME, Diagnosis, Admissions, Appeals
# ---------------------------------------------------------------------------
def generate_dme(spark, episode_ids):
    dme_data    = []
    unbilled_dme = []
    for i in range(100):
        dme_id      = f"DME{str(i+1).zfill(6)}"
        episode_id  = random.choice(episode_ids)
        dme_type    = random.choice(DME_TYPES)
        delivery_date = datetime.now() - timedelta(days=random.randint(0, 150))
        cost        = random.uniform(50.0, 500.0)
        is_unbilled = random.random() < DME_NOT_BILLED_RATE
        if is_unbilled:
            unbilled_dme.append(dme_id)
        dme_data.append((
            dme_id, dme_id, episode_id, dme_type,
            delivery_date.date(), cost, not is_unbilled, datetime.now()
        ))

    schema = StructType([
        StructField("dme_id",              StringType(),    False),
        StructField("source_dme_id",       StringType(),    True),
        StructField("episode_id",          StringType(),    False),
        StructField("dme_description",     StringType(),    True),
        StructField("delivery_date",       DateType(),      True),
        StructField("dme_cost",            DoubleType(),    True),
        StructField("billed_flag",         BooleanType(),   True),
        StructField("ingestion_timestamp", TimestampType(), True),
    ])

    df = spark.createDataFrame(dme_data, schema=schema)
    spark.sql(f"DROP TABLE IF EXISTS {BRONZE_SCHEMA}.bronze_dme")
    df.write.saveAsTable(f"{BRONZE_SCHEMA}.bronze_dme")
    print(f"✓ Generated {df.count()} DME records")
    print(f"  - {len(unbilled_dme)} DME items marked as unbilled ({DME_NOT_BILLED_RATE*100}%)")
    return df


def generate_diagnosis(spark, df_episode, episode_ids):
    diag_data = []
    for i in range(500):
        diag_id    = f"DX{str(i+1).zfill(6)}"
        episode_id = random.choice(episode_ids)
        patient_id = df_episode.filter(F.col('episode_id') == episode_id).select('patient_id').first()[0]
        diag_data.append((
            diag_id, patient_id, episode_id, None, None,
            random.choice(ICD10_CODES),
            random.choice(['Primary', 'Secondary', 'Tertiary']),
            (datetime.now() - timedelta(days=random.randint(0, 180))).date(),
            datetime.now()
        ))

    schema = StructType([
        StructField("diagnosis_id",        StringType(),    False),
        StructField("patient_id",          StringType(),    False),
        StructField("episode_id",          StringType(),    False),
        StructField("documentation_id",    StringType(),    True),
        StructField("claim_id",            StringType(),    True),
        StructField("icd10_code",          StringType(),    True),
        StructField("diagnosis_sequence",  StringType(),    True),
        StructField("diagnosis_date",      DateType(),      True),
        StructField("ingestion_timestamp", TimestampType(), True),
    ])

    df = spark.createDataFrame(diag_data, schema=schema)
    spark.sql(f"DROP TABLE IF EXISTS {BRONZE_SCHEMA}.bronze_diagnosis")
    df.write.saveAsTable(f"{BRONZE_SCHEMA}.bronze_diagnosis")
    print(f"✓ Generated {df.count()} diagnosis records")
    return df


def generate_admissions(spark, df_episode, episode_ids):
    admission_data = []
    for i, ep_id in enumerate(episode_ids):
        admission_id = f"ADM{str(i+1).zfill(6)}"
        ep = df_episode.filter(F.col('episode_id') == ep_id).first()
        admission_data.append((
            admission_id, admission_id, ep.patient_id, ep_id, ep.admit_date,
            random.choice(['Home Health Referral', 'Hospital Discharge', 'Physician Referral']),
            random.choice(['Medicare', 'Medicaid', 'Commercial']),
            datetime.now()
        ))

    schema = StructType([
        StructField("admission_id",        StringType(),    False),
        StructField("source_admission_id", StringType(),    True),
        StructField("patient_id",          StringType(),    False),
        StructField("episode_id",          StringType(),    False),
        StructField("admission_date",      DateType(),      True),
        StructField("admission_source",    StringType(),    True),
        StructField("admission_type",      StringType(),    True),
        StructField("ingestion_timestamp", TimestampType(), True),
    ])

    df = spark.createDataFrame(admission_data, schema=schema)
    spark.sql(f"DROP TABLE IF EXISTS {BRONZE_SCHEMA}.bronze_admission")
    df.write.saveAsTable(f"{BRONZE_SCHEMA}.bronze_admission")
    print(f"✓ Generated {df.count()} admission records")
    return df


def generate_appeals(spark, df_remit):
    denied_claims = df_remit.filter(F.col('denial_reason_code').isNotNull()).select('claim_id').collect()
    appeals = []
    for i in range(min(30, len(denied_claims))):
        appeal_id  = f"APP{str(i+1).zfill(6)}"
        claim_id   = denied_claims[i][0]
        appeal_date = datetime.now() - timedelta(days=random.randint(0, 60))
        appeals.append((
            appeal_id, appeal_id, claim_id, appeal_date.date(),
            random.choice(['Approved', 'Denied', 'Pending']),
            datetime.now()
        ))

    schema = StructType([
        StructField("appeal_id",           StringType(),    False),
        StructField("source_appeal_id",    StringType(),    True),
        StructField("claim_id",            StringType(),    False),
        StructField("appeal_date",         DateType(),      True),
        StructField("appeal_status",       StringType(),    True),
        StructField("ingestion_timestamp", TimestampType(), True),
    ])

    df = spark.createDataFrame(appeals, schema=schema)
    spark.sql(f"DROP TABLE IF EXISTS {BRONZE_SCHEMA}.bronze_appeal")
    df.write.saveAsTable(f"{BRONZE_SCHEMA}.bronze_appeal")
    print(f"✓ Generated {df.count()} appeal records")
    return df


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    from pyspark.sql import functions as F

    df_patient   = spark.table(f"{BRONZE_SCHEMA}.bronze_patient")
    df_payer     = spark.table(f"{BRONZE_SCHEMA}.bronze_payer")
    df_service   = spark.table(f"{BRONZE_SCHEMA}.bronze_service_type")
    df_clinician = spark.table(f"{BRONZE_SCHEMA}.bronze_clinician")

    patient_ids   = [r.patient_id      for r in df_patient.select('patient_id').collect()]
    payer_ids     = [r.payer_id        for r in df_payer.select('payer_id').collect()]
    service_ids   = [r.service_type_id for r in df_service.select('service_type_id').collect()]
    clinician_ids = [r.clinician_id    for r in df_clinician.select('clinician_id').collect()]

    df_episode  = generate_episodes(spark, patient_ids, payer_ids)
    episode_ids = [r.episode_id for r in df_episode.select('episode_id').collect()]

    df_auth               = generate_authorizations(spark, df_episode, episode_ids, service_ids)
    df_visit, df_charge   = generate_visits_and_charges(spark, episode_ids, clinician_ids, service_ids)
    df_claim, df_remit    = generate_claims_and_remittances(spark, df_charge, episode_ids, payer_ids)

    generate_dme(spark, episode_ids)
    generate_diagnosis(spark, df_episode, episode_ids)
    generate_admissions(spark, df_episode, episode_ids)
    generate_appeals(spark, df_remit)

    print("\n" + "="*80)
    print("=== ALL BRONZE LAYER DATA GENERATION COMPLETE ===")
    print("="*80)
