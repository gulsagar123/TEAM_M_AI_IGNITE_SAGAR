# =============================================================================
# 01_bronze_reference_data.py
# Revenue Leakage Detection & Prediction — Home Health
# Purpose: Generate Bronze layer reference data
#          (Patients, Clinicians, Payers, Service Types)
# Run order: 1 of 5
# =============================================================================

import random
from datetime import datetime, timedelta
from pyspark.sql.types import *
from config import (
    BRONZE_SCHEMA, START_DATE, END_DATE,
    FIRST_NAMES, LAST_NAMES, CLINICIAN_TYPES, PAYER_NAMES
)

# ---------------------------------------------------------------------------
# 1. Patients (50 records)
# ---------------------------------------------------------------------------
def generate_patients(spark):
    patient_data = []
    for i in range(50):
        patient_id = f"PAT{str(i+1).zfill(5)}"
        dob = START_DATE - timedelta(days=random.randint(25000, 32000))  # Age 68-87
        patient_data.append((
            patient_id,
            patient_id,
            f"MRN{random.randint(100000, 999999)}",
            dob.date(),
            random.choice(['M', 'F']),
            random.choice(FIRST_NAMES),
            random.choice(LAST_NAMES),
            f"{random.randint(100, 9999)} {random.choice(['Main', 'Oak', 'Elm', 'Maple'])} St",
            random.choice(['Springfield', 'Riverside', 'Greenville', 'Madison', 'Franklin']),
            random.choice(['IL', 'CA', 'TX', 'FL', 'NY']),
            f"{random.randint(10000, 99999)}",
            f"{random.randint(200,999)}-{random.randint(200,999)}-{random.randint(1000,9999)}",
            datetime.now()
        ))

    schema = StructType([
        StructField("patient_id",           StringType(),    False),
        StructField("source_patient_id",    StringType(),    True),
        StructField("patient_mrn",          StringType(),    True),
        StructField("date_of_birth",        DateType(),      True),
        StructField("gender",               StringType(),    True),
        StructField("first_name",           StringType(),    True),
        StructField("last_name",            StringType(),    True),
        StructField("address_line_1",       StringType(),    True),
        StructField("city",                 StringType(),    True),
        StructField("state",                StringType(),    True),
        StructField("zip_code",             StringType(),    True),
        StructField("phone_number",         StringType(),    True),
        StructField("ingestion_timestamp",  TimestampType(), True),
    ])

    df = spark.createDataFrame(patient_data, schema=schema)
    spark.sql(f"DROP TABLE IF EXISTS {BRONZE_SCHEMA}.bronze_patient")
    df.write.saveAsTable(f"{BRONZE_SCHEMA}.bronze_patient")
    print(f"✓ Generated {df.count()} patients")
    return df


# ---------------------------------------------------------------------------
# 2. Clinicians (20 records)
# ---------------------------------------------------------------------------
def generate_clinicians(spark):
    clinician_data = []
    for i in range(20):
        clinician_id = f"CLI{str(i+1).zfill(5)}"
        clinician_data.append((
            clinician_id,
            clinician_id,
            random.choice(FIRST_NAMES),
            random.choice(LAST_NAMES),
            random.choice(CLINICIAN_TYPES),
            f"LIC{random.randint(100000, 999999)}",
            (START_DATE - timedelta(days=random.randint(1, 1000))).date(),
            None if random.random() > 0.1 else (END_DATE + timedelta(days=random.randint(30, 365))).date(),
            datetime.now()
        ))

    schema = StructType([
        StructField("clinician_id",         StringType(),    False),
        StructField("source_clinician_id",  StringType(),    True),
        StructField("first_name",           StringType(),    True),
        StructField("last_name",            StringType(),    True),
        StructField("clinician_type",       StringType(),    True),
        StructField("license_number",       StringType(),    True),
        StructField("hire_date",            DateType(),      True),
        StructField("termination_date",     DateType(),      True),
        StructField("ingestion_timestamp",  TimestampType(), True),
    ])

    df = spark.createDataFrame(clinician_data, schema=schema)
    spark.sql(f"DROP TABLE IF EXISTS {BRONZE_SCHEMA}.bronze_clinician")
    df.write.saveAsTable(f"{BRONZE_SCHEMA}.bronze_clinician")
    print(f"✓ Generated {df.count()} clinicians")
    return df


# ---------------------------------------------------------------------------
# 3. Payers (10 records)
# ---------------------------------------------------------------------------
def generate_payers(spark):
    payer_data = []
    for i, name in enumerate(PAYER_NAMES):
        payer_id = f"PAY{str(i+1).zfill(5)}"
        payer_data.append((
            payer_id,
            payer_id,
            name,
            random.choice(['Medicare', 'Medicaid', 'Commercial', 'Commercial', 'Medicare Advantage']),
            f"{random.randint(10000, 99999)}",
            datetime.now()
        ))

    schema = StructType([
        StructField("payer_id",            StringType(),    False),
        StructField("source_payer_id",     StringType(),    True),
        StructField("payer_name",          StringType(),    True),
        StructField("payer_type",          StringType(),    True),
        StructField("payer_identifier",    StringType(),    True),
        StructField("ingestion_timestamp", TimestampType(), True),
    ])

    df = spark.createDataFrame(payer_data, schema=schema)
    spark.sql(f"DROP TABLE IF EXISTS {BRONZE_SCHEMA}.bronze_payer")
    df.write.saveAsTable(f"{BRONZE_SCHEMA}.bronze_payer")
    print(f"✓ Generated {df.count()} payers")
    return df


# ---------------------------------------------------------------------------
# 4. Service Types (6 standard home health services)
# ---------------------------------------------------------------------------
def generate_service_types(spark):
    service_data = [
        ('SVC00001', 'SVC00001', 'Skilled Nursing',       'SN',  'Nursing visit for assessment and care',        datetime.now()),
        ('SVC00002', 'SVC00002', 'Physical Therapy',       'PT',  'Physical therapy evaluation and treatment',    datetime.now()),
        ('SVC00003', 'SVC00003', 'Occupational Therapy',   'OT',  'Occupational therapy services',                datetime.now()),
        ('SVC00004', 'SVC00004', 'Speech Therapy',         'ST',  'Speech-language pathology services',           datetime.now()),
        ('SVC00005', 'SVC00005', 'Medical Social Work',    'MSW', 'Medical social services',                      datetime.now()),
        ('SVC00006', 'SVC00006', 'Home Health Aide',       'HHA', 'Home health aide services',                    datetime.now()),
    ]

    schema = StructType([
        StructField("service_type_id",        StringType(),    False),
        StructField("source_service_type_id", StringType(),    True),
        StructField("service_name",           StringType(),    True),
        StructField("service_code",           StringType(),    True),
        StructField("service_description",    StringType(),    True),
        StructField("ingestion_timestamp",    TimestampType(), True),
    ])

    df = spark.createDataFrame(service_data, schema=schema)
    spark.sql(f"DROP TABLE IF EXISTS {BRONZE_SCHEMA}.bronze_service_type")
    df.write.saveAsTable(f"{BRONZE_SCHEMA}.bronze_service_type")
    print(f"✓ Generated {df.count()} service types")
    return df


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    df_patient  = generate_patients(spark)
    df_clinician = generate_clinicians(spark)
    df_payer    = generate_payers(spark)
    df_service  = generate_service_types(spark)
    print("\n=== Reference Data Generation Complete ===")
