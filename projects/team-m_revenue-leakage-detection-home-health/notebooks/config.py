# =============================================================================
# config.py
# Revenue Leakage Detection & Prediction — Home Health
# Purpose: Shared configuration and constants used across all modules
# =============================================================================

import random
from datetime import datetime, timedelta
from pyspark.sql import functions as F
from pyspark.sql.types import *

# ---------------------------------------------------------------------------
# Catalog & Schema
# ---------------------------------------------------------------------------
CATALOG       = "hh_revenue_leakage"
BRONZE_SCHEMA = f"{CATALOG}.bronze"
SILVER_SCHEMA = f"{CATALOG}.silver"
GOLD_SCHEMA   = f"{CATALOG}.gold"

# ---------------------------------------------------------------------------
# Date Range — last 6 months
# ---------------------------------------------------------------------------
END_DATE   = datetime.now()
START_DATE = END_DATE - timedelta(days=180)

# ---------------------------------------------------------------------------
# Leakage Scenario Rates
# ---------------------------------------------------------------------------
MISSED_CHARGE_RATE  = 0.15   # 15% of completed visits have no charges
UNDERCODED_RATE     = 0.10   # 10% billed at lower complexity than documented
AUTH_LAPSE_RATE     = 0.05   # 5%  services without valid authorization
DME_NOT_BILLED_RATE = 0.08   # 8%  DME equipment not billed
DENIAL_RATE         = 0.12   # 12% claims have denials or adjustments

# ---------------------------------------------------------------------------
# Reference Lists
# ---------------------------------------------------------------------------
FIRST_NAMES = [
    'James', 'Mary', 'John', 'Patricia', 'Robert', 'Jennifer', 'Michael',
    'Linda', 'William', 'Elizabeth', 'David', 'Barbara', 'Richard', 'Susan',
    'Joseph', 'Jessica', 'Thomas', 'Sarah', 'Charles', 'Karen'
]

LAST_NAMES = [
    'Smith', 'Johnson', 'Williams', 'Brown', 'Jones', 'Garcia', 'Miller',
    'Davis', 'Rodriguez', 'Martinez', 'Hernandez', 'Lopez', 'Gonzalez',
    'Wilson', 'Anderson', 'Thomas', 'Taylor', 'Moore', 'Jackson', 'Martin'
]

CLINICIAN_TYPES  = ['RN', 'LPN', 'PT', 'OT', 'ST', 'MSW']
PAYER_NAMES      = ['Medicare', 'Medicaid', 'Blue Cross', 'Aetna', 'UnitedHealth',
                    'Humana', 'Cigna', 'Anthem', 'WellCare', 'Molina']
DME_TYPES        = ['Walker', 'Wheelchair', 'Hospital Bed', 'Oxygen Concentrator',
                    'CPAP Machine', 'Blood Pressure Monitor', 'Nebulizer']
ICD10_CODES      = [
    'I50.9', 'E11.9', 'J44.9', 'I10', 'N18.3',
    'I25.10', 'M79.3', 'L89.90', 'M81.0', 'J18.9',
    'I48.91', 'E78.5', 'F32.9', 'G30.9', 'K21.9'
]
CPT_LOW    = ['99347', '99348']
CPT_MEDIUM = ['99349', '99350']
CPT_HIGH   = ['G0154', 'G0155']


def print_config():
    print("Configuration loaded successfully")
    print(f"Target catalog : {CATALOG}")
    print(f"Date range     : {START_DATE.date()} to {END_DATE.date()}")
    print("\nLeakage scenario rates:")
    print(f"  - Missed charges      : {MISSED_CHARGE_RATE*100}%")
    print(f"  - Undercoding         : {UNDERCODED_RATE*100}%")
    print(f"  - Authorization lapses: {AUTH_LAPSE_RATE*100}%")
    print(f"  - DME not billed      : {DME_NOT_BILLED_RATE*100}%")
    print(f"  - Denials/adjustments : {DENIAL_RATE*100}%")


if __name__ == "__main__":
    print_config()
