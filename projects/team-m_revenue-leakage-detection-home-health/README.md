# Revenue Leakage Detection & Prediction — Home Health

> **AI Ignite 2026 Hackathon Submission**
> Team: Team M | Track: Healthcare Revenue Cycle

---

## Problem Statement

Home health agencies lose significant revenue each year due to preventable billing gaps — missed visit charges, undercoded claims, expired authorizations, unbilled DME, and uncontested claim denials. These leakages are often invisible until end-of-month reconciliation, by which time recovery is costly or impossible.

This project builds an end-to-end AI-powered pipeline to **detect, quantify, and predict revenue leakage** in real time — giving revenue cycle teams actionable alerts before money is permanently lost.

---

## Solution Overview

A medallion architecture pipeline on Databricks (Bronze → Silver → Gold) that ingests home health operational data, flags leakage scenarios using rule-based and ML logic, and surfaces recovery opportunities through a Gold-layer analytics layer.

### Leakage Scenarios Detected

| Scenario | Rate Modeled | Description |
|---|---|---|
| Missed Charges | 15% | Completed visits with no corresponding charge record |
| Undercoding | 10% | Claims billed at lower complexity than documented |
| Authorization Lapses | 5% | Services rendered without valid prior authorization |
| DME Not Billed | 8% | Durable medical equipment ordered but never invoiced |
| Denials / Adjustments | 12% | Claims denied with no appeal filed (missed recovery) |

---

## Architecture

```
Bronze Layer        Silver Layer         Gold Layer
─────────────       ─────────────        ──────────────────────────
bronze_patient  →   dim_patient     →    claims_appeals_analysis
bronze_clinician →  dim_clinician   →    leakage_summary
bronze_payer    →   dim_payer       →    missed_charge_flags
bronze_episode  →   fact_visit      →    denial_recovery_opportunities
bronze_claim    →   fact_claim      →
bronze_appeal   →   fact_appeal     →
bronze_dme      →                   →
```

**Tech Stack:**
- **Platform:** Databricks Serverless (PySpark)
- **Catalog:** Unity Catalog (`hh_revenue_leakage`)
- **Language:** Python / PySpark
- **ML Tracking:** MLflow
- **Data:** Synthetic — no real PHI used

---

## Repository Structure

```
team-m_revenue-leakage-detection-home-health/
├── README.md                          ← This file
├── .env.example                       ← Environment variable template
├── .gitignore                         ← Excludes secrets and temp files
├── src/
│   └── 01_data_generation_and_pipeline.ipynb   ← Main notebook (full pipeline)
├── presentation/
│   └── team-m_ai-ignite-2026.pdf      ← Final pitch deck
├── docs/
│   └── architecture.md                ← Architecture details
└── data/
    └── README.md                      ← Note: all data is synthetic, generated in-notebook
```

---

## How to Run

### Prerequisites
- Databricks workspace with Unity Catalog enabled
- Serverless compute cluster
- Python 3.10+ (Databricks Runtime 14.x+)

### Steps

1. **Clone or import the notebook** into your Databricks workspace
   - Import `src/01_data_generation_and_pipeline.ipynb` via Databricks UI → Import

2. **Create the catalog** (run once):
   ```sql
   CREATE CATALOG IF NOT EXISTS hh_revenue_leakage;
   CREATE SCHEMA IF NOT EXISTS hh_revenue_leakage.bronze;
   CREATE SCHEMA IF NOT EXISTS hh_revenue_leakage.silver;
   CREATE SCHEMA IF NOT EXISTS hh_revenue_leakage.gold;
   ```

3. **Run the notebook top to bottom**
   - Cell 1: Configuration & setup
   - Cell 2: Bronze layer — synthetic data generation (patients, clinicians, payers, episodes, visits, claims, authorizations, DME, appeals)
   - Cell 3+: Silver layer — cleansing and dimensional modeling
   - Final cells: Gold layer — leakage detection and appeals analysis

4. **View results** in the Gold layer:
   ```sql
   SELECT * FROM hh_revenue_leakage.gold.claims_appeals_analysis
   ORDER BY denied_amount DESC;
   ```

### No environment variables required
All configuration (catalog name, schema names, leakage rates) is defined at the top of the notebook. No secrets or credentials are needed — the notebook uses the Databricks session context automatically.

---

## Key Outputs

| Table | Description |
|---|---|
| `gold.claims_appeals_analysis` | Per-claim appeal tracking with recovery rates and missed opportunity flags |
| `silver.dim_patient` | Cleansed patient dimension |
| `silver.dim_clinician` | Clinician dimension with license validity |
| `silver.fact_claim` | Claim-level fact table with denial flags |
| `silver.fact_visit` | Visit-level fact table with charge linkage |

---

## Sample Results (from synthetic data run)

- **Total Denied Amount:** tracked per payer
- **Recovery Rate:** ~60–80% on approved appeals
- **Missed Opportunities:** claims denied with no appeal filed, quantified in dollars
- **Potential Additional Recovery:** estimated at 60% of uncontested denials

---

## Team

| Name | Role |
|---|---|
| Team M | AI/ML Engineering |

**Organization:** Inferenz Tech
**Hackathon:** AI Ignite 2026

---

## Notes

- All patient and clinical data used in this project is **fully synthetic** — generated programmatically with no real PHI
- No API keys, credentials, or secrets are present in this repository
- Model weights and large artifacts (if any) are stored externally and linked in `docs/`
