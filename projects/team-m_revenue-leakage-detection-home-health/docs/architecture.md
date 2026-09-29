# Architecture — Revenue Leakage Detection & Prediction

## Overview

The pipeline follows a **Medallion Architecture** pattern implemented on Databricks Serverless with Unity Catalog.

```
┌─────────────────────────────────────────────────────────────────┐
│                    Databricks Serverless                        │
│                                                                 │
│  ┌──────────────┐   ┌──────────────┐   ┌──────────────────┐   │
│  │   BRONZE     │   │   SILVER     │   │      GOLD        │   │
│  │  Raw Ingestion│──▶│  Cleansed    │──▶│  Analytics-Ready │   │
│  │              │   │  Dimensional │   │  Leakage Flags   │   │
│  └──────────────┘   └──────────────┘   └──────────────────┘   │
│                                                                 │
│  Unity Catalog: hh_revenue_leakage                             │
└─────────────────────────────────────────────────────────────────┘
```

---

## Bronze Layer — Raw Data

Synthetic data generation simulating real home health source systems.

| Table | Rows | Description |
|---|---|---|
| `bronze_patient` | 50 | Patient demographics |
| `bronze_clinician` | 20 | Clinician records with license dates |
| `bronze_payer` | 10 | Insurance payer configurations |
| `bronze_service_type` | 6 | Service type definitions |
| `bronze_episode` | ~100 | Care episodes per patient |
| `bronze_visit` | ~500 | Individual clinical visits |
| `bronze_charge` | ~450 | Charge records linked to visits |
| `bronze_claim` | ~400 | Insurance claims |
| `bronze_authorization` | ~300 | Prior auth records |
| `bronze_dme_order` | ~150 | DME equipment orders |
| `bronze_appeal` | ~80 | Claim appeal records |

---

## Silver Layer — Cleansed & Dimensional

Standardized, deduplicated, and enriched models.

| Table | Type | Key Transformations |
|---|---|---|
| `dim_patient` | Dimension | Standardized demographics, age calculation |
| `dim_clinician` | Dimension | License validity flag, active status |
| `dim_payer` | Dimension | Payer type classification |
| `fact_visit` | Fact | Charge linkage, missed charge flag |
| `fact_claim` | Fact | Denial flag, undercoding detection |
| `fact_authorization` | Fact | Expiry check, coverage gap flag |

---

## Gold Layer — Leakage Detection

Analytics-ready tables with leakage flags and recovery metrics.

| Table | Purpose |
|---|---|
| `claims_appeals_analysis` | Per-claim appeal tracking: was it appealed, outcome, recovery amount, missed opportunity |

### Key Metrics Produced
- `was_appealed` — binary flag
- `appeal_outcome` — Approved / Denied / Pending / Missed Opportunity
- `recovered_amount` — dollars recovered via appeal
- `recovery_rate_pct` — % of denied amount recovered
- `missed_opportunity_flag` — 1 if denied but not appealed

---

## Leakage Scenario Logic

| Scenario | Detection Method |
|---|---|
| Missed Charges | Visit exists with no matching charge record |
| Undercoding | Documented complexity level > billed HCPCS code complexity |
| Auth Lapse | Service date outside authorization effective date range |
| DME Not Billed | DME order exists with no corresponding claim line |
| Denial / No Appeal | Claim denial with `appeal_id IS NULL` |

---

## MLflow Integration

Experiment tracking is embedded in the notebook:
- Experiment name stored in notebook metadata (`experimentId: 3016514247817214`)
- Run metrics: denial rates, recovery rates, leakage amounts by scenario
- Model artifacts: prediction models (if trained) logged to MLflow Model Registry
