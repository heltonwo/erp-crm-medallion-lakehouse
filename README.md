# erp-crm-medallion-lakehouse

A Databricks Lakehouse project implementing the **Medallion Architecture**
(Bronze → Silver → Gold) to integrate CRM and ERP data into a unified,
analytics-ready star schema, built on the AdventureWorks dataset.

## Overview

This project simulates a real-world data engineering scenario: two separate
source systems (a CRM and an ERP) hold overlapping and complementary data about
the same customers and products, in inconsistent formats. The pipeline cleans,
reconciles, and unifies both sources into a single star schema ready for BI and
analytics.

```
┌─────────────┐     ┌─────────────┐     ┌─────────────┐
│   BRONZE    │ ──► │   SILVER    │ ──► │    GOLD     │
│  Raw CSVs   │     │  Cleaned &  │     │ Star schema │
│  as Delta   │     │ standardized│     │ (dims+fact) │
└─────────────┘     └─────────────┘     └─────────────┘
```

**Gold layer star schema:**

```
dim_customers (customer_sk PK)  ──┐
                                    ├──► fact_sales (customer_sk FK, product_sk FK)
dim_products  (product_sk PK)   ──┘
```

## Tech Stack

- **Databricks** (notebooks, Unity Catalog, Delta Lake)
- **PySpark** (DataFrame API, Spark SQL)
- **Delta Lake** (ACID tables, schema evolution)
- **Git / GitHub** (version control, Databricks Repos integration)

## Repository Structure

```
├── 01_BRONZE_SCRIPTS/     # Raw ingestion: CSV → Delta, no transformation
├── 02_SILVER_SCRIPTS/     # Cleaning, standardization, deduplication
├── 03_GOLD_SCRIPTS/       # Star schema: dimensions + fact table
├── 04_DOCS/               # Detailed script-by-script documentation
│   ├── 01_Bronze_Layer_Doc.md
│   ├── 02_Silver_Layer_Doc.md
│   └── 03_Gold_Layer_Doc.md
└── README.md
```

## What This Project Demonstrates

- **Medallion architecture** design and implementation from scratch
- **Data quality investigation**: inconsistent categorical values, mismatched
  key formats between source systems, malformed dates, sign errors in numeric
  fields, and invalid/junk records — each diagnosed and resolved with a
  documented rationale
- **Slowly Changing Dimension (SCD) Type 2** awareness: recognizing historical
  versioning in source data and making an explicit, documented simplification
  trade-off for the dimension model
- **Referential integrity handling**: closing dimension/fact gaps with an
  explicit "Unknown/Discontinued" placeholder key instead of leaving nulls
- **Idempotent pipeline design**: every table write uses
  `mode("overwrite").option("overwriteSchema", "true")`, making the pipeline
  safe to re-run end-to-end without manual intervention (e.g. via a Databricks
  Job)
- **Surrogate key generation** and star schema modeling for BI consumption

## Documentation

Full script-by-script documentation — including every data quality issue found,
the diagnostic steps taken, and the reasoning behind each fix — is available in
[`04_DOCS`](./04_DOCS):

- [Bronze Layer Documentation](./04_DOCS/01_Bronze_Layer_Doc.md)
- [Silver Layer Documentation](./04_DOCS/02_Silver_Layer_Doc.md)
- [Gold Layer Documentation](./04_DOCS/03_Gold_Layer_Doc.md)

## Business Rules Applied

Beyond generic data cleaning, several transformations encode specific business
logic decisions:

- **CRM is the authoritative source for customer identity.** When both CRM and
  ERP provide overlapping attributes (e.g. gender), the CRM value takes
  precedence; the ERP value is only used as a fallback when CRM's is missing.
  This reflects CRM's role as the primary customer-relationship system in this
  integration scenario.
- **Sales amount must always equal quantity × price.** Any row where this
  didn't hold — due to a null, negative, or inconsistent price — was corrected
  by rebuilding price first, then sales amount, rather than trusting either
  field blindly.
- **A product's "current" version is the one with no end date.** Products carry
  historical price versions (SCD Type 2 pattern); the dimension model
  deliberately keeps only the version currently in effect, treating historical
  pricing as out of scope for this iteration of the model.
- **Country and marital/gender values are standardized to one canonical form**
  per category (e.g. `US`/`USA` → `United States`; `S`/`M` → `Single`/`Married`),
  so downstream aggregations group correctly instead of splitting the same
  real-world value across multiple labels.
- **A sale referencing a fully discontinued product is still a valid sale.**
  Rather than dropping or nulling these transactions, they're explicitly linked
  to an "Unknown/Discontinued Product" placeholder — preserving total revenue
  and order counts while still flagging that no current product record backs
  that line item.
- **Junk/test records are excluded, not silently kept.** Customer keys not
  matching the expected identifier format (with no accompanying name data) were
  treated as non-customers and removed at the source, rather than flowing
  downstream and appearing as anonymous rows in the gold layer.

## 🎯 Learning Objectives & Key Takeaways

This project was built primarily as a hands-on learning exercise in Apache
Spark and the Databricks platform — going beyond tutorials into a scenario
messy enough to force real engineering decisions.

**Goal:** Learn how to perform ingestion and transformation in Databricks
using PySpark, and understand the platform's core building blocks — catalog,
schemas, volumes, and tables — while applying the Medallion Architecture
end-to-end.

**What I learned:**

- 🔧 **Spark & Databricks fundamentals** — ingesting raw files into Delta
  tables, transforming data with the DataFrame API and Spark SQL, and
  understanding how catalog → schema → volumes/tables fit together
  (volumes for raw files, tables for structured data)
- 🕒 **Slowly Changing Dimensions (SCD Type 2)** — recognizing when a source
  system tracks historical versions of an entity (e.g. products with
  changing prices over time), and how to reason about which version is
  "current" vs. historical
- 📦 **Active vs. discontinued records** — handling entities that no longer
  exist in their source dimension but still appear in historical facts,
  without silently dropping or nulling that data
- 🧠 **Business context is a prerequisite, not an afterthought** — the
  correct design (which value wins in a conflict, which record is
  "current", how to treat orphaned foreign keys) can't be decided from the
  data alone; it depends on understanding the business rules and the
  domain's point of view
- 🔤 **Know your abbreviations before you join** — source systems rarely
  document their codes and acronyms. Misreading a column's meaning or an
  abbreviation is an easy way to silently corrupt a join or overwrite the
  wrong table/column, so validating what each field actually represents
  came before writing any transformation logic

**Challenges faced:**

- Reconciling two source systems (CRM and ERP) that described the same
  real-world entities differently, with no shared documentation to fall
  back on
- Deciding how much historical complexity (SCD Type 2) to model versus
  simplify, and documenting that trade-off explicitly instead of hiding it
- Keeping naming and structure (scripts, docs, README) consistent as the
  project evolved — small inconsistencies compound quickly across a
  multi-layer pipeline

## Data Source

Built on the [AdventureWorks](https://learn.microsoft.com/en-us/sql/samples/adventureworks-install-configure)
sample dataset (Microsoft), split across simulated CRM and ERP source files to
mirror a multi-system integration scenario.

## Author

**Helton Santos** — [github.com/heltonwo](https://github.com/heltonwo)
