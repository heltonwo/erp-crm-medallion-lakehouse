# Bronze Layer — Documentation

Detailed script-by-script documentation: what each notebook does, what problems
were found, and what decisions were made.

---

## `00_environment_setup`

**Target:** the `acdproj` catalog and its `bronze`, `silver`, `gold` schemas.

### What this script does
Prepares the Databricks workspace so the rest of the pipeline has somewhere to
write to — creating the catalog and the three schemas that structure the whole
medallion architecture.

### Problem being solved
There was no working environment yet — no catalog, no schemas — so every
downstream `saveAsTable("acdproj.bronze....")` call would otherwise fail with
"schema does not exist." This script exists purely to remove that blocker,
assuming the catalog itself is either pre-existing or created here.

### Decisions made
- **`CREATE CATALOG IF NOT EXISTS` / `CREATE SCHEMA IF NOT EXISTS`, never `DROP`,
  in the main flow:** the first draft of this script included
  `DROP SCHEMA ... CASCADE` before recreating each schema. This was deliberately
  rejected in favor of a purely additive, idempotent approach — a `DROP CASCADE`
  run automatically at the start of a pipeline would silently destroy all
  existing tables (including hours of completed silver/gold work) the next time
  the environment setup ran as part of an automated Job. `CREATE IF NOT EXISTS`
  achieves the same goal (make sure the structure exists) without any
  destructive side effect.
- **Kept separate from every data-loading notebook**, rather than repeating
  catalog/schema creation logic at the top of each one. Environment setup and
  data loading are different responsibilities: one is infrastructure, run once;
  the other is data movement, run repeatedly. Mixing them increases the risk of
  an infrastructure command (especially a destructive one) accidentally running
  as part of a routine data refresh.
- **Loop over a `schemas` list** (`["bronze", "silver", "gold"]`) instead of three
  separate `CREATE SCHEMA` statements — consistent with the loop-based style used
  throughout the rest of the project (rename maps, file-loading loops, etc.).
- **A destructive reset option was intentionally NOT included in the automated
  path.** If the environment ever needs a full wipe, that would be a separate,
  manually-triggered, clearly-labeled block — never part of the routine setup
  flow or any scheduled Job.

### Final code
```python
schemas = ["bronze", "silver", "gold"]

spark.sql("CREATE CATALOG IF NOT EXISTS acdproj")
spark.sql("USE CATALOG acdproj")

for schema in schemas:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema}")
    print(f"Schema '{schema}' ready.")
```

---

## `01_bronze`

**Sources:** individual CSV files under
`/Volumes/dbacademy/sc_raw/raw_sources/source_crm/` and `source_erp/`
**Target:** one bronze table per source file (`acdproj.bronze.crm_*`,
`acdproj.bronze.erp_*`)

### What this script does
The original, manual, one-file-at-a-time version of the bronze load — read a
single CSV, inspect it with `display()`, and write it to its own bronze table.
This was deliberately built and run file-by-file first (rather than jumping
straight to a loop), specifically to learn the read → inspect → write flow
before automating it.

### Problem being solved
Getting raw CSV data (customer, product, sales, and ERP source files) into
queryable Delta tables, as the entry point of the whole medallion pipeline.

### Problems found
1. **Two-part vs three-part table names:** an early attempt used
   `saveAsTable("bronze.crm_cust_info")` (schema + table only), which resolved
   against whatever catalog happened to be active — not necessarily `acdproj` —
   so the table didn't appear where expected. Fixed by always using the fully
   qualified three-part name, `acdproj.bronze.crm_cust_info`.
2. **Volume paths vs table references — a foundational distinction learned
   here:** loading a raw file requires the full filesystem path
   (`/Volumes/dbacademy/sc_raw/raw_sources/...`), because at that point the data
   doesn't exist as a table yet. Once written with `saveAsTable`, later notebooks
   only ever need `schema.table_name` to reference it — no path required. This
   became one of the core structural lessons of the whole project (catalog →
   schemas → tables/volumes → tables hold data, volumes hold raw files).

### Decisions made
- **One file at a time, on purpose, before automating:** each source CSV
  (`cust_info.csv`, `prd_info.csv`, `sales_details.csv` for CRM; the three ERP
  files) was loaded individually first, to build confidence in the
  `spark.read` → `display()` → `saveAsTable()` sequence before wrapping it in a
  loop.
- **Consistent read options across every file:** `.option("header", "true")` and
  `.option("inferSchema", "true")` on every load — letting Spark detect column
  types automatically rather than declaring a schema manually for six different
  source files at this early stage.
- **No transformation of any kind at this layer** — bronze is a raw, 1:1 copy of
  the source data in Delta format. Any cleaning, renaming, or validation happens
  starting in the silver layer, not here.

### Example (repeated per file, with the table name changed each time)
```python
df = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv("/Volumes/dbacademy/sc_raw/raw_sources/source_crm/cust_info.csv")
)

display(df)
df.write.mode("overwrite").saveAsTable("acdproj.bronze.crm_cust_info")
```

---

## `01_bronze_updated`

**Sources:** every `.csv` file under `/Volumes/dbacademy/sc_raw/raw_sources/source_crm/`
and `/Volumes/dbacademy/sc_raw/raw_sources/source_erp/`
**Target:** `acdproj.bronze.crm_*` and `acdproj.bronze.erp_*` (one table per file)

### What this script does
The automated, DRY (Don't Repeat Yourself) version of `01_bronze` — once the
manual read → inspect → write flow was understood and trusted file-by-file, this
script replaces six near-identical manual blocks with two loops (one for CRM,
one for ERP) that discover and load every CSV in a folder automatically.

### Problem being solved
`01_bronze` required copy-pasting and manually editing the same 5 lines of code
six times, changing only the file path and table name — repetitive and
error-prone (a copy-paste mistake in one of the six blocks would be easy to miss).
This script removes that repetition entirely.

### Problems found
1. **Prefix bug during the first loop-based draft:** an early version of the CRM
   loop reused the ERP prefix from the ERP script it was copied from
   (`f"acdproj.bronze.erp_{table_name}"` inside what was meant to be the CRM
   loop), which would have written CRM tables under the wrong naming convention.
   Caught and corrected to `crm_` before running.

### Decisions made
- **`dbutils.fs.ls(folder_path)` to discover files dynamically**, instead of
  hardcoding a list of filenames — the loop automatically picks up every `.csv`
  in the folder, so adding a new source file later wouldn't require touching this
  script's code at all.
- **Derive the table name from the filename itself**
  (`file.name.replace(".csv", "").lower()`), combined with a fixed prefix
  (`crm_` or `erp_`) — keeps the naming convention consistent and predictable
  without manual repetition.
- **One loop per source system (CRM, ERP), not one loop for everything:** kept
  separate because the two systems need different table-name prefixes
  (`crm_` vs `erp_`) and live in different subfolders — merging them into a single
  loop would have required extra branching logic for no real benefit at this
  scale (6 files total).
- **Idempotent write adopted here too:**
  `mode("overwrite").option("overwriteSchema", "true")` — brought in during the
  project's later idempotency pass, so this script (like the silver and gold
  ones) can be re-run safely as part of an orchestrated Job without manual
  intervention.
- **Still no transformation logic** — this remains a pure ingestion script; all
  cleaning starts in the silver layer, unchanged from the design established in
  `01_bronze`.

### Final code
```python
# CRM
folder_path = "/Volumes/dbacademy/sc_raw/raw_sources/source_crm/"
files = dbutils.fs.ls(folder_path)

for file in files:
    if file.name.endswith(".csv"):
        table_name = file.name.replace(".csv", "").lower()
        full_table_name = f"acdproj.bronze.crm_{table_name}"

        print(f"Loading {file.path} -> {full_table_name}")

        df = (
            spark.read
            .option("header", "true")
            .option("inferSchema", "true")
            .csv(file.path)
        )

        df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(full_table_name)

print("Done loading all CRM bronze tables.")
```

```python
# ERP
folder_path = "/Volumes/dbacademy/sc_raw/raw_sources/source_erp/"
files = dbutils.fs.ls(folder_path)

for file in files:
    if file.name.endswith(".csv"):
        table_name = file.name.replace(".csv", "").lower()
        full_table_name = f"acdproj.bronze.erp_{table_name}"

        print(f"Loading {file.path} -> {full_table_name}")

        df = (
            spark.read
            .option("header", "true")
            .option("inferSchema", "true")
            .csv(file.path)
        )

        df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(full_table_name)

print("Done loading all ERP bronze tables.")
```

---

## Summary — How the Bronze Layer Evolved

| Stage | Approach | Why it changed |
|---|---|---|
| `01_bronze` | One file, fully manual, repeated 6 times | Needed to learn and trust the core read → inspect → write flow first |
| `01_bronze_updated` | Two loops (CRM, ERP), auto-discovering files | Removed repetition once the flow was understood; safer against copy-paste mistakes |
| (idempotency pass) | Added `overwriteSchema` to both loops | Made the ingestion step safe to re-run automatically as part of a Job |

**Key takeaway:** the bronze layer's own evolution mirrors a pattern that
repeated throughout the whole project — build it manually and understand it
first, then automate once the logic is trusted, and finally revisit for
idempotency once orchestration became a goal. No transformation logic was ever
introduced at this layer; bronze stayed a faithful, schema-inferred, raw copy of
the source files throughout every revision.
