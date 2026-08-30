# Gold Layer — Documentation

Detailed script-by-script documentation of the star schema build: what each
notebook does, what problems were found, and what decisions were made.

---

## `03_01_gold_customers_dim`

**Sources:** `acdproj.silver.crm_customers`, `acdproj.silver.erp_customers`,
`acdproj.silver.erp_locations`
**Target:** `acdproj.gold.dim_customers`

### What this script does
Builds the single unified customer dimension by joining the CRM customer master
with the two ERP customer-related tables (birth date/gender, and country), then
adds a surrogate key.

### Problems found
1. **Overlapping attribute across sources:** both `crm_customers` and
   `erp_customers` carry a `customer_gender` column. When they exist in two
   sources, a rule is needed for which one "wins" — this isn't a data error, it's
   a modeling decision that has to be made explicitly.
2. **Join keys already aligned — this script benefited directly from silver-layer
   work:** because `crm_customers.customer_key`, `erp_customers.customer_id`
   (after the `NAS` prefix removal), and `erp_locations.customer_id` (after the
   hyphen removal) were all normalized to the same `AW` + 8-digit format back in
   the silver layer, the joins here needed no further key cleanup — a direct
   payoff of the earlier silver-layer fixes.

### Decisions made
- **`LEFT JOIN` from the CRM table, not `INNER JOIN`:** CRM is treated as the
  primary/master source, so every CRM customer is kept in the dimension even if a
  matching ERP record doesn't exist for some reason — an `INNER JOIN` would have
  silently dropped customers with incomplete ERP data.
- **Conflict resolution for `customer_gender`:** CRM value used first; ERP value
  used only as a fallback when CRM's value is `"n/a"`:
  ```python
  F.when(F.col("crm.customer_gender") != "n/a", F.col("crm.customer_gender"))
   .otherwise(F.col("erp.customer_gender"))
  ```
  CRM was chosen as authoritative because it's the primary customer-relationship
  system in this project's source structure.
- **Table aliases (`.alias("crm")`, `.alias("erp")`, `.alias("loc")`)** were
  required specifically because of the gender-column name collision above — without
  them, Spark can't disambiguate which table's `customer_gender` a given
  expression refers to.
- **Surrogate key generation:** `row_number().over(Window.orderBy("customer_key"))`
  — the standard pattern used across every gold dimension in this project, chosen
  over `monotonically_increasing_id()` for its predictable, sequential 1, 2, 3...
  output (acceptable here given the small dimension size; see the performance note
  in the summary section below).
- **Validation before finalizing:** row count was checked against the source
  (`crm_customers`) to confirm the joins didn't duplicate or drop rows
  unexpectedly — this is how the 4 invalid junk customer keys (see silver layer
  docs) were actually discovered: their presence in `crm_customers` produced rows
  in `dim_customers` with null names once surrogate keys were generated and
  inspected, which triggered going back to fix the issue at the silver source
  rather than patching it here in gold.

### Final write
```python
dim_customers.write.mode("overwrite").saveAsTable("acdproj.gold.dim_customers")
```

---

## `03_02_gold_products_dim`

**Sources:** `acdproj.silver.crm_products`, `acdproj.silver.erp_categories`
**Target:** `acdproj.gold.dim_products`

### What this script does
Builds the product dimension by joining the current-version product data with
the ERP category/subcategory table, then adds a surrogate key and an
unknown/discontinued placeholder row.

### Problems found
1. **`category_id` format mismatch between sources — the main bug of this
   script:** `crm_products.category_id` used hyphens (`CO-RF`, `AC-HE`), while
   `erp_categories.category_id` used underscores (`AC_BR`, `BI_MB`). Visually
   similar but not equal strings, so the initial join silently matched nothing —
   every `category`/`subcategory`/`maintenance` column came back null, with no
   error thrown (a `LEFT JOIN` with zero matches fails silently, unlike an error
   that would have flagged the problem immediately).
2. **`product_key_short` initially missing from the dimension:** the first
   version of this script only carried `product_key` (the full key) into
   `dim_products`. This wasn't discovered until building `fact_sales`, where the
   sales table's `product_key` turned out to reference the *short* key format, not
   the full one (see `03_03_gold_sales_fact` below) — the schema had to be
   revisited and extended after the fact.
3. **Schema-change conflict on re-write:** after adding `product_key_short`,
   `saveAsTable(..., mode="overwrite")` alone failed, because Delta Lake doesn't
   allow an `overwrite` to silently change a table's schema. This first required
   a manual `DROP TABLE` to work around — later replaced with the proper fix
   (`overwriteSchema` option) for all future schema changes.

### Decisions made
- **Keep only the current version of each product** (`product_end_date IS NULL`)
  before joining — a direct continuation of the SCD Type 2 simplification decided
  at the silver layer (see silver layer docs, `02_02_silver_product`). This
  reduces the row count from 397 (all historical versions) to 197 (only current
  products).
- **Fix the category_id format mismatch with `regexp_replace`:** replaced hyphens
  with underscores in `crm_products.category_id` before joining, to match the ERP
  side's format:
  ```python
  df_crmp.withColumn("category_id", F.regexp_replace(F.col("category_id"), "-", "_"))
  ```
- **Carry both `product_key` (full) and `product_key_short` into the dimension** —
  once the sales-table mismatch was discovered, both were kept: `product_key` for
  human-readable identification/reporting, `product_key_short` specifically to
  support the join from `fact_sales`.
- **Add an "Unknown/Discontinued Product" placeholder row** (`product_sk = -1`),
  built with `spark.createDataFrame([...], schema=dim_products.schema)` and merged
  in via `unionByName`. This is the "refinement" requested explicitly for this
  project, addressing the referential-integrity gap left by the SCD
  simplification: sales referencing a `product_key` whose every historical
  version has ended (fully discontinued, no "current" row survives the filter)
  would otherwise produce a `null` surrogate key in `fact_sales`. The placeholder
  gives them an explicit, queryable fallback instead. `-1` was chosen as a
  negative sentinel value specifically because real surrogate keys generated by
  `row_number()` are always positive — no risk of collision.
- **Idempotency fix adopted from this point forward:**
  `mode("overwrite").option("overwriteSchema", "true")` replaces the need for a
  manual `DROP TABLE` whenever this table's schema changes in the future.

### Final write
```python
dim_products.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("acdproj.gold.dim_products")
```

---

## `03_03_gold_sales_fact`

**Sources:** `acdproj.silver.crm_sales`, `acdproj.gold.dim_customers`,
`acdproj.gold.dim_products`
**Target:** `acdproj.gold.fact_sales`

### What this script does
Builds the central fact table by joining the cleaned sales transactions to both
dimensions, replacing natural keys with surrogate keys, and closing the
referential-integrity gaps discovered along the way.

### Problems found
1. **`customer_id` type/format mismatch:** `crm_sales.customer_id` is a plain
   integer (e.g. `11000`), while `dim_customers.customer_key` is a formatted
   string (`AW00011000`). The first join attempt threw
   `CAST_INVALID_INPUT: The value 'AW00011000' ... cannot be cast to "BIGINT"` —
   Spark tried to reconcile the type mismatch by casting the string side to a
   number, which failed outright rather than silently returning no matches (a
   more visible failure than the category_id issue in the products dimension).
2. **`product_key` in sales actually references the *short* key, not the full
   one:** the first join attempt (against `dim_products.product_key`, the full
   key) produced 60,487 null `product_sk` values out of a ~60,000-row fact table
   — essentially the entire table failed to match. Diagnosed methodically: a
   distinct-key comparison plus a `length()` check on both sides showed
   `crm_sales.product_key` values were 7–10 characters long, while
   `dim_products.product_key` values were 13–16 characters long — the lengths
   matched `product_key_short` instead. This traced directly back to the missing
   column identified while building this very script, which required revisiting
   `03_02_gold_products_dim` to add it.
3. **3,241 sales referencing genuinely discontinued products:** after fixing the
   key mismatch above, `product_sk` nulls dropped from 60,487 to 3,241. Confirmed
   by joining the unmatched `product_key` values back to the full (unfiltered)
   `crm_products` history: every one of them had a non-null `product_end_date`,
   confirming these are sales for products with no surviving "current" version —
   not a further key-matching bug, but the expected downstream consequence of the
   SCD Type 2 simplification made in the silver layer.

### Decisions made
- **Rebuild the formatted customer key before joining, rather than reformatting
  the dimension side:**
  ```python
  F.concat(F.lit("AW"), F.lpad(F.col("customer_id").cast("string"), 8, "0"))
  ```
  `.cast("string")` handles the *type* conversion (int → string); `lpad` and
  `concat` are *string functions* that then reconstruct the exact *format*
  (zero-padding + prefix) — a deliberate two-step distinction between changing a
  value's type versus changing its format/content.
- **Join against `product_key_short`, not `product_key`**, once the root cause was
  identified — the fix lived entirely in the join condition
  (`sales.product_key == prod.product_key_short`), no change needed to the sales
  table itself.
- **`coalesce(product_sk, -1)` to close the discontinued-product gap:** rather
  than leaving those 3,241 rows with a null foreign key, they're explicitly
  pointed at the `-1` "Unknown/Discontinued Product" placeholder row added to
  `dim_products` in the previous script. This makes the gap visible and queryable
  in any downstream BI tool instead of silently blank.
- **`customer_sk` was never coalesced** — no fallback was needed there, since the
  customer join produced zero nulls after the key-format fix. Only the product
  side required a placeholder, because only the product side had a case (SCD
  simplification) where a valid natural key could exist in sales with no
  corresponding dimension row.
- **Validation before writing:** counted nulls in both surrogate key columns
  separately (printed, not just chained into one combined count) after every fix,
  confirming `customer_sk` nulls = 0 throughout, and `product_sk` nulls dropped in
  two stages (60,487 → 3,241 → 0, once the placeholder was introduced) before
  considering the table ready to save.
- **Idempotent write from the start:**
  `mode("overwrite").option("overwriteSchema", "true")`, consistent with the fix
  adopted in the products dimension script.

### Final write
```python
fact_sales.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("acdproj.gold.fact_sales")
```

---

## Summary — The Star Schema

```
dim_customers (customer_sk PK)  ──┐
                                    ├──► fact_sales (customer_sk FK, product_sk FK)
dim_products  (product_sk PK)   ──┘
```

- `dim_customers`: CRM + ERP customer + ERP location, unified, deduplicated of
  4 junk records, one surrogate key per real customer.
- `dim_products`: current-version-only products (SCD Type 2 simplified) + ERP
  categories, plus one placeholder row (`product_sk = -1`) for discontinued
  products with no surviving current version.
- `fact_sales`: every sales transaction, fully resolved to surrogate keys —
  zero nulls in either foreign key, with the placeholder absorbing the
  legitimate referential gap left by the dimension's SCD simplification.

## Summary — Bugs Found Across the Gold Layer, by Type

| Bug type | Where | Symptom | How found |
|---|---|---|---|
| Silent join mismatch (format) | `dim_products` (category_id: hyphen vs underscore) | All joined columns null, no error | Manual side-by-side inspection of sample values |
| Silent join mismatch (format) | `fact_sales` (product_key vs product_key_short) | 60,487 of ~60,000 rows had null product_sk | `length()` comparison on both join keys |
| Type mismatch, loud failure | `fact_sales` (customer_id int vs customer_key string) | Job crashed: `CAST_INVALID_INPUT` | Error message pointed directly at the cast attempt |
| Missing column, discovered late | `dim_products` → `fact_sales` | Couldn't join on the right key at all | Realized only while building the *next* script downstream |
| Schema evolution conflict | `dim_products` write | `saveAsTable` overwrite rejected the new column | Delta Lake's default schema-enforcement behavior |
| Referential integrity gap (by design, not a bug) | `fact_sales` product_sk | 3,241 nulls remained after the format fix | Traced back to `product_end_date` on every unmatched key |

**Pattern worth noting for future projects:** two of the three join failures were
*silent* (no error, just null results) rather than *loud* (a thrown exception).
The type-mismatch case was caught immediately because Spark refused to proceed;
the two format-mismatch cases required actively comparing sample values and
string lengths on both sides of a join before trusting that "it probably worked."
A `LEFT JOIN` returning unexpectedly high null counts on the joined side is the
main practical signal to watch for.
