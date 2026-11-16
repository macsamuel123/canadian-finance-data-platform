# Canadian Finance Data Platform

**A Databricks lakehouse that gives an AI agent trustworthy financial data instead of letting it guess.**

This is the data layer behind [TrueNorth](https://github.com/macsamuel123/Financial_Agent), a governed AI financial agent for Canadian accounts. LLMs are unreliable at remembering yearly contribution limits or current interest rates, so this pipeline builds curated, validated tables of that data and serves them to the agent's Postgres database.

## Architecture

```
 Sources                 Bronze (raw)              Silver (clean)            Gold (serving)          Postgres (Neon)
 ───────                 ────────────              ──────────────            ──────────────          ───────────────
 CRA TFSA limits    ──▶  cra_limits_raw       ──▶  cra_limits_clean     ──▶  dim_tfsa_limits     ──▶  dim_tfsa_limits
 Bank of Canada     ──▶  boc_rates_raw        ──▶  boc_rates_clean      ──▶  fct_boc_policy_rates ──▶ fct_boc_policy_rates
   Valet API
 Synthetic core     ──▶  members_raw          ──▶  dim_member_scd2
   banking              member_transactions_raw     (SCD Type 2)
                                                                                      ▲
                                                                     TrueNorth agent reads these
```

## Notebooks

| Layer | Notebook | What it does |
|---|---|---|
| Bronze | `01_bronze/bronze_ingest` | Lands TFSA dollar limits (2009–2025) from the CRA and the overnight policy rate (series `V39079`) from the Bank of Canada Valet API, with source and ingestion-time columns on every row. If the API is unreachable, the existing rates table is kept instead of being overwritten with nothing. |
| Bronze | `01_bronze/synthetic_banking_generator` | Generates a reproducible core-banking dataset: 500 members and 100,000 transactions. |
| Silver | `02_silver/silver_transform` | Deduplicates and validates the reference data. |
| Silver | `02_silver/member_scd_type2` | Builds a member dimension that keeps full history (SCD Type 2) with one idempotent Delta `MERGE`. |
| Gold | `03_gold/gold_agg` | Publishes the curated tables the agent reads. |
| Serving | `04_serving/sync_gold_postgres` | Syncs gold tables into Postgres for the agent. |

## Design decisions

**Synthetic data that respects the rules.** Members are generated first, then transactions. A member's transactions only begin once they could legally hold the account: the later of 2009 (when the TFSA launched), the year they turned 18, and the year they became a resident. FHSA transactions start no earlier than April 1, 2023, when the account was introduced. Every transaction belongs to a real member, so there are no orphan rows.

**Reproducible randomness.** Every random column uses a fixed seed, and the partition count is fixed too. In Spark, seeds alone aren't enough: the values also depend on how the data is split across partitions. Fixing both means every run produces identical data, so downstream numbers and tests stay stable. The members table joins to transactions with a broadcast join, so the large side keeps its partitioning.

**Bronze stays raw, silver conforms.** Synthetic transactions keep the source system's labels (`DEPOSIT` / `WITHDRAWAL`). Conforming them to the platform's vocabulary is silver's job.

**Money is decimal, not float.** Amounts and limits are stored as `DECIMAL`, because floating-point money produces rounding errors.

**SCD Type 2 with one MERGE.** A hash of the tracked columns detects real changes. A staged merge then expires the old version and inserts the new one in a single atomic operation. Applying the same snapshot twice changes nothing, and the notebook proves it. This matters for audits that ask, "What did we know about this member on a given date?"

**An atomic, typed sync to Postgres.** Column types come from the Spark schema, so decimals stay `NUMERIC` and dates stay `DATE`. Each table loads into a staging table and is swapped in within one transaction, so re-runs never duplicate rows and the agent never reads a half-loaded table.

**Test data stays separate.** Synthetic members use IDs from 1000 up. IDs 1 to 6 are hand-planted test members in Postgres, each built to test one TFSA rule, and the pipeline never touches them.

## Validation

Every notebook ends with checks that fail the run if the data breaks a rule that downstream layers depend on, including:

- Row counts match what was generated or ingested
- Every transaction belongs to an existing member
- No transaction is dated before its member was eligible
- No FHSA transaction is dated before the account existed
- Every member has exactly one current SCD2 version, and no version ends before it starts
- Re-applying the same snapshot adds no rows (idempotency)
- TFSA limits start in 2009 and are all positive; no policy rate is negative

## Run order

1. `bronze_ingest`
2. `synthetic_banking_generator`
3. `silver_transform`
4. `member_scd_type2`
5. `gold_agg`
6. `sync_gold_postgres` (needs the Neon password stored as the Databricks secret `neon-scope` / `neon-password`)

## Stack

Databricks (Free Edition), PySpark, Delta Lake, Unity Catalog, Python, PostgreSQL on Neon, Bank of Canada Valet API

---

Built by **Fortune** · [GitHub](https://github.com/macsamuel123)
