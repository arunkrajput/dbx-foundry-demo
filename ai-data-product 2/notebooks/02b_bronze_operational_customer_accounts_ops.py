# Databricks notebook source
# MAGIC %md
# MAGIC # 02b · BRONZE · OPERATIONAL product `retail_banking.customer_accounts_ops`
# MAGIC Fast, as-received, governed. For each new file from on-prem:
# MAGIC 1. **Audit** – append every row untouched to `bronze.customer_accounts_raw` (internal).
# MAGIC 2. **Flag** – check each row against `contracts/customer_accounts_ops.yml` and record `_dq_status` / `_dq_errors`.
# MAGIC    Failed rows are **kept and flagged**, not hidden: operations needs to see what the source sent.
# MAGIC 3. **Publish** – MERGE the latest row per account into the operational product (values stay STRING).
# MAGIC 4. **Quarantine** only rows with no `account_id`, because they cannot be keyed.

# COMMAND ----------

from delta.tables import DeltaTable
from pyspark.sql import functions as F

from dplib import auto_load, latest_per_key, load_contract, log_run, new_run_id, quarantine, validate

C = load_contract("customer_accounts_ops")
OUT, RAW, PK = C["output"]["table"], C["output"]["raw_table"], C["output"]["primary_key"]
COLS = [c["name"] for c in C["schema"]]


def process_batch(batch_df, batch_id):
    spark_ = batch_df.sparkSession
    rows_in = batch_df.count()
    if rows_in == 0:
        return
    run_id = new_run_id()

    # 1. AUDIT: everything, exactly as received
    batch_df.withColumn("_ingested_at", F.current_timestamp()).write.mode("append").saveAsTable(RAW)

    # 2. FLAG
    checked = validate(batch_df, C["schema"]).withColumn(
        "_dq_status", F.when(F.size("_errors") == 0, "passed").otherwise("failed"))
    no_key = checked.filter(F.col(PK).isNull() | (F.trim(F.col(PK)) == ""))
    keyed = checked.filter(F.col(PK).isNotNull() & (F.trim(F.col(PK)) != ""))

    # 4. QUARANTINE unkeyable rows
    quarantine(no_key, C, run_id)

    # 3. PUBLISH latest row per account, flags included
    latest = latest_per_key(keyed, PK).select(
        *COLS, "_dq_status", F.col("_errors").alias("_dq_errors"), "_source_file",
        F.current_timestamp().alias("_updated_at"))
    (DeltaTable.forName(spark_, OUT).alias("t")
        .merge(latest.alias("s"), f"t.{PK} = s.{PK}")
        .whenMatchedUpdateAll().whenNotMatchedInsertAll().execute())

    rows_passed = checked.filter("_dq_status = 'passed'").count()
    files = [r[0] for r in batch_df.select("_source_file").distinct().collect()]
    log_run(spark_, C, run_id, batch_id, rows_in, rows_passed, files)


auto_load(spark, C, process_batch)

# COMMAND ----------

display(spark.sql(f"SELECT _dq_status, count(*) AS accounts FROM {OUT} GROUP BY _dq_status"))

# COMMAND ----------

display(spark.sql(f"SELECT account_id, _dq_errors, _source_file FROM {OUT} "
                  f"WHERE _dq_status = 'failed' LIMIT 20"))
