# Databricks notebook source
# MAGIC %md
# MAGIC # 02c · GOLD · CONSUMER-ALIGNED product `retail_banking.customer_accounts`
# MAGIC Built from two other data products, never from files:
# MAGIC - **bronze operational** `customer_accounts_ops`: only rows with `_dq_status = 'passed'`
# MAGIC - **silver reference** `ref_branch`: referential integrity + enrichment (branch name, region)
# MAGIC
# MAGIC Incremental: only accounts whose operational row, or whose branch reference row, changed since the last build.
# MAGIC Rows whose branch code isn't in `ref_branch` are quarantined with the reason.

# COMMAND ----------

from delta.tables import DeltaTable
from pyspark.sql import functions as F

from dplib import load_contract, log_run, new_run_id, quarantine

C = load_contract("customer_accounts")
OPS = load_contract("customer_accounts_ops")["output"]["table"]
REF = load_contract("ref_branch")["output"]["table"]
OUT, PK = C["output"]["table"], C["output"]["primary_key"]

watermark = spark.sql(f"SELECT coalesce(max(_updated_at), TIMESTAMP'1900-01-01') FROM {OUT}").first()[0]
print(f"Building changes since {watermark}")

# COMMAND ----------

ops = spark.table(OPS).filter("_dq_status = 'passed'").alias("o")
ref = spark.table(REF).alias("r")

candidates = (ops.join(ref, F.col("o.branch_code") == F.col("r.branch_code"), "left")
                 .filter((F.col("o._updated_at") > F.lit(watermark)) | (F.col("r._updated_at") > F.lit(watermark))))

built = candidates.select(
    F.col("o.account_id").alias("account_id"),
    F.col("o.customer_name").alias("customer_name"),
    F.col("o.ic_number").alias("ic_number"),
    F.col("o.segment").alias("segment"),
    F.col("o.branch_code").alias("branch_code"),
    F.col("r.branch_name").alias("branch_name"),
    F.col("r.region").alias("region"),
    F.expr("CAST(o.account_open_date AS DATE)").alias("account_open_date"),
    F.expr("CAST(floor(months_between(current_date(), CAST(o.account_open_date AS DATE)) / 12) AS INT)")
     .alias("tenure_years"),
    F.expr("CAST(o.balance_myr AS DECIMAL(18,2))").alias("balance_myr"),
    F.col("o._source_file").alias("_source_file"),
    F.when(F.col("r.branch_code").isNull(), F.array(F.lit("branch_code: not in reference.ref_branch")))
     .otherwise(F.array().cast("array<string>")).alias("_errors"),
)

rows_in = built.count()
run_id = new_run_id()
if rows_in == 0:
    print("Nothing changed upstream.")
else:
    failed = built.filter(F.size("_errors") > 0)
    passed = built.filter(F.size("_errors") == 0)
    quarantine(failed, C, run_id)
    cols = [c["name"] for c in C["schema"]]
    (DeltaTable.forName(spark, OUT).alias("t")
        .merge(passed.select(*cols, F.current_timestamp().alias("_updated_at")).alias("s"), f"t.{PK} = s.{PK}")
        .whenMatchedUpdateAll().whenNotMatchedInsertAll().execute())
    log_run(spark, C, run_id, 0, rows_in, passed.count(), [OPS, REF])

# COMMAND ----------

display(spark.sql(f"SELECT region, segment, count(*) AS accounts FROM {OUT} GROUP BY ALL ORDER BY ALL"))
