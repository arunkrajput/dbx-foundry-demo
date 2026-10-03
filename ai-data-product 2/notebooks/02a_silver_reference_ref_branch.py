# Databricks notebook source
# MAGIC %md
# MAGIC # 02a · SILVER · REFERENCE product `reference.ref_branch`
# MAGIC Reference data must be fully clean, because other products validate against it:
# MAGIC - every row is checked against `contracts/ref_branch.yml`;
# MAGIC - **if any row fails, the whole file is rejected** (quarantined) and the current reference set stays as it was;
# MAGIC - otherwise rows are typed and MERGEd (one row per branch code; removed branches are kept with `active = false` by the source).

# COMMAND ----------

from delta.tables import DeltaTable
from pyspark.sql import functions as F

from dplib import auto_load, latest_per_key, load_contract, log_run, new_run_id, quarantine, typed_select, validate

C = load_contract("ref_branch")
OUT, PK = C["output"]["table"], C["output"]["primary_key"]


def process_batch(batch_df, batch_id):
    spark_ = batch_df.sparkSession
    rows_in = batch_df.count()
    if rows_in == 0:
        return
    run_id = new_run_id()
    checked = validate(batch_df, C["schema"])
    failed = checked.filter(F.size("_errors") > 0)
    n_failed = failed.count()
    files = [r[0] for r in batch_df.select("_source_file").distinct().collect()]

    if n_failed:
        # All-or-nothing: quarantine every row, publish nothing, keep yesterday's reference set
        quarantine(checked.withColumn("_errors", F.when(F.size("_errors") > 0, F.col("_errors"))
                                      .otherwise(F.array(F.lit("file rejected: other rows failed")))), C, run_id)
        log_run(spark_, C, run_id, batch_id, rows_in, 0, files)
        print(f"REJECTED {files}: {n_failed} invalid rows. Reference data unchanged.")
        return

    typed = latest_per_key(checked, PK).select(*typed_select(checked, C["schema"]),
                                               F.current_timestamp().alias("_updated_at"))
    (DeltaTable.forName(spark_, OUT).alias("t")
        .merge(typed.alias("s"), f"t.{PK} = s.{PK}")
        .whenMatchedUpdateAll().whenNotMatchedInsertAll().execute())
    log_run(spark_, C, run_id, batch_id, rows_in, rows_in, files)


auto_load(spark, C, process_batch)

# COMMAND ----------

display(spark.table(OUT).orderBy(PK))
