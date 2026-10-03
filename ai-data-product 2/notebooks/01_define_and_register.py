# Databricks notebook source
# MAGIC %md
# MAGIC # 01 · DEFINE + PROVISION + REGISTER every data product
# MAGIC Reads **every** contract in `contracts/` and, for each one:
# MAGIC 1. **Provision** its output port in the layer the contract names: bronze = operational, silver = reference, gold = consumer-aligned.
# MAGIC 2. **Register** it in `governance.data_product_registry` with its layer, product type and inputs.
# MAGIC
# MAGIC It also creates the shared run log, quarantine and the `product_health` view. Idempotent: re-run whenever a contract changes.
# MAGIC
# MAGIC **Ran the first version of this guide?** Set `reset_dev = yes` once to drop the old single-product objects, then set it back to `no`.

# COMMAND ----------

from dplib import all_contracts, ensure_output_table

dbutils.widgets.dropdown("reset_dev", "no", ["no", "yes"])
contracts = all_contracts()
GOV = "dp_dev.governance"
for c in contracts:
    P = c["product"]
    print(f"{P['layer']:<7} {P['product_type']:<17} {P['id']} v{P['version']} -> {c['output']['table']}")

# COMMAND ----------

if dbutils.widgets.get("reset_dev") == "yes":
    for t in ["dp_dev.gold.customer_accounts", "dp_dev.bronze.customer_accounts_raw",
              "dp_dev.bronze.customer_accounts_ops", "dp_dev.silver.ref_branch",
              f"{GOV}.data_product_registry", f"{GOV}.dq_runs", f"{GOV}.quarantine"]:
        spark.sql(f"DROP TABLE IF EXISTS {t}")
    spark.sql(f"DROP VIEW IF EXISTS {GOV}.product_health")
    for d in ["customer_accounts", "customer_accounts_ops", "ref_branch"]:
        try:
            dbutils.fs.rm(f"/Volumes/dp_dev/staging/checkpoints/{d}", True)
        except Exception:
            pass
    print("Dev objects reset. Set reset_dev back to 'no'.")

# COMMAND ----------

# MAGIC %md ## Shared governance tables

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {GOV}.data_product_registry (
  product_id STRING, name STRING, domain STRING, layer STRING, product_type STRING,
  version STRING, status STRING, owner STRING, description STRING, usage STRING,
  output_table STRING, primary_key STRING, upstreams ARRAY<STRING>, inputs ARRAY<STRING>,
  freshness_sla_hours INT, min_valid_ratio DOUBLE, contract_yaml STRING,
  registered_at TIMESTAMP, last_refreshed_at TIMESTAMP
) COMMENT 'Catalog of data products: bronze operational, silver reference, gold consumer-aligned'
""")
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {GOV}.dq_runs (
  run_id STRING, product_id STRING, product_version STRING, batch_id BIGINT, run_at TIMESTAMP,
  rows_in BIGINT, rows_valid BIGINT, rows_quarantined BIGINT, source_files ARRAY<STRING>
) COMMENT 'One row per load per product'
""")
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {GOV}.quarantine (
  product_id STRING, run_id STRING, record STRING, errors ARRAY<STRING>,
  source_file STRING, quarantined_at TIMESTAMP
) COMMENT 'Rows a product rejected: no key, failed rules, or failed referential integrity'
""")

# Raw audit trail behind the operational product. Internal: never granted to consumers.
spark.sql("""
CREATE TABLE IF NOT EXISTS dp_dev.bronze.customer_accounts_raw (
  account_id STRING, customer_name STRING, ic_number STRING, segment STRING, branch_code STRING,
  account_open_date STRING, balance_myr STRING, _rescued_data STRING, _source_file STRING,
  _ingested_at TIMESTAMP
) COMMENT 'Append-only copy of every row received. Internal only.'
""")

# COMMAND ----------

# MAGIC %md ## Provision each output port and register the product

# COMMAND ----------

rows = []
for c in contracts:
    ensure_output_table(spark, c)
    P = c["product"]
    rows.append((
        P["id"], P["name"], P["domain"], P["layer"], P["product_type"], P["version"], P["status"],
        P["owner"], " ".join(P["description"].split()),
        " ".join(c.get("agent", {}).get("usage", "").split()),
        c["output"]["table"], c["output"]["primary_key"], P.get("upstreams", []), P.get("inputs", []),
        int(c["sla"]["freshness_hours"]), float(c["sla"]["min_valid_ratio"]),
        open(c["_path"]).read(),
    ))

spark.createDataFrame(rows, """product_id string, name string, domain string, layer string,
    product_type string, version string, status string, owner string, description string, usage string,
    output_table string, primary_key string, upstreams array<string>, inputs array<string>,
    freshness_sla_hours int, min_valid_ratio double, contract_yaml string""").createOrReplaceTempView("incoming")

spark.sql(f"""
MERGE INTO {GOV}.data_product_registry t USING incoming s ON t.product_id = s.product_id
WHEN MATCHED THEN UPDATE SET
  name = s.name, domain = s.domain, layer = s.layer, product_type = s.product_type,
  version = s.version, status = s.status, owner = s.owner, description = s.description,
  usage = s.usage, output_table = s.output_table, primary_key = s.primary_key,
  upstreams = s.upstreams, inputs = s.inputs, freshness_sla_hours = s.freshness_sla_hours,
  min_valid_ratio = s.min_valid_ratio, contract_yaml = s.contract_yaml
WHEN NOT MATCHED THEN INSERT (
  product_id, name, domain, layer, product_type, version, status, owner, description, usage,
  output_table, primary_key, upstreams, inputs, freshness_sla_hours, min_valid_ratio,
  contract_yaml, registered_at, last_refreshed_at)
VALUES (
  s.product_id, s.name, s.domain, s.layer, s.product_type, s.version, s.status, s.owner,
  s.description, s.usage, s.output_table, s.primary_key, s.upstreams, s.inputs,
  s.freshness_sla_hours, s.min_valid_ratio, s.contract_yaml, current_timestamp(), NULL)
""")

# COMMAND ----------

# MAGIC %md ## Health view: one row per product (the agent also checks each product's inputs)

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE VIEW {GOV}.product_health AS
WITH last_run AS (
  SELECT *, row_number() OVER (PARTITION BY product_id ORDER BY run_at DESC) AS rn FROM {GOV}.dq_runs
)
SELECT
  r.product_id, r.layer, r.product_type, r.version, r.status, r.inputs,
  r.freshness_sla_hours, r.min_valid_ratio,
  l.run_at AS last_load_at, l.rows_in, l.rows_valid, l.rows_quarantined,
  round(l.rows_valid / nullif(l.rows_in, 0), 4) AS valid_ratio,
  round((unix_timestamp(current_timestamp()) - unix_timestamp(l.run_at)) / 3600, 1) AS age_hours,
  coalesce(
    r.status = 'active'
    AND (unix_timestamp(current_timestamp()) - unix_timestamp(l.run_at)) / 3600 <= r.freshness_sla_hours
    AND l.rows_valid / nullif(l.rows_in, 0) >= r.min_valid_ratio,
    false) AS trustworthy
FROM {GOV}.data_product_registry r
LEFT JOIN last_run l ON r.product_id = l.product_id AND l.rn = 1
""")

display(spark.sql(f"SELECT product_id, layer, product_type, version, status, output_table, inputs "
                  f"FROM {GOV}.data_product_registry ORDER BY layer"))
