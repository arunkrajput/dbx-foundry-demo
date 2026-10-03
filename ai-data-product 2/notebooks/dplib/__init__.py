"""Shared data product lifecycle helpers, driven entirely by the YAML contracts.

Notebooks in ../notebooks import this with `from dplib import ...` (Git folders put the
notebook's directory on sys.path).
"""
from __future__ import annotations

import uuid
from pathlib import Path

import yaml
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

CONTRACTS_DIR = Path(__file__).resolve().parent.parent.parent / "contracts"


# --------------------------------------------------------------------------- contracts
def load_contract(name: str) -> dict:
    """name = file stem, e.g. 'customer_accounts_ops'."""
    c = yaml.safe_load((CONTRACTS_DIR / f"{name}.yml").read_text())
    c["_path"] = str(CONTRACTS_DIR / f"{name}.yml")
    return c


def all_contracts() -> list[dict]:
    return [load_contract(p.stem) for p in sorted(CONTRACTS_DIR.glob("*.yml"))]


def gov_schema(contract: dict) -> str:
    return contract["output"]["table"].split(".")[0] + ".governance"


def _q(s: str) -> str:
    return str(s).strip().replace("\\", "\\\\").replace("'", "\\'")


# --------------------------------------------------------------------------- provisioning
def ensure_output_table(spark: SparkSession, c: dict) -> None:
    """Create the product's output port from its contract (types, comments, tags)."""
    P, out = c["product"], c["output"]
    as_received = out.get("storage") == "as_received"
    cols = []
    for col in c["schema"]:
        typ = "STRING" if as_received else col["type"]
        not_null = " NOT NULL" if col.get("required") and not as_received else ""
        cols.append(f"{col['name']} {typ}{not_null} COMMENT '{_q(col['description'])}'")
    if P["layer"] == "bronze":
        cols += ["_dq_status STRING COMMENT 'passed or failed against the contract'",
                 "_dq_errors ARRAY<STRING> COMMENT 'Rule violations for this row'",
                 "_source_file STRING COMMENT 'File the row last arrived in'"]
    cols += ["_updated_at TIMESTAMP COMMENT 'When the row was last written'"]

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {out['table']} ({', '.join(cols)})
        COMMENT '{_q(P['description'])}'
        TBLPROPERTIES ('data_product.id' = '{P['id']}', 'delta.enableChangeDataFeed' = 'true')""")
    spark.sql(f"""ALTER TABLE {out['table']} SET TAGS (
        'data_product' = '{P['id']}', 'layer' = '{P['layer']}', 'product_type' = '{P['product_type']}',
        'domain' = '{P['domain']}', 'version' = '{P['version']}', 'status' = '{P['status']}')""")
    for col in c["schema"]:
        if col.get("pii"):
            spark.sql(f"ALTER TABLE {out['table']} ALTER COLUMN {col['name']} SET TAGS ('pii' = 'true')")


# --------------------------------------------------------------------------- validation
def validate(df: DataFrame, schema: list[dict]) -> DataFrame:
    """Adds `_errors` (array<string>) from the contract rules; empty = row passes."""
    checks = []
    for c in schema:
        name, col = c["name"], F.col(c["name"])
        if c.get("required"):
            checks.append(F.when(col.isNull() | (F.trim(col.cast("string")) == ""), F.lit(f"{name}: required")))
        if "pattern" in c:
            checks.append(F.when(col.isNotNull() & ~col.rlike(c["pattern"]), F.lit(f"{name}: bad format")))
        if "allowed_values" in c:
            checks.append(F.when(col.isNotNull() & ~col.isin(c["allowed_values"]),
                                 F.lit(f"{name}: not an allowed value")))
        if c["type"] != "STRING":
            checks.append(F.when(col.isNotNull() & F.expr(f"try_cast({name} AS {c['type']})").isNull(),
                                 F.lit(f"{name}: not a valid {c['type']}")))
        if "min" in c:
            checks.append(F.when(F.expr(f"try_cast({name} AS DOUBLE)") < F.lit(c["min"]),
                                 F.lit(f"{name}: below minimum {c['min']}")))
    return df.withColumn("_errors", F.filter(F.array(*checks), lambda e: e.isNotNull()))


def latest_per_key(df: DataFrame, key: str, order_col: str = "_source_file") -> DataFrame:
    w = Window.partitionBy(key).orderBy(F.desc(order_col))
    return df.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn")


def typed_select(df: DataFrame, schema: list[dict]) -> list:
    return [F.expr(f"CAST({c['name']} AS {c['type']})").alias(c["name"]) for c in schema]


# --------------------------------------------------------------------------- observability
def quarantine(df: DataFrame, c: dict, run_id: str, errors_col: str = "_errors") -> None:
    cols = [x["name"] for x in c["schema"] if x["name"] in df.columns]
    src = F.col("_source_file") if "_source_file" in df.columns else F.lit(None).cast("string")
    (df.select(F.lit(c["product"]["id"]).alias("product_id"), F.lit(run_id).alias("run_id"),
               F.to_json(F.struct(*cols)).alias("record"), F.col(errors_col).alias("errors"),
               src.alias("source_file"), F.current_timestamp().alias("quarantined_at"))
       .write.mode("append").saveAsTable(f"{gov_schema(c)}.quarantine"))


def log_run(spark: SparkSession, c: dict, run_id: str, batch_id: int,
            rows_in: int, rows_valid: int, files: list[str]) -> None:
    P, gov = c["product"], gov_schema(c)
    (spark.createDataFrame(
        [(run_id, P["id"], P["version"], int(batch_id), int(rows_in), int(rows_valid),
          int(rows_in - rows_valid), files)],
        "run_id string, product_id string, product_version string, batch_id bigint, rows_in bigint, "
        "rows_valid bigint, rows_quarantined bigint, source_files array<string>")
     .withColumn("run_at", F.current_timestamp())
     .select("run_id", "product_id", "product_version", "batch_id", "run_at", "rows_in", "rows_valid",
             "rows_quarantined", "source_files")
     .write.mode("append").saveAsTable(f"{gov}.dq_runs"))
    spark.sql(f"UPDATE {gov}.data_product_registry SET last_refreshed_at = current_timestamp() "
              f"WHERE product_id = '{P['id']}'")
    print(f"[{P['id']}] {rows_in} in, {rows_valid} valid, {rows_in - rows_valid} failed, files={files}")


def new_run_id() -> str:
    return str(uuid.uuid4())


# --------------------------------------------------------------------------- ingestion
def auto_load(spark: SparkSession, c: dict, process_batch) -> None:
    """Auto Loader over the contract's landing path: only unseen files, then stop."""
    name = c["product"]["name"]
    catalog = c["output"]["table"].split(".")[0]
    checkpoint = f"/Volumes/{catalog}/staging/checkpoints/{name}"
    raw_schema = ", ".join(f"{col['name']} STRING" for col in c["schema"])
    stream = (spark.readStream.format("cloudFiles")
              .option("cloudFiles.format", c["source"]["format"])
              .option("cloudFiles.schemaLocation", f"{checkpoint}/schema")
              .option("header", "true")
              .option("rescuedDataColumn", "_rescued_data")
              .option("pathGlobFilter", c["source"]["file_pattern"])
              .schema(raw_schema)
              .load(c["source"]["landing_path"])
              .withColumn("_source_file", F.col("_metadata.file_path")))
    (stream.writeStream.foreachBatch(process_batch)
           .option("checkpointLocation", f"{checkpoint}/cp")
           .trigger(availableNow=True)
           .start()
           .awaitTermination())
