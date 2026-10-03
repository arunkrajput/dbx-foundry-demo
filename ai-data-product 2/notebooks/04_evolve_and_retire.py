# Databricks notebook source
# MAGIC %md
# MAGIC # 04 · EVOLVE → DEPRECATE → RETIRE (any product, any layer)
# MAGIC - **check_contract** – compare the contract with the live table; additive = minor version, breaking = major.
# MAGIC - **apply_additive** – add new nullable columns, bump the registered version.
# MAGIC - **deprecate** – still served, but `product_health.trustworthy` turns false and agents warn users.
# MAGIC - **retire** – consumer access revoked; data kept for audit and retention.
# MAGIC
# MAGIC Deprecating or retiring an input (e.g. `ref_branch`) also makes products that depend on it untrustworthy,
# MAGIC because the agent checks the health of each product's inputs.

# COMMAND ----------

from dplib import all_contracts, load_contract

dbutils.widgets.dropdown("product", "customer_accounts",
                         ["customer_accounts", "customer_accounts_ops", "ref_branch"])
dbutils.widgets.dropdown("action", "check_contract", ["check_contract", "apply_additive", "deprecate", "retire"])
C = load_contract(dbutils.widgets.get("product"))
ACTION = dbutils.widgets.get("action")
P, OUT = C["product"], C["output"]["table"]
GOV = "dp_dev.governance"
CONSUMERS = "`dp_kyc_ops`, `dp_operations`, `dp_analysts`, `dp_ai_agents`"
AS_RECEIVED = C["output"].get("storage") == "as_received"

dependants = [c["product"]["id"] for c in all_contracts() if P["id"] in c["product"].get("inputs", [])]
print(f"{P['id']} ({P['layer']}, {P['product_type']}) v{P['version']}; used by: {dependants or 'nothing'}")

# COMMAND ----------

def diff_contract() -> dict:
    live = {f.name: f.dataType.simpleString().upper() for f in spark.table(OUT).schema
            if not f.name.startswith("_")}
    wanted = {c["name"]: ("STRING" if AS_RECEIVED else c["type"].upper().replace(" ", ""))
              for c in C["schema"]}
    return {"added": [c for c in wanted if c not in live],
            "removed": [c for c in live if c not in wanted],
            "retyped": [c for c in wanted if c in live and live[c] != wanted[c]]}

d = diff_contract()
breaking = bool(d["removed"] or d["retyped"])
print(d, "-> BREAKING: publish a new major version as a new table" if breaking else "-> additive or no change")

# COMMAND ----------

if ACTION == "apply_additive":
    if breaking:
        raise ValueError(f"Breaking change. Create {OUT}_v2, register it as a new product, deprecate v1, "
                         f"migrate {dependants or 'consumers'}, then retire v1.")
    for c in C["schema"]:
        if c["name"] in d["added"]:
            typ = "STRING" if AS_RECEIVED else c["type"]
            spark.sql(f"ALTER TABLE {OUT} ADD COLUMNS ({c['name']} {typ} COMMENT '{c['description']}')")
    spark.sql(f"ALTER TABLE {OUT} SET TAGS ('version' = '{P['version']}')")
    print(f"Added {d['added']}. Now re-run notebook 01 so the registry stores the new contract and version.")

elif ACTION == "deprecate":
    spark.sql(f"UPDATE {GOV}.data_product_registry SET status = 'deprecated' WHERE product_id = '{P['id']}'")
    spark.sql(f"ALTER TABLE {OUT} SET TAGS ('status' = 'deprecated')")
    print(f"Deprecated. Dependants now report an unhealthy input: {dependants}")

elif ACTION == "retire":
    spark.sql(f"UPDATE {GOV}.data_product_registry SET status = 'retired' WHERE product_id = '{P['id']}'")
    spark.sql(f"ALTER TABLE {OUT} SET TAGS ('status' = 'retired')")
    spark.sql(f"REVOKE SELECT ON TABLE {OUT} FROM {CONSUMERS}")
    print("Retired: consumer access revoked. Re-run 01 and 03 to restore in dev.")

display(spark.sql(f"SELECT * FROM {GOV}.product_health ORDER BY layer"))
