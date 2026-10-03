# AI-ready data products: Azure Databricks + Microsoft Foundry agent

Layer convention: **bronze = operational**, **silver = reference**, **gold = consumer-aligned**.
Follow the step-by-step guide (Claude Doc).

| Path | Layer / stage |
|---|---|
| contracts/customer_accounts_ops.yml | BRONZE operational product contract |
| contracts/ref_branch.yml | SILVER reference product contract |
| contracts/customer_accounts.yml | GOLD consumer-aligned product contract (inputs: the two above) |
| onprem/generate_sample.py, generate_ref_branch.py, upload_to_staging.sh | SOURCE: simulated on-prem drops |
| notebooks/00_setup_unity_catalog.sql | one-time platform setup |
| notebooks/dplib/ | shared contract-driven lifecycle code |
| notebooks/01_define_and_register.py | PROVISION + REGISTER all products, health view |
| notebooks/02a_silver_reference_ref_branch.py | SILVER: all-or-nothing reference load |
| notebooks/02b_bronze_operational_customer_accounts_ops.py | BRONZE: audit, flag, publish as received |
| notebooks/02c_gold_consumer_customer_accounts.py | GOLD: passed rows + referential integrity + enrichment |
| notebooks/03_govern_policies.sql | GOVERN: masks on bronze and gold, grants per layer |
| notebooks/04_evolve_and_retire.py | EVOLVE / DEPRECATE / RETIRE any product |
| agent/dataproduct/core.py | DataProduct interface: discover, describe, health (incl. inputs), query, aggregate |
| agent/foundry_agent.py | CONSUME: Foundry agent with tools for every product |
| databricks.yml | path to prod: two file-arrival jobs |
