-- Databricks notebook source
-- MAGIC %md
-- MAGIC # 03 · GOVERN: the same policies on every layer that is a product
-- MAGIC Bronze operational products hold raw PII too, so they get the **same masks** as gold.
-- MAGIC The raw audit table (`bronze.customer_accounts_raw`) and quarantine are never granted to consumers.
-- MAGIC
-- MAGIC Prerequisite: **account-level** groups `dp_pipelines`, `dp_kyc_ops`, `dp_operations`, `dp_analysts`, `dp_ai_agents`.
-- MAGIC
-- MAGIC **Why `dp_pipelines`:** gold is built by reading the bronze operational product. If the identity running
-- MAGIC the pipeline were masked, masked IC numbers would be written into gold permanently. Pipeline identities
-- MAGIC (you in dev, the job's service principal in prod) are exempt from the IC mask.

-- COMMAND ----------

CREATE OR REPLACE FUNCTION dp_dev.governance.mask_ic(ic STRING)
RETURNS STRING
COMMENT 'NRIC visible to dp_kyc_ops and pipeline identities only'
RETURN CASE WHEN is_account_group_member('dp_kyc_ops') OR is_account_group_member('dp_pipelines') THEN ic
            ELSE concat('******-**-', right(ic, 4)) END;

CREATE OR REPLACE FUNCTION dp_dev.governance.mask_name(n STRING)
RETURNS STRING
COMMENT 'AI agents see initials only'
RETURN CASE WHEN is_account_group_member('dp_ai_agents')
            THEN concat_ws(' ', transform(split(n, ' '), w -> concat(left(w, 1), '.')))
            ELSE n END;

-- Typed balance (gold)
CREATE OR REPLACE FUNCTION dp_dev.governance.mask_balance(b DECIMAL(18,2))
RETURNS DECIMAL(18,2)
COMMENT 'Balances never exposed to AI agents'
RETURN CASE WHEN is_account_group_member('dp_ai_agents') THEN NULL ELSE b END;

-- As-received balance (bronze operational, STRING)
CREATE OR REPLACE FUNCTION dp_dev.governance.mask_balance_str(b STRING)
RETURNS STRING
COMMENT 'Balances never exposed to AI agents'
RETURN CASE WHEN is_account_group_member('dp_ai_agents') THEN NULL ELSE b END;

-- COMMAND ----------

-- GOLD consumer-aligned
ALTER TABLE dp_dev.gold.customer_accounts ALTER COLUMN ic_number     SET MASK dp_dev.governance.mask_ic;
ALTER TABLE dp_dev.gold.customer_accounts ALTER COLUMN customer_name SET MASK dp_dev.governance.mask_name;
ALTER TABLE dp_dev.gold.customer_accounts ALTER COLUMN balance_myr   SET MASK dp_dev.governance.mask_balance;

-- BRONZE operational
ALTER TABLE dp_dev.bronze.customer_accounts_ops ALTER COLUMN ic_number     SET MASK dp_dev.governance.mask_ic;
ALTER TABLE dp_dev.bronze.customer_accounts_ops ALTER COLUMN customer_name SET MASK dp_dev.governance.mask_name;
ALTER TABLE dp_dev.bronze.customer_accounts_ops ALTER COLUMN balance_myr   SET MASK dp_dev.governance.mask_balance_str;

-- SILVER reference: no PII, no masks

-- COMMAND ----------

GRANT USE CATALOG ON CATALOG dp_dev TO `dp_kyc_ops`, `dp_operations`, `dp_analysts`, `dp_ai_agents`;
GRANT USE SCHEMA ON SCHEMA dp_dev.bronze     TO `dp_kyc_ops`, `dp_operations`, `dp_ai_agents`;
GRANT USE SCHEMA ON SCHEMA dp_dev.silver     TO `dp_kyc_ops`, `dp_operations`, `dp_analysts`, `dp_ai_agents`;
GRANT USE SCHEMA ON SCHEMA dp_dev.gold       TO `dp_kyc_ops`, `dp_operations`, `dp_analysts`, `dp_ai_agents`;
GRANT USE SCHEMA ON SCHEMA dp_dev.governance TO `dp_kyc_ops`, `dp_operations`, `dp_analysts`, `dp_ai_agents`;

-- Operational product: operations teams, KYC and the agent (for data-issue questions). Not analysts.
GRANT SELECT ON TABLE dp_dev.bronze.customer_accounts_ops TO `dp_kyc_ops`, `dp_operations`, `dp_ai_agents`;
-- Reference product: everyone
GRANT SELECT ON TABLE dp_dev.silver.ref_branch            TO `dp_kyc_ops`, `dp_operations`, `dp_analysts`, `dp_ai_agents`;
-- Consumer product: everyone
GRANT SELECT ON TABLE dp_dev.gold.customer_accounts       TO `dp_kyc_ops`, `dp_operations`, `dp_analysts`, `dp_ai_agents`;
-- Discovery + health
GRANT SELECT ON TABLE dp_dev.governance.data_product_registry TO `dp_kyc_ops`, `dp_operations`, `dp_analysts`, `dp_ai_agents`;
GRANT SELECT ON TABLE dp_dev.governance.product_health        TO `dp_kyc_ops`, `dp_operations`, `dp_analysts`, `dp_ai_agents`;

-- COMMAND ----------

-- What YOU see depends on your own groups
SELECT 'gold' AS layer, account_id, customer_name, ic_number, balance_myr FROM dp_dev.gold.customer_accounts LIMIT 3;

-- COMMAND ----------

SELECT 'bronze_ops' AS layer, account_id, customer_name, ic_number, balance_myr, _dq_status
FROM dp_dev.bronze.customer_accounts_ops LIMIT 3;
