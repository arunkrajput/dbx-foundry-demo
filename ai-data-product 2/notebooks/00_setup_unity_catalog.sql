-- Databricks notebook source
-- MAGIC %md
-- MAGIC # 00 · Unity Catalog setup (run once)
-- MAGIC Replace `<storage_account>` with your ADLS Gen2 account name (Edit → Find and replace).
-- MAGIC Prerequisite: storage credential **dp_cred** created in Catalog Explorer from the Access Connector (guide Step 2.1).
-- MAGIC Run as a metastore admin or a user with CREATE EXTERNAL LOCATION and CREATE CATALOG.

-- COMMAND ----------

-- External locations: where on-prem drops files, and where UC stores managed tables
CREATE EXTERNAL LOCATION IF NOT EXISTS dp_staging
  URL 'abfss://staging@stdpdev6633.dfs.core.windows.net/'
  WITH (STORAGE CREDENTIAL dp_cred)
  COMMENT 'Landing zone for on-prem extracts';

CREATE EXTERNAL LOCATION IF NOT EXISTS dp_managed
  URL 'abfss://uc-managed@stdpdev6633.dfs.core.windows.net/'
  WITH (STORAGE CREDENTIAL dp_cred)
  COMMENT 'Managed storage for dp_dev catalog';

-- COMMAND ----------

CREATE CATALOG IF NOT EXISTS dp_dev
  MANAGED LOCATION 'abfss://uc-managed@stdpdev6633.dfs.core.windows.net/dp_dev'
  COMMENT 'AI-ready data products (dev)';

CREATE SCHEMA IF NOT EXISTS dp_dev.staging    COMMENT 'Landing volumes and checkpoints';
CREATE SCHEMA IF NOT EXISTS dp_dev.bronze     COMMENT 'As-received data: raw audit tables + OPERATIONAL data products';
CREATE SCHEMA IF NOT EXISTS dp_dev.silver     COMMENT 'Conformed REFERENCE data products';
CREATE SCHEMA IF NOT EXISTS dp_dev.gold       COMMENT 'CONSUMER-ALIGNED data products (AI-ready)';
CREATE SCHEMA IF NOT EXISTS dp_dev.governance COMMENT 'Registry, quality runs, quarantine, policies';

-- COMMAND ----------

-- The on-prem drop lands here: /Volumes/dp_dev/staging/landing/customer_accounts/
CREATE EXTERNAL VOLUME IF NOT EXISTS dp_dev.staging.landing
  LOCATION 'abfss://staging@stdpdev6633.dfs.core.windows.net/landing'
  COMMENT 'On-prem file drops';

-- Auto Loader checkpoints and schema tracking
CREATE VOLUME IF NOT EXISTS dp_dev.staging.checkpoints;

-- COMMAND ----------

-- Sanity check: lists customer_accounts/ and ref_branch/ once you have uploaded files
LIST '/Volumes/dp_dev/staging/landing/';
