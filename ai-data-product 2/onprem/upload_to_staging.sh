#!/usr/bin/env bash
# Simulates the on-prem jobs that drop extracts into ADLS staging.
# The file name decides the landing folder (and so which data product picks it up):
#   customer_accounts_*.csv -> landing/customer_accounts/  (bronze operational -> gold)
#   ref_branch_*.csv        -> landing/ref_branch/         (silver reference  -> gold)
# Usage: STORAGE_ACCOUNT=stdpdev01 ./upload_to_staging.sh ref_branch_20261003.csv
set -euo pipefail
: "${STORAGE_ACCOUNT:?set STORAGE_ACCOUNT}"
FILE="$1"
NAME="$(basename "$FILE")"

case "$NAME" in
  customer_accounts_*) FOLDER=customer_accounts ;;
  ref_branch_*)        FOLDER=ref_branch ;;
  *) FOLDER=unrouted; echo "WARNING: $NAME matches no data product; it will be ignored." ;;
esac

# Option A (dev laptop): reuse your `az login` session (needs Storage Blob Data Contributor).
export AZCOPY_AUTO_LOGIN_TYPE=AZCLI
# Option B (real on-prem server): service principal
#   export AZCOPY_AUTO_LOGIN_TYPE=SPN AZCOPY_SPA_APPLICATION_ID=... AZCOPY_SPA_CLIENT_SECRET=... AZCOPY_TENANT_ID=...

azcopy copy "$FILE" "https://${STORAGE_ACCOUNT}.blob.core.windows.net/staging/landing/${FOLDER}/${NAME}"
echo "Dropped $NAME into staging/landing/${FOLDER}/"
