# Azure Databricks Job Migration Plan

> **Branch**: `azure-databricks-job`  
> **Goal**: Deploy ETL validation pipeline as an Azure Databricks Job with dedicated clusters, Azure Blob Storage for source files, Key Vault-backed secrets, and re-enabled caching.

---

## Table of Contents

1. [Azure Infrastructure Setup](#a-azure-infrastructure-setup)
2. [Code Changes (file by file)](#b-code-changes-file-by-file)
3. [Databricks Job Configuration](#c-databricks-job-configuration)
4. [Cost Estimation](#d-cost-estimation)
5. [Step-by-Step Deployment Runbook](#e-step-by-step-deployment-runbook)
6. [Testing Checklist](#f-testing-checklist)
7. [Questions Before Starting](#g-questions-before-starting)

---

## A. Azure Infrastructure Setup

### Step 1: Create Resource Group & Core Services

```bash
# Resource Group
az group create -n rg-etl-testing -l eastus

# Databricks Workspace (Standard tier — cheapest for Jobs)
az databricks workspace create \
  -n dbw-etl-testing \
  -g rg-etl-testing \
  --sku standard \
  -l eastus

# Storage Account (Blob — cheapest for file storage)
az storage account create \
  -n etlstorage0907 \
  -g rg-etl-testing \
  -l eastus \
  --sku Standard_LRS \
  --kind StorageV2

# Container for source files
az storage container create \
  -n etl-source-data \
  --account-name etlstorage0907

# Key Vault for secrets
az keyvault create \
  -n kv-etl-secrets \
  -g rg-etl-testing \
  -l eastus
```

**Estimated one-time cost**: $0 (all free tier eligible).

### Step 2: Populate Key Vault with Secrets

```bash
# Snowflake credentials
az keyvault secret set --vault-name kv-etl-secrets -n sf-account    --value "RPDEFQT-SJ73076"
az keyvault secret set --vault-name kv-etl-secrets -n sf-user       --value "ARSHPREETSINGH98"
az keyvault secret set --vault-name kv-etl-secrets -n sf-password   --value "<YOUR_SF_PASSWORD>"
az keyvault secret set --vault-name kv-etl-secrets -n sf-database   --value "ETL_OUTPUT_SNOWFLAKE_TARGET_JOINS"
az keyvault secret set --vault-name kv-etl-secrets -n sf-schema     --value "PUBLIC"
az keyvault secret set --vault-name kv-etl-secrets -n sf-warehouse  --value "ETL_WH"
az keyvault secret set --vault-name kv-etl-secrets -n sf-role       --value "ACCOUNTADMIN"

# Azure Blob Storage account key (used for wasbs:// access)
STORAGE_KEY=$(az storage account keys list --account-name etlstorage0907 -g rg-etl-testing --query "[0].value" -o tsv)
az keyvault secret set --vault-name kv-etl-secrets -n blob-storage-key --value "$STORAGE_KEY"
```

> **Note**: We use **storage account key** (not connection string) because Spark's `wasbs://` driver needs the key directly via `spark.hadoop.fs.azure.account.key.<account>.blob.core.windows.net`.

### Step 3: Link Key Vault to Databricks Secret Scope

1. Get Key Vault properties:
   ```bash
   az keyvault show --name kv-etl-secrets --query "{dns:properties.vaultUri, id:id}" -o table
   ```
2. Navigate to: `https://<databricks-instance>#secrets/createScope`
3. Create scope:
   - **Scope name**: `etl-secrets`
   - **Manage Principal**: `All Users`
   - **DNS Name**: from step above (e.g., `https://kv-etl-secrets.vault.azure.net/`)
   - **Resource ID**: from step above

### Step 4: Upload Source Files to Blob Storage

```bash
# Per table/sub-path — example for warranty_claims/xl
az storage blob upload-batch \
  -d etl-source-data/warranty_claims/xl \
  -s output/warranty_claims/xl \
  --account-name etlstorage0907 \
  --pattern "source_raw*"

az storage blob upload \
  -c etl-source-data \
  -f output/warranty_claims/xl/source_db_schema.json \
  -n warranty_claims/xl/source_db_schema.json \
  --account-name etlstorage0907

# Repeat for each table (cost_ledger, employee_master, etc.)
```

**Blob path pattern**: `wasbs://etl-source-data@etlstorage0907.blob.core.windows.net/{TABLE_NAME}/{SUB_PATH}/`

---

## B. Code Changes (file by file)

### 1. DELETE `custom_execution.py`
Local-only entry point. No longer needed.

### 2. REWRITE `run_on_databricks.py` → `main.py`
Convert from notebook cells to a clean Python Job script:
- Remove `# COMMAND ----------`, `# MAGIC`, `display()`, `dbutils.widgets.text()` for passwords
- Accept job parameters via `dbutils.widgets.text()` (Databricks Jobs can pass widget values as parameters)
- Construct `wasbs://` paths from `STORAGE_ACCOUNT` + `CONTAINER` parameters
- Set Spark conf for blob storage access using secret
- Re-enable cache cleanup at the end
- Add `sys.exit(exit_code)` at bottom

**Parameters accepted**:
| Parameter | Default | Description |
|-----------|---------|-------------|
| `TABLE_NAME` | `warranty_claims` | Table to validate |
| `SUB_PATH` | `xl` | Sub-path for data files |
| `STORAGE_ACCOUNT` | `etlstorage0907` | Azure Blob storage account name |
| `CONTAINER` | `etl-source-data` | Blob container name |
| `VERIFY_SCHEMA` | `true` | Enable schema verification |
| `PK_FILTER_MODE` | `full` | `full` / `pk_range` / `pk_set` |
| `DATE_WATERMARK_MODE` | `full` | `full` / `range` |

### 3. SIMPLIFY `utils/connections/spark_session.py`
- Remove `SPARK_MODE` flag, `_get_local_session()`, `_set_env_from_config()`, `_set_python_executable()`, `_discover_jars()`
- `get_spark_session()` just returns `SparkSession.getActiveSession()` or `getOrCreate()`

### 4. SIMPLIFY `utils/connections/target_connection.py`
- Remove `_SF_DIRECT_CONFIG`, `set_snowflake_config()`, and all direct-config branches
- Keep ONLY Databricks Secrets path (`dbutils.secrets.get("etl-secrets", ...)`)

### 5. MODIFY `utils/get_data.py`
- **Remove**: `_is_databricks()`, `_read_snowflake_python_connector()` (local fallback)
- **`get_data_from_snowflake()`**: call Spark connector directly, no if/else
- **Re-enable caching**: uncomment `df.cache()` + add `df.foreach(lambda _: None)` for forced materialization
- **`get_data_from_storage()`**: 
  - Uncomment `df.cache()`
  - Replace `open(schema_json_path)` with `dbutils.fs.head()` or `spark.read.text()` — Python `open()` won't work with `wasbs://` paths
  - Or: use `dbutils.fs.head(path, 1048576)` to read JSON string, then `json.loads()`

### 6. MODIFY `utils/custom_execution_utils.py`
- `step_2_transform()`: uncomment `transformed_df.cache()` and `source_df.unpersist()` block
- `resolve_query()`: file paths for query/DDL files are in the repo — they'll be on the Workspace filesystem when cloned via Repos, so `open()` works. **No change needed.**

### 7. MODIFY `utils/comparator.py`
Re-enable all `[SERVERLESS]`-tagged caching lines:
- `source_norm.cache()`, `target_norm.cache()`
- `source_df.unpersist()`, `target_df.unpersist()` after normalization
- `joined.cache()` / `joined.unpersist()` in all 3 strategies
- `diff_df.coalesce(N).cache()` in all strategies
- `source_norm.unpersist()`, `target_norm.unpersist()` in finally block

### 8. MODIFY `utils/csv_writer.py`
- Re-enable `is_cached` branch
- **Critical**: `glob.glob()` + `shutil.move()` won't work with `wasbs://` paths
- **Solution**: Write CSV to local `/tmp/etl_output/` first, then copy to blob via `dbutils.fs.cp("file:/tmp/...", "wasbs://...")`
- OR: Use `df.write.csv()` directly to `wasbs://` path (Spark handles it natively), then rename via `dbutils.fs.mv()`

### 9. MODIFY `utils/verify_schema.py`
- `verify_schema_from_json_file()` uses `open()` to read the schema JSON — if the file is on blob storage, needs `dbutils.fs.head()`. Check if this function reads from blob or repo.
- The `source_db_schema.json` is on blob storage → **must change to use dbutils or spark**

### 10. Delete `.env` file / `dotenv` usage
No longer needed — all secrets come from Key Vault.

### Summary of File Changes

| File | Action |
|------|--------|
| `custom_execution.py` | **DELETE** |
| `run_on_databricks.py` | **REWRITE** → `main.py` |
| `utils/connections/spark_session.py` | **SIMPLIFY** (remove local mode) |
| `utils/connections/target_connection.py` | **SIMPLIFY** (secrets only) |
| `utils/get_data.py` | **MODIFY** (remove local, re-enable cache, blob-compatible reads) |
| `utils/custom_execution_utils.py` | **MODIFY** (re-enable cache) |
| `utils/comparator.py` | **MODIFY** (re-enable all caching) |
| `utils/csv_writer.py` | **MODIFY** (re-enable cache, blob-compatible writes) |
| `utils/verify_schema.py` | **MODIFY** (blob-compatible JSON reads) |
| `SERVERLESS_CACHING_GUIDE.md` | **DELETE** (no longer relevant) |

---

## C. Databricks Job Configuration

### Cluster Configuration

**Test (≤1.25 GB)**:
```json
{
  "num_workers": 0,
  "spark_version": "15.4.x-scala2.12",
  "node_type_id": "Standard_DS3_v2",
  "spark_conf": {
    "spark.databricks.cluster.profile": "singleNode",
    "spark.master": "local[*]",
    "spark.sql.legacy.timeParserPolicy": "LEGACY",
    "spark.sql.debug.maxToStringFields": "500"
  }
}
```

**Production (20M×80 cols)**:
```json
{
  "num_workers": 3,
  "spark_version": "15.4.x-scala2.12",
  "node_type_id": "Standard_E4s_v3",
  "driver_node_type_id": "Standard_E4s_v3",
  "spark_conf": {
    "spark.sql.legacy.timeParserPolicy": "LEGACY",
    "spark.sql.debug.maxToStringFields": "500",
    "spark.sql.shuffle.partitions": "64"
  }
}
```

### Maven Libraries (attach to cluster / job)
```
net.snowflake:spark-snowflake_2.12:2.16.0-spark_3.5
net.snowflake:snowflake-jdbc:3.18.0
```

### Job Definition (JSON)
```json
{
  "name": "ETL_Validation_Pipeline",
  "tasks": [{
    "task_key": "validate",
    "spark_python_task": {
      "python_file": "main.py",
      "parameters": ["{{job.parameters.TABLE_NAME}}", "{{job.parameters.SUB_PATH}}"]
    },
    "new_cluster": { "...see above..." },
    "libraries": [
      {"maven": {"coordinates": "net.snowflake:spark-snowflake_2.12:2.16.0-spark_3.5"}},
      {"maven": {"coordinates": "net.snowflake:snowflake-jdbc:3.18.0"}}
    ]
  }],
  "parameters": [
    {"name": "TABLE_NAME", "default": "warranty_claims"},
    {"name": "SUB_PATH", "default": "xl"}
  ]
}
```

---

## D. Cost Estimation

### Pricing Components
- **Azure Databricks Standard (Jobs compute)**: ~$0.20/DBU
- **VM costs**: pay-per-minute, auto-terminates after job

### Cluster Cost Comparison

| Config | VM Type | Cores/RAM | DBU/hr | VM $/hr | Databricks $/hr | **Total $/hr** |
|--------|---------|-----------|--------|---------|-----------------|-----------------|
| **Single Node** | DS3_v2 | 4c / 14GB | 0.75 | $0.19 | $0.15 | **$0.34** |
| **1 driver + 1 worker** | DS3_v2 | 8c / 28GB | 1.50 | $0.38 | $0.30 | **$0.68** |
| **1 driver + 2 workers** | DS4_v2 | 24c / 84GB | 4.50 | $1.15 | $0.90 | **$2.05** |
| **1 driver + 3 workers** | E4s_v3 | 16c / 128GB | 4.00 | $1.01 | $0.80 | **$1.81** |
| **1 driver + 4 workers** | DS5_v2 | 80c / 280GB | 10.0 | $2.56 | $2.00 | **$4.56** |

### Runtime Estimates

| Config | 1.25 GB test | 20M×80 cols (~15-25GB) |
|--------|-------------|------------------------|
| **Single Node DS3_v2** | 5-8 min | ❌ OOM (14GB RAM) |
| **1+1 DS3_v2** | 4-6 min | ❌ OOM (28GB RAM) |
| **1+2 DS4_v2** | 3-5 min | 25-40 min (tight) |
| **1+3 E4s_v3** ⭐ | 3-5 min | 8-15 min |
| **1+4 DS5_v2** | 2-3 min | 5-10 min |

### How Far Does $200 Go?

| Config | $/run (1.25GB) | $/run (production) | Test runs | Prod runs |
|--------|---------------|-------------------|-----------|-----------|
| **Single Node DS3_v2** | ~$0.05 | N/A | **~4,000** | N/A |
| **1+3 E4s_v3** | ~$0.15 | ~$0.45 | ~1,333 | **~444** |
| **1+4 DS5_v2** | ~$0.23 | ~$0.76 | ~870 | ~263 |

### Recommendation
- **Testing phase**: Single Node DS3_v2 (~$0.05/run) — you get ~4,000 runs
- **Production**: 1 driver + 3 workers E4s_v3 (~$0.45/run) — ~444 runs
- **Total budget estimate**: 50 test runs ($2.50) + 10 production runs ($4.50) = **~$7 for initial validation**

### Other Costs (Negligible)
| Service | Monthly Cost |
|---------|-------------|
| Blob Storage (25GB) | ~$0.50 |
| Key Vault (secrets) | ~$0.03 |
| Databricks Workspace | $0 (pay per job) |
| **Total idle cost** | **~$0.53/month** |

---

## E. Step-by-Step Deployment Runbook

### Phase 1: Azure Setup (~30 min)
1. Install Azure CLI: `winget install Microsoft.AzureCLI`
2. Login: `az login`
3. Run all commands from [Section A](#a-azure-infrastructure-setup) Steps 1-4
4. Note the Databricks workspace URL from: `az databricks workspace show -n dbw-etl-testing -g rg-etl-testing --query workspaceUrl -o tsv`

### Phase 2: Databricks Setup (~15 min)
1. Open Databricks workspace URL in browser
2. Create secret scope (Section A Step 3)
3. Verify secrets: In a notebook, run `dbutils.secrets.list("etl-secrets")` — should show all keys

### Phase 3: Code Changes (~2-3 hours)
1. Switch to branch: `git checkout azure-databricks-job`
2. Make all changes from [Section B](#b-code-changes-file-by-file)
3. Commit and push

### Phase 4: Connect Repo to Databricks (~5 min)
1. In Databricks: **Repos → Add Repo**
2. Enter your Git URL + select branch `azure-databricks-job`
3. Verify files appear under `/Workspace/Repos/<your-email>/ETL_Testing_Databricks/`

### Phase 5: First Test Run (~10 min)
1. **Jobs → Create Job**
2. Task type: **Python script**
3. Source: **Workspace** → select `main.py` from Repos
4. Cluster: **Single Node DS3_v2** (see Section C)
5. Libraries: Add both Maven coordinates
6. Parameters: `TABLE_NAME=warranty_claims`, `SUB_PATH=xl`
7. **Run Now**
8. Check logs for completion

### Phase 6: Verify Output (~5 min)
1. Check job logs for "Pipeline complete. Exit code: 0"
2. Verify diff_report.csv in blob storage:
   ```bash
   az storage blob list -c etl-source-data --account-name etlstorage0907 --prefix "output/" -o table
   ```
3. Download and inspect: `az storage blob download -c etl-source-data -n output/warranty_claims/xl/diff_report.csv -f diff_report.csv --account-name etlstorage0907`

---

## F. Testing Checklist

| # | Test | How to Verify | Expected |
|---|------|---------------|----------|
| 1 | Secret scope accessible | `dbutils.secrets.list("etl-secrets")` in a notebook | Lists all 8 secrets |
| 2 | Blob storage readable | `dbutils.fs.ls("wasbs://etl-source-data@etlstorage0907.blob.core.windows.net/warranty_claims/xl/")` | Shows source_raw.csv, etc. |
| 3 | Schema JSON parseable | Job log: "Schema loaded from..." | No errors |
| 4 | Source CSV loads | Job log: "Loaded from storage: X rows, Y columns" | Row count matches expected |
| 5 | Snowflake connected | Job log: "Snowflake extract: Xs \| Row count: N" | Row count > 0 |
| 6 | Transform runs | Job log: "Transformed DataFrame: N rows" | Count matches source |
| 7 | Comparison runs | Job log: "Pipeline complete. Exit code: 0" | Exit 0 = pass |
| 8 | Diff report written | Blob storage has `output/{TABLE}/diff_report.csv` | File exists |
| 9 | Caching active | Spark UI → Storage tab during run | Shows cached DFs |
| 10 | Cache cleanup | Job log: "Cache cleanup complete." | No memory leaks |

---

## G. Decisions (Confirmed)

| # | Question | Answer |
|---|----------|--------|
| 1 | Storage account name | `etlstorage0907` |
| 2 | Output location | Same blob container, under `output/{TABLE_NAME}/{SUB_PATH}/` |
| 3 | Git provider | **GitHub** now, migrate to Azure DevOps later (see Section H) |
| 4 | Snowflake credentials | Correct — password goes into Key Vault secret `sf-password` |
| 5 | Multi-table runs | One table per job run (parameterized) |
| 6 | Notifications | None — only diff_report.csv output |

---

## H. GitHub → Azure DevOps Transition Plan

### Current Setup (GitHub)
- Databricks Repos → Add Repo → GitHub URL + branch `azure-databricks-job`
- Job source: Workspace file from `/Workspace/Repos/<email>/ETL_Testing_Databricks/main.py`

### Migration to Azure DevOps (When Ready)

1. **Create Azure DevOps project**: `az devops project create --name ETL_Testing_Databricks --org https://dev.azure.com/<your-org>`
2. **Push repo to Azure DevOps**:
   ```bash
   git remote add azdo https://dev.azure.com/<org>/<project>/_git/ETL_Testing_Databricks
   git push azdo azure-databricks-job
   ```
3. **Update Databricks Repos**: 
   - Delete existing GitHub-linked repo in Databricks
   - Add Repo → Azure DevOps → select new repo + branch
   - Generate Azure DevOps PAT (Personal Access Token) with `Code (Read)` scope
4. **Update Job**: Edit job → change source path to new Repos location (path stays the same if username/repo name match)
5. **Optional — CI/CD**: Create Azure Pipeline (`azure-pipelines.yml`) to auto-deploy on push:
   ```yaml
   trigger:
     branches: [azure-databricks-job]
   pool:
     vmImage: ubuntu-latest
   steps:
     - script: |
         pip install databricks-cli
         databricks jobs run-now --job-id <JOB_ID>
       env:
         DATABRICKS_HOST: $(DATABRICKS_HOST)
         DATABRICKS_TOKEN: $(DATABRICKS_TOKEN)
   ```

**Impact**: Zero code changes needed. Only Databricks Repos provider changes.

