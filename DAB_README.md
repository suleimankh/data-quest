# Population Orchestration Pipeline

This repository contains the Declarative Automation Bundles (DAB) configuration for the population orchestration pipeline.

## Architecture

The pipeline consists of three Spark Declarative Pipelines orchestrated by a Databricks job:

1. **BLS Pipeline** - Ingests and processes Bureau of Labor Statistics data → `rearc.bls`
2. **DataUSA Pipeline** - Ingests and processes DataUSA data → `rearc.datausa`
3. **Analytics Pipeline** - Combines data from BLS and DataUSA → `rearc_analytics.population_analytics`

## Project Structure

```
data-quest/
├── databricks.yml                      # Main DAB configuration
├── resources/
│   ├── pipelines/
│   │   ├── bls_pipeline.yml           # BLS pipeline definition
│   │   ├── datausa_pipeline.yml       # DataUSA pipeline definition
│   │   └── analytics_pipeline.yml     # Analytics pipeline definition
│   └── jobs/
│       └── orchestration_job.yml      # Orchestration job definition
├── bls_pipeline/
│   └── transformations/               # BLS transformation notebooks
├── datausa_pipeline/
│   └── transformations/               # DataUSA transformation notebooks
├── analytics_pipeline/
│   └── transformations/               # Analytics transformation notebooks
├── .github/
│   └── workflows/
│       └── deploy.yml                 # CI/CD workflow
├── .gitignore
└── README.md
```

## Environments

### Development
- **Branch**: `develop`
- **Databricks Repo**: `/Workspace/Repos/development/data-quest`
- **Catalog Prefix**: `dev_`
- **Resources**: 
  - `bls_pipeline_dev` → `dev_rearc.bls`
  - `datausa_pipeline_dev` → `dev_rearc.datausa`
  - `analytics_pipeline_dev` → `dev_rearc_analytics.population_analytics`
  - `population_orchestration_pipeline_dev`

### Production
- **Branch**: `main`
- **Databricks Repo**: `/Workspace/Repos/Production/data-quest`
- **Catalog Prefix**: (none)
- **Resources**:
  - `bls_pipeline_prod` → `rearc.bls`
  - `datausa_pipeline_prod` → `rearc.datausa`
  - `analytics_pipeline_prod` → `rearc_analytics.population_analytics`
  - `population_orchestration_pipeline_prod`

## Setup Instructions

### Prerequisites
- Databricks CLI installed: `curl -fsSL https://databricks.com/install.sh | sh`
- Databricks authentication configured: `databricks auth login --host https://dbc-c2ce80d3-dded.cloud.databricks.com`

### Initial Setup in Databricks

1. **Clone repo to Development** (if not done yet):
   - In Databricks UI: **Repos** → **Add Repo**
   - URL: `https://github.com/suleimankh/data-quest`
   - Path: `/Workspace/Repos/development/data-quest`
   - Branch: `develop`

2. **Production repo** (already cloned):
   - Path: `/Workspace/Repos/Production/data-quest`
   - Branch: `main`

### Deployment

#### Development Deployment
```bash
# Validate bundle
databricks bundle validate -t dev

# Deploy to development
databricks bundle deploy -t dev

# Run the orchestration job
databricks bundle run population_orchestration -t dev
```

#### Production Deployment
```bash
# Validate bundle
databricks bundle validate -t prod

# Deploy to production
databricks bundle deploy -t prod

# Run the orchestration job
databricks bundle run population_orchestration -t prod
```

## Workflow Orchestration

The orchestration job runs tasks in this order:

```
  bls_pipeline  +  datausa_pipeline  (parallel)
              ↓
      analytics_pipeline
```

* bls_pipeline and datausa_pipeline run in parallel
* analytics_pipeline waits for both to complete
* Dependencies defined in `orchestration_job.yml`

## CI/CD

The repository includes GitHub Actions workflows that automatically:
- Validate bundles on all PRs
- Deploy to development on push to `develop` branch
- Deploy to production on push to `main` branch

### ⚠️ Required GitHub Secrets Setup

**BEFORE the CI/CD workflow can run**, you MUST configure these GitHub Repository Secrets:

#### Step 1: Generate Databricks Personal Access Token

1. Go to Databricks workspace: https://dbc-c2ce80d3-dded.cloud.databricks.com
2. Click your user profile (top-right) → **Settings**
3. Navigate to: **Developer** → **Access tokens**
4. Click **"Manage"** → **"Generate new token"**
5. Configure:
   - Comment: `GitHub Actions CI/CD for data-quest`
   - Lifetime: `90 days` (or as needed)
6. Click **"Generate"**
7. **⚠️ COPY THE TOKEN IMMEDIATELY** - you won't see it again!

#### Step 2: Add Secrets to GitHub

1. Go to your repository: https://github.com/suleimankh/data-quest
2. Click **Settings** (top tab)
3. Left sidebar: **Secrets and variables** → **Actions**
4. Click **"New repository secret"**
5. Add **FIRST** secret:
   - Name: `DATABRICKS_HOST`
   - Value: `https://dbc-c2ce80d3-dded.cloud.databricks.com`
6. Add **SECOND** secret:
   - Name: `DATABRICKS_TOKEN`
   - Value: `<your token from Step 1>`

#### Step 3: Verify

After adding secrets, push a commit or re-run the failed GitHub Action.

**For detailed instructions with screenshots, see:** `GITHUB_SECRETS_SETUP.md`

## Commands Reference

### Bundle Management
```bash
# Validate configuration
databricks bundle validate -t {dev|prod}

# Deploy resources
databricks bundle deploy -t {dev|prod}

# Run job
databricks bundle run population_orchestration -t {dev|prod}

# Destroy resources (careful!)
databricks bundle destroy -t {dev|prod}
```

### Pipeline Management
```bash
# List pipeline updates
databricks pipelines list-updates <pipeline-id>

# Get pipeline details
databricks pipelines get <pipeline-id>
```

## Troubleshooting

### GitHub Actions: Authentication Error

**Error:**
```
Error: failed during request visitor: default auth: cannot configure default credentials
env:
  DATABRICKS_HOST: 
  DATABRICKS_TOKEN: 
```

**Solution:**
GitHub Secrets are not configured. Follow the **"Required GitHub Secrets Setup"** section above or see `GITHUB_SECRETS_SETUP.md` for detailed instructions.

### Pipeline Libraries: "Failed to load notebook"

**Error:**
```
Failed to load notebook '/Workspace/Repos/.../transformations/bronze.py'. 
Only SQL and Python notebooks are supported currently.
```

**Cause:**
For Python files in Git repos (not Databricks notebooks), use `glob` patterns instead of `notebook` or `file` types.

**Solution:**
Use glob patterns to include all transformation files:

```yaml
resources:
  pipelines:
    my_pipeline:
      libraries:
        - glob:
            include: ${var.git_source_path}/my_pipeline/transformations/**
```

**Why glob?**
- ✅ Works with plain Python files (.py) in Git repos
- ✅ Automatically includes all files in the directory
- ✅ No need to list individual files
- ✅ Matches the working pattern from manual pipeline creation

**Incorrect:**
```yaml
libraries:
  - notebook:
      path: ${var.git_source_path}/my_pipeline/transformations/bronze.py  # ❌ Fails for .py files
```

**Correct:**
```yaml
libraries:
  - glob:
      include: ${var.git_source_path}/my_pipeline/transformations/**  # ✅ Works!
```

### Pipeline Target: "Cannot contain periods"

**Error:**
```
Error: CreatePipeline target_schema_name "catalog.schema" is not a valid name. 
Valid names cannot contain spaces, periods, forward slashes, or control characters.
```

**Cause:**
The `target` field expects ONLY the schema name, not `catalog.schema`.

**Solution:**
Separate catalog and schema into two distinct fields:

```yaml
resources:
  pipelines:
    my_pipeline:
      catalog: my_catalog          # ← Catalog name
      target: my_schema            # ← Schema name ONLY (no periods!)
      serverless: true
```

**Incorrect:**
```yaml
target: rearc.bls                  # ❌ Contains period
```

**Correct:**
```yaml
catalog: rearc
target: bls                        # ✅ Schema name only
```

### Serverless Pipeline: "Must specify a catalog"

**Error:**
```
Error: cannot create resources.pipelines.<pipeline_name>: You must specify a 
catalog when using serverless compute. (400 INVALID_PARAMETER_VALUE)
```

**Solution:**
Serverless Spark Declarative Pipelines require a `catalog` field. Add it to each pipeline YAML:

```yaml
resources:
  pipelines:
    my_pipeline:
      name: my_pipeline_${bundle.target}
      target: my_catalog.my_schema.my_table
      catalog: my_catalog                    # ← ADD THIS
      
      photon: true
      serverless: true
```

The `catalog` field should match the catalog in your `target` path.

### Bundle deployment fails
```bash
# Check bundle validation
databricks bundle validate -t dev

# Check Databricks authentication
databricks auth login --host https://dbc-c2ce80d3-dded.cloud.databricks.com
```

### Pipeline not found
- Ensure the Databricks Repo is cloned to the correct location
- Verify the `git_source_path` variable in `databricks.yml`
- Check that transformation files exist in pipeline folders

### Permission errors
- Ensure your user has workspace admin or appropriate permissions
- Check Unity Catalog permissions for target schemas

## Contributing

1. Create a feature branch from `develop`
2. Make your changes
3. Test in development environment
4. Create a PR to `develop`
5. After approval and testing, merge `develop` to `main`

## License

[Add your license here]
