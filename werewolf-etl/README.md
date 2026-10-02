# Werewolf ETL bundle

One Databricks pipeline ingests completed Werewolf episode files, produces silver records, and calculates gold model/team performance. The kickoff job starts a normal pipeline update. An AI/BI dashboard compares models, model pairs, and complete team lineups using those gold tables.

## Layout

```text
werewolf-etl/
├── databricks.yml
├── resources/
│   ├── werewolf.pipeline.yml
│   ├── werewolf_kickoff.job.yml
│   └── werewolf_performance.dashboard.yml
├── src/
│   ├── werewolf_pipeline.py
│   └── werewolf_dashboard.lvdash.json
├── tests/
│   ├── test_event_compaction.py
│   └── fixtures/74788868.json
├── docs/
│   └── 74792170-structure-assessment.md
└── README.md
```

The combined source includes the gold definitions. A separate gold pipeline is not registered. Tests and assessment files stay in Git but are excluded from bundle workspace synchronization.

## Resources and output tables

| Bundle resource key | Purpose |
|---|---|
| `werewolf` | Serverless, triggered Lakeflow pipeline using `src/werewolf_pipeline.py` |
| `WW_ETL_Job` | File-arrival job referencing `${resources.pipelines.werewolf.id}` |
| `werewolf_performance` | AI/BI dashboard using the existing SQL warehouse and gold tables |

Within the configured catalog:

- `bronze.werewolf_json_normalized`: streaming ingestion of normalized episode files.
- `silver.episodes`, `silver.players`, `silver.actions`, `silver.events`: materialized views.
- `gold.model_role_performance`, `gold.model_overall_performance`, `gold.model_team_performance`, `gold.model_pair_performance`, `gold.team_lineup_performance`, `gold.team_episode_results`: materialized views.

The code names the `silver` and `gold` schemas explicitly. Use different catalogs for dev and prod to isolate their tables. Changing only the default pipeline schema does not isolate those outputs. The catalog and source volume must exist, and the execution identity needs permission to read the source and create the output datasets.

## Configuration

The existing workspace, dev/prod modes, and production run identity are preserved in `databricks.yml`. Supply these bundle variables for each target:

| Variable | Required | Purpose |
|---|---|---|
| `catalog` | Yes | Destination catalog; default pipeline schema is `bronze` |
| `source_path` | Yes | Directory such as `/Volumes/main/werewolf/raw/episodes` |
| `warehouse_id` | Yes | Existing SQL warehouse used to query the dashboard datasets |
| `gold_min_games` | No; default `20` | Minimum games for gold ranking eligibility |

The resource definition maps these to `werewolf.source_path`, `werewolf.silver_schema`, and `werewolf.gold_min_games`. Input JSON files remain in the volume; the bundle deploys code and configuration.

For example, from the repository root in PowerShell:

```powershell
Set-Location .\werewolf-etl
$env:BUNDLE_VAR_catalog = "YOUR_DEV_CATALOG"
$env:BUNDLE_VAR_source_path = "/Volumes/YOUR_CATALOG/YOUR_SCHEMA/YOUR_VOLUME/episodes"
$env:BUNDLE_VAR_warehouse_id = "YOUR_SQL_WAREHOUSE_ID"
databricks auth login --host https://dbc-b14fd7ae-c2b5.cloud.databricks.com --profile werewolf
databricks bundle validate -t dev -p werewolf
```

Replace the example values before validation. The resource uses serverless compute; confirm this matches the existing pipeline when adopting it.

## Adopt existing resources

The checked-in kickoff definition uses resource key `WW_ETL_Job` and a file-arrival trigger on `/Volumes/workspace/landing/landing/`. Reconcile that trigger with the target's source volume before deploying the job. Keep the pipeline ID as the bundle substitution rather than a literal ID. Existing deployment state may still use the earlier key `werewolf_kickoff`; reconcile that binding before a full deployment to avoid replacing the job unintentionally.

Bind each resource before the first deployment if it already exists:

```powershell
databricks bundle deployment bind werewolf YOUR_PIPELINE_ID -t dev -p werewolf
databricks bundle deployment bind WW_ETL_Job YOUR_JOB_ID -t dev -p werewolf
```

Match the existing catalog/schema, compute settings, and execution identity before binding/deploying. Resource bindings belong to a target; bind production separately if it already exists. Without binding, the bundle creates new resources.

## Deploy and run

From the bundle directory, with variables set:

```powershell
databricks bundle validate -t dev -p werewolf
databricks bundle deploy -t dev -p werewolf
databricks bundle run WW_ETL_Job -t dev -p werewolf
```

For production, set production variable values and use `-t prod`. Development mode pauses automatic job triggers by default.

In a Databricks workspace bundle editor, the Deployments panel can also deploy and run these resources after the required variables are configured.

## Performance dashboard

The editable dashboard definition is [src/werewolf_dashboard.lvdash.json](src/werewolf_dashboard.lvdash.json). Its resource settings are in [resources/werewolf_performance.dashboard.yml](resources/werewolf_performance.dashboard.yml).

The bundle uses the supplied dashboard export without changing its charts or queries. It contains four pages:

- Overall Model Performance
- Seat Performance 1
- Seat Performance 2
- Model Pair Performance

Its ten datasets include six gold tables, silver players/episodes/actions, and a SQL dataset joining players to actions. Table-backed datasets explicitly reference the `workspace` catalog. The SQL dataset uses `silver.players` and `silver.actions` with the bundle's default catalog. Changing `${var.catalog}` does not retarget explicit `workspace.*` references; update those references before deploying against another catalog.

The bundle resource key remains `werewolf_performance`, preserving the existing dashboard ID and URL when the source file changes. Publishing uses viewer credentials (`embed_credentials: false`); viewers need dashboard access, warehouse access, and permission to read the referenced silver and gold tables.

### Deploy just the development dashboard

For dashboard development against the existing `workspace.gold` tables, run from `werewolf-etl`:

```powershell
$env:BUNDLE_VAR_catalog = "workspace"
$env:BUNDLE_VAR_source_path = "/Volumes/workspace/landing/landing/"
$env:BUNDLE_VAR_warehouse_id = "3f5e230aa22fb0a1"
databricks bundle validate -t dev -p werewolf
databricks bundle plan -t dev -p werewolf --select dashboards.werewolf_performance
databricks bundle deploy -t dev -p werewolf --select dashboards.werewolf_performance
databricks bundle summary -t dev -p werewolf
```

The warehouse ID above is the existing Serverless Starter Warehouse in this workspace. Supply another ID for another workspace. `source_path` is needed to resolve the bundle configuration even when only the dashboard is selected. Keep `--select` for this workflow: it deploys the dashboard without deploying the development pipeline/job against the existing production tables. A full development pipeline deployment needs a separate destination catalog. Use a reviewed full bundle deployment when promoting to production; resolve the job binding noted above first.

The bundle creates and tracks the new dashboard, so no existing dashboard ID needs binding. After editing the deployed dashboard in the UI, use `databricks bundle generate dashboard --resource werewolf_performance -t dev -p werewolf` to update the local definition, review the diff, and commit the changes. Do not force deployment over unexported UI edits.

### Replacement dashboard deployment verification

On October 2, 2026, the bundle was switched to `src/werewolf_dashboard.lvdash.json` and the existing development dashboard was republished. The supplied JSON was preserved. Bundle validation and dataset-reference checks passed. The deployed definition matches the supplied four page IDs and ten dataset IDs, and the published revision was verified through the API. A subsequent deployment plan reported no changes. The pipeline and kickoff job were not selected. Browser rendering was not inspected.

### Original dashboard deployment verification

The original `werewolf_performance.lvdash.json` was deployed and published on October 1, 2026, from branch `dash_dev`. These checks describe that original version:

- [Open the development dashboard](https://dbc-b14fd7ae-c2b5.cloud.databricks.com/dashboardsv3/01f1be14a05c16f7ba4089e5e7a51543/published).
- Bundle validation passed with one existing warning about an unmatched `__pycache__` exclusion.
- All four dataset queries executed successfully against `workspace.gold`. Dataset output columns matched all chart, table, filter, and sort references across four pages and 30 widgets.
- Deployment created one dashboard; the pipeline and kickoff job were not selected. The published dashboard uses viewer credentials and warehouse `3f5e230aa22fb0a1`.
- The deployed definition contains all four datasets with catalog `workspace` and schema `gold`. A subsequent dashboard deployment plan reported no changes.

Validation used the SQL and dashboard APIs; browser rendering was not inspected. At verification time, there were 32 model/role rows (32 eligible), 8 overall rows (8 eligible), 72 pair rows (68 eligible), and 1,208 lineup rows (32 eligible).

## Tests

From `werewolf-etl`, using Python 3.9 or newer:

```powershell
python -m unittest discover -s tests -p test_event_compaction.py
```

These standard-library tests load the actual parser helpers without importing Spark. They verify event compaction and preservation of all 53 action/event matches in the sample. The pipeline itself runs in Databricks and requires `pyspark.pipelines`.

The layout migration verified source/fixture integrity and local file references. Python tests and `databricks bundle validate` could not be executed in the editing environment: its Python launcher is unusable and the Databricks CLI is unavailable. No deployment was performed.

## Pipeline behavior and history

See the [assessment and change record](docs/74792170-structure-assessment.md) for schema details and event compaction behavior. The bundle layout change preserves the Python pipeline logic. Historical bronze rows are not compacted by deploying code alone; rebuilding them requires a complete source archive or a separate migration plan.

## Databricks references

- [Pipeline bundle tutorial](https://docs.databricks.com/aws/en/dev-tools/bundles/pipelines-tutorial)
- [Bundle variables](https://docs.databricks.com/aws/en/dev-tools/bundles/variables)
- [Adopt existing resources](https://docs.databricks.com/aws/en/dev-tools/bundles/migrate-resources)
