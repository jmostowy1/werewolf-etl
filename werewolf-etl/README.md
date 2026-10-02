# Werewolf ETL bundle

One Databricks pipeline ingests completed Werewolf episode files, produces silver records, and calculates gold model/team performance. The kickoff job starts a normal pipeline update.

## Layout

```text
werewolf-etl/
├── databricks.yml
├── resources/
│   ├── werewolf.pipeline.yml
│   └── werewolf_kickoff.job.yml
├── src/
│   └── werewolf_pipeline.py
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
| `werewolf_kickoff` | Job referencing `${resources.pipelines.werewolf.id}`; `full_refresh: false` |

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
| `gold_min_games` | No; default `20` | Minimum games for gold ranking eligibility |

The resource definition maps these to `werewolf.source_path`, `werewolf.silver_schema`, and `werewolf.gold_min_games`. Input JSON files remain in the volume; the bundle deploys code and configuration.

For example, from the repository root in PowerShell:

```powershell
Set-Location .\werewolf-etl
$env:BUNDLE_VAR_catalog = "YOUR_DEV_CATALOG"
$env:BUNDLE_VAR_source_path = "/Volumes/YOUR_CATALOG/YOUR_SCHEMA/YOUR_VOLUME/episodes"
databricks auth login --host https://dbc-b14fd7ae-c2b5.cloud.databricks.com --profile werewolf
databricks bundle validate -t dev -p werewolf
```

Replace the example values before validation. The resource uses serverless compute; confirm this matches the existing pipeline when adopting it.

## Adopt existing resources

The checked-in kickoff definition is a minimal, manually triggered job. No existing job ID, schedule, notifications, or additional tasks were available in the repository. Before deploying over an existing job, reconcile those settings with this definition or export the existing job configuration. Keep the pipeline ID as the bundle substitution rather than a literal ID.

Bind each resource before the first deployment if it already exists:

```powershell
databricks bundle deployment bind werewolf YOUR_PIPELINE_ID -t dev -p werewolf
databricks bundle deployment bind werewolf_kickoff YOUR_JOB_ID -t dev -p werewolf
```

Match the existing catalog/schema, compute settings, and execution identity before binding/deploying. Resource bindings belong to a target; bind production separately if it already exists. Without binding, the bundle creates new resources.

## Deploy and run

From the bundle directory, with variables set:

```powershell
databricks bundle validate -t dev -p werewolf
databricks bundle deploy -t dev -p werewolf
databricks bundle run werewolf_kickoff -t dev -p werewolf
```

For production, set production variable values and use `-t prod`. The kickoff job has no schedule by default. Add the existing schedule/trigger when adopting the job if automatic execution is needed.

In a Databricks workspace bundle editor, the Deployments panel can also deploy and run these resources after the required variables are configured.

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
