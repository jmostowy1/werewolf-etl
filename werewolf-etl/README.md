# Werewolf ETL bundle

One Databricks pipeline ingests completed Werewolf episode files, produces silver records, calculates gold model/team performance, and publishes a game timeline view. The file-arrival job starts a normal pipeline update. The AI/BI dashboard compares model performance and shows the progression of individual games.

## Layout

```text
werewolf-etl/
  databricks.yml
  resources/
    werewolf.pipeline.yml
    werewolf_kickoff.job.yml
    werewolf_performance.dashboard.yml
  src/
    werewolf_pipeline.py
    ww_game_timeline.sql
    werewolf_dashboard.lvdash.json
    game_timeline.vl.json
  tests/
    test_event_compaction.py
    Invoke-Sql.ps1
    Test-GameTimeline.ps1
    Test-TimelineDashboard.ps1
    Sync-TimelineSpec.ps1
    fixtures/
      74788868.json
      timeline_cases.sql
  docs/
    74792170-structure-assessment.md
  README.md
```

The Python source includes the performance gold definitions. The SQL source defines the timeline. Both are libraries of the same pipeline. Tests and assessment files stay in Git and are excluded from bundle synchronization. `werewolf_performance.lvdash.json` is an older export; the bundle uses `werewolf_dashboard.lvdash.json`.

## Resources and outputs

| Resource key | Purpose |
|---|---|
| `werewolf` | Serverless, triggered pipeline using the Python and SQL sources |
| `werewolf-etl-kickoff` | File-arrival job referencing `${resources.pipelines.werewolf.id}` |
| `werewolf_performance` | AI/BI dashboard using the configured SQL warehouse |

Within the configured catalog:

- `bronze.werewolf_json_normalized`: streaming ingestion of normalized episode files.
- `silver.episodes`, `silver.players`, `silver.actions`, `silver.events`: materialized views.
- `gold.model_role_performance`, `gold.model_overall_performance`, `gold.model_team_performance`, `gold.model_pair_performance`, `gold.team_lineup_performance`, `gold.team_episode_results`: materialized views.
- `gold.game_timeline`: a persistent Unity Catalog **regular view** joining silver actions, events, and players. It stores its SQL definition, so full action text is not copied into another gold table.

The code names the `silver` and `gold` schemas explicitly. A separate environment needs a separate catalog; changing only the pipeline's default schema does not isolate these outputs. The catalog and source volume must exist, and the execution identity needs permission to read the source and create the outputs.

## Configuration

The current `databricks.yml` defines only the `prod` target. Its workspace, run identity, and variable values are checked in:

| Variable | Current value / purpose |
|---|---|
| `catalog` | `workspace`: destination catalog; default pipeline schema is `bronze` |
| `source_path` | `/Volumes/workspace/landing/landing/`: completed episode JSON files |
| `warehouse_id` | `3f5e230aa22fb0a1`: existing Serverless Starter Warehouse |
| `gold_min_games` | Default `20`: minimum games for ranking eligibility |

The pipeline resource maps variables to `werewolf.source_path`, `werewolf.silver_schema`, and `werewolf.gold_min_games`. Input files remain in the volume. For another environment, add a target and its variables; the examples below use the existing production target.

From the repository root in PowerShell:

```powershell
Set-Location .\werewolf-etl
databricks auth login --host https://dbc-b14fd7ae-c2b5.cloud.databricks.com --profile werewolf
databricks bundle validate -t prod -p werewolf
```

## Deployment and existing resource bindings

The production pipeline and dashboard are already bound to this bundle. Use selected deployment for their changes:

```powershell
databricks bundle plan -t prod -p werewolf --select pipelines.werewolf,dashboards.werewolf_performance
databricks bundle deploy -t prod -p werewolf --select pipelines.werewolf
databricks bundle run werewolf -t prod -p werewolf
# After the pipeline update succeeds:
databricks bundle deploy -t prod -p werewolf --select dashboards.werewolf_performance
databricks bundle summary -t prod -p werewolf
```

A normal update publishes the timeline view; no bronze full refresh is needed. Deploying code alone does not run the pipeline.

The kickoff job definition currently uses key `werewolf-etl-kickoff`, while earlier deployment state used `werewolf_kickoff`. Reconcile that existing job binding before a full bundle deployment so the rename does not unintentionally create or replace a job. The timeline deployment selected only the pipeline and dashboard. The job's file-arrival trigger is unpaused and currently watches the production source volume.

For a new target adopting resources that already exist, bind the appropriate IDs before deployment:

```powershell
databricks bundle deployment bind werewolf YOUR_PIPELINE_ID -t YOUR_TARGET -p werewolf
databricks bundle deployment bind werewolf-etl-kickoff YOUR_JOB_ID -t YOUR_TARGET -p werewolf
databricks bundle deployment bind werewolf_performance YOUR_DASHBOARD_ID -t YOUR_TARGET -p werewolf
```

Resource bindings are per target. Check the resource configuration and deployment plan before adopting existing resources. After the job binding is reconciled, a full deployment and job run use:

```powershell
databricks bundle deploy -t prod -p werewolf
databricks bundle run werewolf-etl-kickoff -t prod -p werewolf
```

## Dashboard

[Open the production dashboard](https://dbc-b14fd7ae-c2b5.cloud.databricks.com/dashboardsv3/01f1be6b23c114628ecbced60e61b187/published).

The editable export is [src/werewolf_dashboard.lvdash.json](src/werewolf_dashboard.lvdash.json), configured by [resources/werewolf_performance.dashboard.yml](resources/werewolf_performance.dashboard.yml). It contains five pages and twelve datasets:

- Overall Model Performance
- Seat Performance 1
- Seat Performance 2
- Model Pair Performance
- Game progression

The first four pages retain the supplied charts and queries. Some existing datasets explicitly reference `workspace`; changing the bundle catalog does not retarget those references. The new timeline queries use `gold.game_timeline` and `silver.episodes` in the configured catalog.

The resource key `werewolf_performance` preserves the dashboard ID and URL. Publishing uses viewer credentials (`embed_credentials: false`); viewers need dashboard, warehouse, and data access. To bring later UI edits back into the repository, use `databricks bundle generate dashboard --resource werewolf_performance -t prod -p werewolf` and review the diff before redeploying.

### Game progression

Choose one episode (default `74788868`), and optionally a day/night phase. The dataset binds the `timeline_episode` SQL parameter before retrieving action text. It does not retrieve full reasoning for every episode and then filter in the browser.

Players appear in seat order on the vertical axis. The horizontal axis follows source event/step order. Shaded columns distinguish day and night. Invisible roster anchors keep eliminated or nonacting players on the axis when a phase is selected.

| Marker | Meaning |
|---|---|
| Colored circle | One actor action; color identifies its type |
| Text above a vote circle | That actor's target, including `Abstain` for target `-1` |
| Purple outlined diamond | Final elected target |
| Green square | Successful save |
| Red cross | Recorded elimination |

Outcome markers appear on the affected player's row in separate chronological columns. A wolf election does not imply a death: a save and an elimination are separate source events. A day exile produces an election and an elimination. A tie/no-target result uses a `Game outcome` row; the view does not guess a victim.

Hover fields include player/model/role/team, target, action, outcome, source coordinates, event timestamp, grouping status, threat level, token/cost/error data, **full reasoning**, and **full message**. Neither SQL nor the Vega specification truncates these text values. Tooltip presentation is controlled by Databricks. The detail table also includes full text and follows the episode and day/night controls.

The chart uses Databricks' custom Vega-Lite visualization and its reserved `databricks_query` dataset. Custom visualizations must be available in the destination workspace. Click-to-filter is disabled because Databricks' renderer has a documented failure with selections in layered Vega specifications. Layers are needed for vote labels and outcome markers; use the native filters and detail table to inspect records.

### Timeline grouping and provenance

[ww_game_timeline.sql](src/ww_game_timeline.sql) creates one action marker per episode/step/seat and deduplicates matching telemetry. Actions match events on episode, step/batch, event type, actor, target, and chat message where applicable. An unmatched action remains visible with a null timestamp and an explicit status.

Discussion and voting groups use recorded player-order events. An order wrap advances a shared round, so a skipped speaker does not shift every later action for that player. Separate order events create separate voting sessions within the same phase. Round numbers are **inferred**: retries or wholly missing rounds cannot always be distinguished from the retained records. Without a protocol order, actions remain separate steps rather than being assigned a guessed round.

`group_order` is numeric; display labels are not used to determine chronology. Outcomes use event coordinates and their structured targets. Full text is joined from `silver.actions`. Roster anchors carry no action text. This view is for visualization; use the existing silver and performance tables for analytics.

### Editing the Vega-Lite chart

Edit [src/game_timeline.vl.json](src/game_timeline.vl.json), then synchronize its embedded copy into the dashboard export:

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File tests/Sync-TimelineSpec.ps1
powershell -NoProfile -ExecutionPolicy Bypass -File tests/Test-TimelineDashboard.ps1
databricks bundle validate -t prod -p werewolf
databricks bundle deploy -t prod -p werewolf --select dashboards.werewolf_performance
```

The sync script updates the embedded chart specification and generates the required `spec.encodings.fields` list from the chart query. PowerShell may reformat the surrounding JSON. Review the diff. If a UI export changes the custom chart, update the standalone specification before synchronizing it.

### Invalid-widget repair

The first timeline deployment omitted `spec.encodings.fields`. Databricks therefore replaced the invalid custom widget with a blank bar widget and reported `cannot-fix-invalid-spec`. Rendering the inner Vega JSON locally did not detect this outer wrapper error.

The corrected wrapper binds all 40 query columns. The layered chart's `databricks_mark_selection` parameter and selection-dependent opacity were also removed to avoid a separate renderer limitation. Vote labels, outcome shapes, full-text tooltips, and the detail table remain. See the [Databricks custom visualization reference](https://github.com/databricks/databricks-agent-skills/blob/main/plugins/databricks/copilot/skills/databricks-aibi-dashboards/references/6-custom-visualizations.md) for the binding requirements and layered-selection limitation.

`Test-TimelineDashboard.ps1` checks the outer widget version, query binding, encoded columns, Vega field references, and the layered-selection limitation. It reproduces the missing-encoding failure on the original deployed export and passes on the repaired export. This check runs without a warehouse and is also included in the full timeline test script.

The repair was republished on October 2, 2026 at 17:07:15 UTC. The downloaded dashboard passes the new wrapper checks, the corrected chart renders the sample data locally, and the dashboard deployment plan reports no pending changes. An authenticated hosted-browser check was unavailable: the workspace web page requires an interactive sign-in even with the existing CLI API authentication.

### Replacement of `gold.game_action`

The bundle dashboard now uses `gold.game_timeline`; it has no dependency on `gold.game_action`. The standalone `ww_game_actions.py` overwrite writer is no longer part of the codebase.

The existing remote `workspace.gold.game_action` table was retained because the separate **Gold ETL Dashboard** (`01f1bce81bd51eed8bc9f7f335a866cc`) still references it. Retire that table only after migrating or retiring its remaining consumers. No compatibility table or duplicate text storage was introduced for the new dashboard.

## Tests and deployment verification

Run the timeline tests from `werewolf-etl`:

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File tests/Test-GameTimeline.ps1
# Optional overrides:
# -Profile YOUR_PROFILE -WarehouseId YOUR_WAREHOUSE_ID
```

These tests execute the actual view query against synthetic silver inputs on the SQL warehouse. They create no persistent tables. Twelve assertions cover action preservation, unique markers, skipped speakers, repeated vote sessions, abstentions, ties, duplicate outcomes, saves, missing telemetry, numeric chronology, episode isolation, and overlapping action markers. They also check the dashboard's embedded specification against the standalone source. The SQL helper requires an authenticated Databricks CLI profile; request/result files are written under ignored `.databricks/`.

Existing parser tests use Python 3.9 or newer:

```powershell
python -m unittest discover -s tests -p test_event_compaction.py
```

Those tests load the parser helpers without importing Spark and verify event compaction and all 53 sample action/event matches. The pipeline runs in Databricks and requires `pyspark.pipelines`.

Verification on October 2, 2026:

- Twelve synthetic timeline assertions passed on the warehouse.
- Pipeline update `233f9142-d716-450f-93ef-f4dc88934bf5` completed without a full refresh; Unity Catalog reports `workspace.gold.game_timeline` as `VIEW`.
- The published view returned 103 unique markers across 24 groups for episode `74788868`: 53 actions, 5 elected targets, 4 eliminations, 1 save, and 40 invisible roster anchors. All 53 actions matched event timestamps and retained reasoning; 28 retained chat messages.
- The standalone Vega-Lite specification compiled and rendered in headless Chrome using data from that published view. Vote labels, outcome markers, chronology, and player rows were visually inspected.
- The production dashboard API confirms five pages, twelve datasets, the custom visualization, and a published revision dated October 2, 2026. Publishing uses the existing warehouse and viewer credentials.
- The dashboard's exact SQL query succeeded with the `timeline_episode` parameter and returned the same 103 rows. The other four page definitions are unchanged.
- Bundle validation passed with the existing unmatched `__pycache__` exclusion warning. The final selected deployment plan reported zero additions, changes, or deletions for the pipeline and dashboard.
- Hosted Databricks hover behavior has not been exercised in an authenticated browser. Local rendering and API publication checks do not establish that interaction behavior. The invalid-widget repair above adds wrapper checks missing from the initial verification.
- The existing Python parser suite was not rerun locally because this machine's Python launcher is unavailable; the parser source was unchanged.

Earlier dashboard deployments on October 1-2 used the development dashboard (`01f1be14a05c16f7ba4089e5e7a51543`). This change targets the current production dashboard; it does not update that older dashboard.

## Pipeline history

See the [assessment and change record](docs/74792170-structure-assessment.md) for ingestion schemas and event compaction history. The timeline addition preserves the Python pipeline and its bronze/silver schemas. Historical bronze rows are not compacted by deploying code alone; rebuilding them requires a complete source archive or a separate migration plan.

## References

- [Published views in pipelines](https://docs.databricks.com/aws/en/ldp/developer/ldp-sql-ref-create-view)
- [Custom dashboard visualizations](https://docs.databricks.com/aws/en/dashboards/manage/visualizations/custom-visualizations)
- [Bundle variables](https://docs.databricks.com/aws/en/dev-tools/bundles/variables)
- [Adopt existing resources](https://docs.databricks.com/aws/en/dev-tools/bundles/migrate-resources)
