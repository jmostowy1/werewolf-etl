# Werewolf JSON structure and extraction assessment

## Recommendation

Extract three primary datasets: **episode metadata, actual player assignments, and actions**. Add the decoded moderator event log for game chronology and outcomes. The pipeline omits observation storage to reduce output size; the original JSON remains available for player-perspective analysis.

The source already contains structured actions, targets, messages, roles, outcomes, and usage metrics. Parsing narrative prompts or using an LLM to recover these fields would introduce unnecessary ambiguity. Converting the entire file to YAML improves readability but does not remove the repeated information.

This assessment examines `D:\Data\werewolf\74792170.json`, not the earlier `74788868.json`. Embedded prompts and instructions were treated as recorded data, not instructions for this assessment. Findings below are specific to this episode; proposed extraction rules should be validated against additional episodes.

## 1. File inventory

| Property | Observed value |
|---|---|
| File size | 4,813,085 bytes, approximately 4.59 MiB |
| Episode ID | `74792170` |
| Root UUID | `a950d3d0-23d2-11f1-8c0a-0242ac130206` |
| Game | `werewolf` / `Werewolf` |
| Game version / module version / schema version | `1.0.1` / `1.27.3` / `1` |
| Players | 8 |
| Step snapshots | 42, each containing 8 player records; 336 records total |
| Nonempty actions | 43 |
| Moderator batches / entries | 42 / 238 |
| Outcome | Villagers win on day 2; all eight final statuses are `DONE` |
| Error handling | One action failed; `terminated_with_agent_error` is `false` |

Array indices in this report are zero-based. A step is a snapshot, not necessarily one action: step 1 contains both a doctor action and a seer action.

### Top-level layout

```text
root
├── id, name, title, description
├── version, module_version, schema_version
├── configuration
│   ├── agents[8]                 configured model seats and initial assignments
│   └── protocols, limits, randomization flags, seed
├── specification                schema/configuration descriptions
├── info
│   ├── EpisodeId, Agents[8], TeamNames, LiveVideoPath
│   ├── GAME_END                 final results and actual player roster
│   └── MODERATOR_OBSERVATION[42][variable]
│       └── {data_type, json_str}
├── steps[42][8]
│   └── {action, info, observation, reward, status}
├── rewards[8]
└── statuses[8]
```

`specification` describes the environment rather than recording gameplay. Its action definition is broad (`type: object`, `additionalProperties: true`); it is not a complete validator for every action variant. `LiveVideoPath` is null in this file.

## 2. Sources to use for each question

| Question | Preferred source | Important qualification |
|---|---|---|
| What rules and seed were used? | `configuration` | Preserve protocols and their parameters as nested JSON. |
| Which model played which character and role? | `info.GAME_END.all_players[*].agent`, checked against observations | Configured names and roles precede randomization. |
| What did each model submit? | `steps[s][seat].action` | Read all seats; skip empty action objects. |
| What did the game report as happening? | Decoded `info.MODERATOR_OBSERVATION[s][j].json_str` | Preserve event subtype, order, and visibility. |
| What could a particular player see? | `steps[s][seat].observation.raw_observation` | These are player-specific views; repeated snapshots are common. |
| Who won and who survived? | `info.GAME_END` | Winning and surviving are different fields. |
| What did the model receive and return? | Action `kwargs.raw_prompt` and `kwargs.raw_completion` | Keep as optional audit text, separate from core analysis. |
| What were token use, cost, and failures? | Action `kwargs` | Count once per action; do not sum event copies as well. |

## 3. Player identity: resolve the randomized assignments

Both `configuration.randomize_ids` and `configuration.randomize_roles` are true. For example, configuration seat 1 lists Jordan/Werewolf, but its actual player is Alex/Doctor. Joining configuration and runtime data by configured character name would misattribute actions and outcomes.

The observed seat mapping is:

| Seat | Actual player ID | Actual role | Model |
|---|---|---|---|
| 0 | Kai | Villager | Grok 4.1 Fast Reasoning |
| 1 | Alex | Doctor | Gemini 3 Flash Preview |
| 2 | Charlie | Werewolf | Grok 4.1 Fast Reasoning |
| 3 | Casey | Villager | Grok 4 |
| 4 | Taylor | Villager | Claude Opus 4.6 |
| 5 | Quinn | Villager | GPT-5.4 mini |
| 6 | Jamie | Werewolf | Gemini 3.1 Pro Preview |
| 7 | Jordan | Seer | GPT-5.4 mini |

Each seat's populated observations consistently identify the same player. All 43 action `actor_id` values match the corresponding observation's `player_id`. Initial moderator `game_start` events agree with the final role assignments.

Use `(episode_id, player_id)` as player identity and retain `seat_index` for positional arrays such as rewards. Validate the mapping for each episode rather than assuming final roster order always matches seat order. Model names are not unique player identifiers; this game has two GPT-5.4 mini players and two Grok 4.1 Fast Reasoning players. The configured `agent_id` value `random` is also unsuitable as a unique identifier.

In the final roster, the human-readable role is `all_players[*].agent.role`. The sibling `all_players[*].role` is an empty object here.

## 4. Action records

Each populated action has this shape:

```text
action
├── action_type
└── kwargs
    ├── actor_id, day, phase
    ├── target_id OR message       depends on action type
    ├── reasoning, perceived_threat_level
    ├── prompt_tokens, completion_tokens, cost
    └── error, raw_prompt, raw_completion
```

| Action type | Count | Interpretation |
|---|---:|---|
| `ChatAction` | 24 | Player utterances |
| `VoteAction` | 12 | Day exile votes |
| `EliminateProposalAction` | 3 | Werewolf night target proposals |
| `HealAction` | 2 | Doctor choices |
| `InspectAction` | 1 | Seer choice |
| `NoOpAction` | 1 | Failed action recorded as a no-op |
| **Total** | **43** | |

Use `(episode_id, step_index, seat_index)` as the source action key. Store the action type and structured fields directly. Keep the full `kwargs` payload as an extension field if future action types must be supported.

### Timing and failure details

- An action and its containing observation do not necessarily describe the same moment. At step 1, Alex's `HealAction` is recorded while Alex's status is already `INACTIVE`; the active player has advanced. Use the action's own day/phase for the submission, and the preceding snapshot when investigating decision context.
- `observation.step` is not present consistently across seats. Use the outer array index as the source step coordinate.
- At step 25, seat 7/Jordan has a `NoOpAction` with an error reporting failure to parse LLM output. The raw output contains an attempted inspection, but the recorded action is not a successful `InspectAction`. Do not silently recover it and count it as an executed inspection.
- The episode completed despite that error. A false `terminated_with_agent_error` flag does not mean every action succeeded.
- Preserve missing values, nulls, empty strings, and zero distinctly. Record cost as the source-reported value; currency and billing semantics are not established by the numeric field alone.

## 5. Moderator events: a second JSON parsing layer

`info.MODERATOR_OBSERVATION` is an array of 42 variable-length batches. Each entry is a wrapper containing `data_type` and **a string** named `json_str`. Parse that string as JSON to obtain the event:

```text
decoded event
├── event_name, day, phase, detailed_phase
├── source, created_at, description
├── public, visible_to, visible_in_ui
└── data                          type-specific structured payload
```

There are 238 entries and 238 distinct serialized event strings. These batches are not repeated copies of one cumulative moderator log. Preserve `(episode_id, batch_index, entry_index)` as an event key and source order. Timestamps include timezone information and fractional seconds; retain the original value and use array order to break ties.

The wrapper's `data_type` describes the payload variant. `NoneType` still wraps a real event, often one with null data. `dict` is also a valid variant and often carries action telemetry. Neither should be discarded automatically.

### Do not count events by name alone

There are **48 `discussion` events but only 24 chat actions**. Each chat appears as a private `dict` event containing action details and a public `ChatDataEntry` event containing the player-facing message. Those are related representations, not two utterances.

Similarly, there are **27 `vote_action` events**:

- 12 private `dict` records for day votes;
- 12 `DayExileVoteDataEntry` records for the corresponding day votes;
- 3 `WerewolfNightVoteDataEntry` records for night proposals.

Use the action table for action counts. For a public discussion transcript, select `event_name == "discussion"` and `data_type == "ChatDataEntry"`. For event-based vote analysis, select the explicit day/night payload variants and retain phase. Keep both private and public event records in the archival event table.

Other useful event payloads include:

| Event / variant | Useful structured fields |
|---|---|
| `game_start` | `player_id`, `role`, `team` |
| `inspect_result` | `actor_id`, `target_id`, `role`, `team` |
| Night `vote_result` | `elected_target_player_id` |
| Night `elimination` | `eliminated_player_id`, role name, team name |
| `vote_order` | `vote_order_of_player_ids` |
| `discussion_order` | `chat_order_of_player_ids` |
| `game_end` | Winners, scores, roles, eliminations, final roster |

Payload shape varies within an event name; dispatch using both `event_name` and `data_type`, and retain structured payload fields. The current pipeline removes named audit/instruction text fields as documented in section 9.3; the original JSON retains the full payload. In particular, inspect the day elimination variant separately from the night elimination variant.

## 6. Where the repetition comes from

### Player snapshots and event views

Of 336 player records, 276 contain `raw_observation`. Across those 276 payloads, only **51 distinct complete observations** were found. They contain **2,534 `new_player_event_views` entries**, representing **158 distinct serialized player event views**, and 2,534 announcement strings.

The name `new_player_event_views` must not be interpreted as globally new data every time a snapshot is read. Repeated observation payloads repeat those arrays. Blindly appending all of them would greatly inflate a transcript.

A raw observation contains:

- Player identity, role, team, and alive status;
- Day, broad phase (`game_state_phase`), and detailed phase;
- Alive players, all player IDs, and revealed roles;
- Player thumbnail mapping;
- Structured player event views and parallel human-readable announcements.

The current pipeline omits these payloads and their occurrences. If observation storage is added later, store a distinct payload once and reference it from each `(episode_id, step_index, seat_index)` occurrence. For views, use a separate content identity and preserve every player/step occurrence in a linking table. Equality of payloads is useful for storage; it does not justify discarding when or to whom the payload was shown.

### Other repeated content

- Action `raw_prompt` strings contain instructions, schemas, and accumulated game history. Much of that history is available as structured events elsewhere.
- Moderator telemetry repeats prompts: 40 moderator entries contain `data.raw_prompt`, in addition to prompt fields in the action records.
- Model output can appear in `raw_completion`, extracted action fields, moderator telemetry, and subsequent prompt history.
- Each populated observation repeats a thumbnail map and player lists.
- All eight final `steps[-1][seat].info` objects equal `info.GAME_END` after consistent serialization.
- Final results provide overlapping summaries: player roster, role map, scores, winner/loser IDs, survivor map, and elimination information.

### Relative storage weight

These are approximate UTF-8 sizes after compact PowerShell JSON reserialization, **not byte offsets or exact original-file contributions**. Escaping differs from the source. Nested rows overlap and must not be summed.

| Component | Approximate serialized bytes |
|---|---:|
| Entire `steps` array | 3,437,069 |
| Entire `info` object | 1,449,043 |
| Moderator wrapper array within `info` | 1,443,523 |
| Player observations, summed across records | 2,768,557 |
| Structured player event-view arrays within observations | 1,843,918 |
| Announcement arrays within observations | 578,328 |
| Action objects, summed across records | 612,185 |
| Action `raw_prompt` values | 541,373 |
| Repeated thumbnail maps | 215,280 |

Observation repetition is a larger contributor than the final result copies. Removing only thumbnails or terminal summaries will not address most of the duplication. The distinct-payload counts indicate substantial reuse, but they are not a measured compression ratio for a finished normalized dataset.

## 7. Suggested extraction schema

| Dataset | Key | Core fields |
|---|---|---|
| `episodes` | `episode_id` | UUID, versions, seed, configuration, winner team, final day/phase, termination flag, source path/hash |
| `players` | `(episode_id, player_id)` | Seat, model name, actual role/team, role parameters, final alive status, elimination day/phase, score, winner flag, calculated `is_alive_winner` flag |
| `actions` | `(episode_id, step_index, seat_index)` | Actor, action type, day/phase, target/message, reasoning, threat level, tokens, reported cost, error, audit-text references |
| `events` | `(episode_id, batch_index, entry_index)` | Wrapper type, event name, source, timestamp, day/phases, visibility fields, retained narrative description, reduced `data_json` payload |
| `text_blobs` (optional) | Content hash | Raw prompt/completion/error text and text kind |

For a small analysis project, separate JSONL files are a straightforward output: one row per episode, player, action, or event. Use SQLite if joins and repeated queries are central. Keep optional nested payloads intact initially instead of expanding every variable field into columns. These are design recommendations; no extraction database or converter was created as part of this assessment.

Do not recursively flatten everything into one CSV. Independently expanding steps, player events, and roster arrays can multiply rows and make counts misleading. Build chat and vote views from the normalized tables after establishing their row meaning.

## 8. Extraction sequence and validation

1. Parse the source JSON once and retain the original file. At this file size, an ordinary in-memory parser is a reasonable starting point; corpus-scale processing can operate one episode at a time.
2. Read episode metadata and configuration once. Preserve version fields and distinguish the numeric episode ID from the UUID.
3. Resolve seat/player mappings from populated observations. Join actual roster records by player ID and check initial role events against final assignments.
4. Enumerate every step and every seat. Emit one action row for each nonempty action. Keep source indices even when status is inactive or done.
5. Decode each moderator `json_str` exactly once. Emit one event row per wrapper, retaining wrapper type and source coordinates. Report parse failures with their coordinates rather than silently skipping them.
6. Extract final results once. Validate scores against the positional reward array using the verified seat mapping. Preserve raw elimination day values, including day 0 and the `-1` survivor sentinel; derive nullable fields separately if desired.
7. Read source observations only to validate player identity and role/team mappings; omit observation payloads and occurrences from persisted outputs. Use the original JSON for later player-knowledge analysis.
8. Put long audit text into an optional dataset. If hashing content, use canonical JSON for objects and exact bytes for text, with a defined encoding. Preserve occurrence references even when content is shared.
9. Validate counts and relationships before analyzing behavior.

For this file, useful checks are:

- Exactly 42 snapshots, eight seats each, and 43 nonempty actions with the type counts above;
- Exactly 238 successfully decoded moderator entries, with 24 public chat events and 12 day vote payloads;
- Every action actor resolves to the observed player in its seat;
- Targets resolve to players or explicitly supported special values; do not require a player ID for every future abstention/no-op representation;
- Exactly one errored action, retained as `NoOpAction`;
- The final roster, role map, scores, winner lists, and initial role events agree;
- Event counts and visibility do not change when removing repeated storage.

### Interpretation limits

`DONE` is an environment status, not an alive/dead indicator. Casey and Taylor are dead but still appear among the six winning villagers. Public/private flags and player-specific views matter if the goal is decision analysis: final roles and moderator inspection results can reveal information a player did not have. Finally, recorded `reasoning` is a submitted text field, not independent evidence that a claimed belief or factual assertion was correct.

The most useful compact first deliverable would therefore be **one episode row, eight player rows, 43 action rows, and 238 moderator event rows**, with optional audit-text storage. Observation tables are excluded from the current pipeline. This retains explicit game facts and source provenance while making repeated snapshots and narrative histories optional for routine queries.

## 9. Databricks Declarative Pipeline definitions

### 9.1 Deployment and design

The Python source in `src/werewolf_pipeline.py` publishes the streaming bronze table `werewolf_json_normalized` and four silver materialized views: `episodes`, `players`, `actions`, and `events`. Layer membership is recorded with the `quality` table property. The bundle sets the default schema to `bronze`; the source explicitly publishes the four silver views in `silver` and the six gold views in `gold`, within the configured catalog. Both observation tables and their intermediate payloads are omitted to reduce storage. Source observations are scanned only to resolve and validate player identities. The optional `text_blobs` table is not created.

Use a **Lakeflow Spark Declarative Pipeline** with Python source support and `from pyspark import pipelines as dp`. Auto Loader incrementally ingests complete episode files into a published streaming table using `@dp.table`. The four silver definitions use batch reads and `@dp.materialized_view`. A pipeline-scoped temporary view performs shared deduplication and episode-conflict checks before silver records are expanded. [Databricks mixed streaming and materialized-view pattern](https://docs.databricks.com/aws/en/ldp/transform)

Setup:

1. Upload the original JSON files to a Unity Catalog volume or another cloud location readable by the pipeline. A Databricks worker cannot read the local Windows `D:` drive.
2. Select a target catalog and schema for the pipeline. Reserve the bronze table name and four silver view names for this pipeline.
3. Deploy from the `werewolf-etl` bundle directory using the [bundle README](../README.md). The pipeline resource loads `src/werewolf_pipeline.py`; the same file contains the gold definitions.
4. Supply the required bundle variables `catalog` and `source_path`, for example `/Volumes/main/werewolf/raw/episodes` for the latter. The bundle sets the pipeline configuration keys automatically. Keep complete source files immutable and retain them for full rebuilds and audit references.
5. Run a triggered update and execute the validation queries in section 9.4 in the target catalog/schema.

The input reader uses Auto Loader (`cloudFiles`) with `cloudFiles.format = binaryFile`, which supplies one row per discovered file with its path and binary content. This handles both one-line and pretty-printed JSON and allows hashing the exact original bytes. [Databricks Auto Loader binary-file example](https://docs.databricks.com/gcp/en/ingestion/cloud-object-storage/auto-loader/patterns)

The parser is a pure Python UDF with explicit Spark output schemas. It reads one episode per invocation and preserves variable-shaped objects as JSON strings in columns ending in `_json`. This avoids losing event fields to a narrow inferred schema. It is intended for files of the size assessed here; whole-file parsing and Python serialization should be benchmarked before using it for much larger episodes. The published bronze streaming table stores normalized records so silver refreshes read persisted data without reparsing source JSON. Spark may still reevaluate ingestion tasks during retries. Databricks manages Auto Loader checkpoints and schema locations in the pipeline; do not add a manual writeStream or checkpoint path. [Auto Loader schema and checkpoint management](https://docs.databricks.com/aws/en/ingestion/cloud-object-storage/auto-loader/schema)

### 9.2 Python pipeline source file

The complete pipeline definitions are maintained in [src/werewolf_pipeline.py](../src/werewolf_pipeline.py). Add this file to the Databricks pipeline as a Python source. The current implementation calculates `players.is_alive_winner` as true only when both `is_alive_final` and `is_winner` are true; all other combinations, including nulls, produce false. This field is calculated in silver from persisted bronze data.

### 9.3 Refresh, integrity, and payload behavior

- **Completed episode contract:** the parser requires schema version 1, a final roster, scores, rewards, and a stable observed role/team for each player. It fails with the source path and relevant coordinates if those assumptions are violated. In-progress episodes or future formats need a separate contract.
- **Refreshes:** bronze appends newly discovered files using the pipeline-managed Auto Loader checkpoint. Existing files are included on initial ingestion. Normal updates do not reread already tracked files, and deleting an ingested source file does not retract its bronze or silver records. A full refresh resets ingestion state and rebuilds from the files still available, so retain the input archive. Changes to parser logic affect new ingestions; apply a deliberate full rebuild to reparse historical files. Silver materialized views refresh from persisted bronze; no incremental-refresh performance guarantee is made for their batch windows.
- **Duplicate episodes:** bronze preserves separately discovered files, including identical copies under different paths. The temporary silver preparation view collapses byte-identical copies to the lexicographically smallest source path, then fails its expectation if different contents claim the same episode ID. This detects conflicts across ingestion batches, including differences in formatting. Conflicting files can already exist in bronze when silver validation fails; deleting the original file will not remove the conflict from persisted bronze. Resolve the source set and deliberately rebuild if required.
- **Input contract:** `cloudFiles.allowOverwrites` is explicitly false. Upload completed files at stable, unique paths; overwriting or appending to a processed file is not a supported correction workflow. [Auto Loader overwrite behavior](https://docs.databricks.com/aws/en/ingestion/cloud-object-storage/auto-loader/faq)
- **Keys:** action and event keys come from array positions. Player keys come from the validated runtime mapping. These are logical keys, not declared primary-key constraints.
- **Audit references:** `raw_prompt_pointer` and `raw_completion_pointer` are JSON Pointers into the source file identified by `source_path` and `source_sha256`; they are not foreign keys to an omitted `text_blobs` table. `kwargs_json` preserves all other submitted action fields, including unknown fields and explicit nulls. Keep source files immutable if those references must remain usable.
- **Events:** `event_json` is omitted from the bronze `events[]` schema and `silver.events`. `data_json` preserves structured results, identifiers, messages, candidates, ordering, and unknown fields while recursively removing the named audit/instruction fields below. Null payloads remain JSON `null`; no private/public event pair is collapsed. Event coordinates, visibility, `created_at_raw`, and parsed `created_at` are preserved.
- **Removed event payload fields:** `raw_prompt`, `raw_completion`, `reasoning`, `action_json_schema`, `rule_of_role`, `discussion_rule`, `voting_protocol_rule`, `day_discussion_protocol_rule`, `night_werewolf_discussion_protocol_rule`, `day_voting_protocol_rule`, and `night_werewolf_voting_protocol_rule`. Where an `error` field exists, its full value is replaced with boolean `has_error`; complete error details remain in actions and the source archive. This applies inside nested objects and arrays as well as the top-level payload.
- **Event descriptions:** descriptions are null for `dict` telemetry with event names `discussion`, `vote_action`, `eliminate_proposal_action`, `heal_action`, `inspect_action`, or `no_op_action`. Descriptions are also null when the original top-level data contains `raw_prompt` or `raw_completion`, covering additional telemetry variants. Other descriptions remain, including public chat messages and requests with null data that carry vote tallies/options only in their narrative.
- **Historical migration:** compaction occurs inside `normalize_episode()` before the streaming bronze write. Updating code alone does not compact already-ingested records. Rebuild the affected bronze table and refresh downstream silver after confirming the source archive is complete, or plan a separate migration if the source archive is incomplete. Removing columns does not immediately reclaim all old physical files; storage cleanup remains subject to Delta retention. This update changes only event storage; action, episode, and player payloads retain their existing fields.
- **Observations:** neither observation payloads nor occurrence records are persisted, including in `werewolf_json_normalized`. The parser still loads the source JSON and scans observations for player identity, role, and team validation. This reduces persisted storage and serialization work; it does not reduce source file size or whole-file parsing memory. Per-step status, reward, and overage-time histories remain available only in the original JSON.
- **Time:** `created_at_raw` retains timezone and original precision. `created_at` is a Spark timestamp for queries; set the SQL session timezone to UTC when displaying it. Array indices remain the authoritative source ordering.
- **Failures:** parser exceptions fail processing instead of silently dropping records. `expect_or_fail` rejects invalid rows in the affected flow; a pipeline update is not an atomic transaction across every published dataset. Check the overall update result before consuming a refreshed set. [Databricks expectation semantics](https://docs.databricks.com/aws/en/ldp/developer/ldp-python-ref-expectations)

### 9.4 Validation after the first update

Run these SQL statements separately with the destination catalog selected and `silver` as the current schema. Bronze references are explicitly qualified. Expected results apply to episode `74792170`; other episodes can have different counts.

```sql
-- Expected: quality=bronze for the streaming table; silver for each view.
SHOW TBLPROPERTIES bronze.werewolf_json_normalized ('quality');
SHOW TBLPROPERTIES episodes ('quality');
SHOW TBLPROPERTIES players ('quality');
SHOW TBLPROPERTIES actions ('quality');
SHOW TBLPROPERTIES events ('quality');

-- Expected: one row for this episode when only its original file is ingested.
SELECT episode_id, source_path, source_sha256
FROM bronze.werewolf_json_normalized WHERE episode_id = 74792170;

-- Expected: 1, 8, 43, and 238 respectively.
SELECT 'episodes' AS dataset, count(*) AS row_count
FROM episodes WHERE episode_id = 74792170
UNION ALL
SELECT 'players', count(*) FROM players WHERE episode_id = 74792170
UNION ALL
SELECT 'actions', count(*) FROM actions WHERE episode_id = 74792170
UNION ALL
SELECT 'events', count(*) FROM events WHERE episode_id = 74792170;

-- Expected: 24 ChatAction, 12 VoteAction, 3 EliminateProposalAction,
-- 2 HealAction, 1 InspectAction, 1 NoOpAction.
SELECT action_type, count(*) AS action_count
FROM actions WHERE episode_id = 74792170
GROUP BY action_type ORDER BY action_type;

-- Expected: one row: step 25, seat 7, Jordan, NoOpAction.
SELECT step_index, seat_index, actor_id, action_type, error
FROM actions
WHERE episode_id = 74792170 AND error IS NOT NULL AND error <> '';

-- Expected: no rows across all four logical keys.
SELECT 'episodes' AS dataset, count(*) AS duplicate_count
FROM episodes GROUP BY episode_id HAVING count(*) > 1
UNION ALL
SELECT 'players', count(*) FROM players
GROUP BY episode_id, player_id HAVING count(*) > 1
UNION ALL
SELECT 'actions', count(*) FROM actions
GROUP BY episode_id, step_index, seat_index HAVING count(*) > 1
UNION ALL
SELECT 'events', count(*) FROM events
GROUP BY episode_id, batch_index, entry_index HAVING count(*) > 1;

-- Expected: no rows; action identity must resolve to the same seat.
SELECT a.episode_id, a.step_index, a.seat_index, a.actor_id
FROM actions AS a
LEFT JOIN players AS p
  ON a.episode_id = p.episode_id AND a.actor_id = p.player_id
WHERE p.player_id IS NULL OR a.seat_index <> p.seat_index;

-- Expected: 24 public chats. The event name alone would yield 48.
SELECT count(*) AS public_chat_count
FROM events
WHERE episode_id = 74792170 AND event_name = 'discussion'
  AND data_type = 'ChatDataEntry' AND public = true;
```

**Validation status:** these definitions were prepared against the inspected file structure and checked against the linked API documentation. They have not been executed in a Databricks pipeline. The SQL above supplies acceptance checks for the first deployment; no Databricks resources have been created or modified.

### 9.5 Event compaction verification

The standard-library tests in [tests/test_event_compaction.py](../tests/test_event_compaction.py) load the actual parser helpers without importing Spark. Run from the `werewolf-etl` bundle directory:

```powershell
python -m unittest discover -s tests -p test_event_compaction.py
```

They cover recursive field removal, error flags, preservation of unknown structured fields, null payloads, narrative descriptions, and the sample's 290 events and 53 unique action/event matches. Python tests and Databricks execution have not been run in this workspace because the available Python executable cannot launch. Independent sample checks are recorded separately below.

An independent PowerShell check using the field-removal lists from the Python source preserved all 290 events and all 53 unique action/event matches in `tests/fixtures/74788868.json`, while clearing 48 telemetry descriptions. The combined `event_json`, serialized `data_json`, and description text decreased from approximately 4,794,696 bytes to 99,819 bytes (97.92%). These are uncompressed logical text estimates using PowerShell JSON serialization, not measured Delta storage savings or an execution of the Python parser. Configuration, player, and action storage is outside this measurement.

## 10. Pipeline change record

The version labels below track revisions to the pipeline definitions in `src/werewolf_pipeline.py`. They are separate from the source JSON's game, module, and schema versions and do not indicate deployed releases. Section 9 links to the current source file and documents its deployment and validation.

| Pipeline version | Change from previous version | Effect |
|---|---|---|
| v1 — Initial definitions | Introduced volume-based JSON ingestion, a private `_werewolf_normalized` intermediate, and six published datasets: `episodes`, `players`, `actions`, `events`, `observations`, and `observation_occurrences`. Added source deduplication, player identity validation, nested event decoding, observation deduplication, and validation SQL. | Established structured extraction with player observations and their step/seat occurrences available for analysis. |
| v2 — Remove observation storage | Removed `observations` and `observation_occurrences`, their intermediate arrays, observation hashing and serialization, and their validation queries. Updated the suggested schema and documentation to describe four published datasets. | Reduces persisted data and serialization work. Source observations are still scanned to validate randomized player identities, roles, and teams. Player-perspective observations and per-step status, reward, and overage-time histories must be read from the original JSON. Whole-file parsing memory is not reduced. |
| v3 — Rename and publish streaming bronze with silver views | Renames `_werewolf_normalized` to `werewolf_json_normalized` and replaces the private materialized view with a published Auto Loader streaming table with `quality=bronze`. Marks `episodes`, `players`, `actions`, and `events` as silver materialized views. Moves batch deduplication and cross-file episode validation to the temporary `_werewolf_silver_input` view, and updates downstream reads, documentation, and validation queries. | Bronze is visible in the catalog and persists ingested episodes across updates and source-file deletion. Silver refreshes use retained bronze records. Full rebuilds require the source archive; observation storage remains excluded. |
| v4 — Surviving winner flag | Adds the calculated boolean `is_alive_winner` to the silver `players` materialized view. It is true when both `is_alive_final` and `is_winner` are true, and false otherwise, including null inputs. | Enables filtering for players who both survived and won. A silver refresh computes the field for existing bronze records without reingesting source files. |
| v5 — Compact event storage (current) | Removes `event_json` from bronze and silver, recursively strips repeated audit/instruction text from `data_json`, replaces event error details with `has_error`, and clears action-telemetry descriptions. | Retains event rows, keys, timestamps, visibility, structured results, and action-matching fields. Useful narrative descriptions remain. Historical bronze rows need a rebuild or migration to realize the same reduction. |

The pipeline source was moved from section 9 into `werewolf_pipeline.py` without changing v3 behavior. This file organization change does not introduce a new pipeline version.

The repository was subsequently organized under the `werewolf-etl` bundle: source in `src/`, tests and sample data in `tests/`, and this assessment in `docs/`. Pipeline and kickoff-job resource YAML files now reference the combined source. This packaging change preserves v5 transformation logic. See the [bundle README](../README.md) for configuration, adoption of existing resources, and deployment.

The version record documents changes to the definitions. No pipeline version has been executed or deployed to Databricks from this workspace.
