param([string]$Profile = 'werewolf', [string]$WarehouseId = '3f5e230aa22fb0a1')
$ErrorActionPreference = 'Stop'
$utf8 = New-Object System.Text.UTF8Encoding($false)
$root = Split-Path $PSScriptRoot -Parent
Set-Location $root
$source = [System.IO.File]::ReadAllText((Join-Path $root 'src/ww_game_timeline.sql'))
$body = $source.Substring($source.IndexOf('WITH event_fields')).Trim().TrimEnd(';')
$synthetic = $body.Replace('silver.actions','test_actions').Replace('silver.events','test_events').Replace('silver.players','test_players')
$inputs = [System.IO.File]::ReadAllText((Join-Path $PSScriptRoot 'fixtures/timeline_cases.sql'))
$assertions = @'
SELECT check.* FROM (SELECT explode(array(
named_struct('test', 'all actions retained once', 'passed', count_if(marker_kind = 'action') = 12),
named_struct('test', 'unique marker IDs', 'passed', count(*) = count(DISTINCT marker_id)),
named_struct('test', 'skipped speakers retain shared rounds', 'passed', count_if(
  marker_kind = 'action' AND episode_id = 9001 AND action_type = 'ChatAction'
  AND action_round = CASE WHEN step_index <= 2 THEN 0 ELSE 1 END) = 4),
named_struct('test', 'separate voting sessions', 'passed', count(DISTINCT CASE
  WHEN marker_kind = 'action' AND action_type = 'VoteAction' THEN group_id END) = 2),
named_struct('test', 'abstention label', 'passed', count_if(marker_kind = 'action' AND target_label = 'Abstain') = 1),
named_struct('test', 'tie on game row without a death', 'passed', count_if(marker_kind = 'vote_result' AND display_player_id = '__game__') = 1
  AND count_if(marker_kind = 'elimination' AND batch_index = 9) = 0),
named_struct('test', 'duplicate event does not duplicate exile', 'passed', count_if(marker_kind = 'elimination') = 1),
named_struct('test', 'save is not a death', 'passed', count_if(marker_kind = 'save' AND display_player_id = 'A') = 1
  AND count_if(marker_kind = 'elimination' AND display_player_id = 'A') = 0),
named_struct('test', 'unknown action retains text and missing timestamp', 'passed', count_if(action_type = 'FutureAction'
  AND reasoning = 'Full reasoning 16' AND created_at IS NULL AND action_round IS NULL) = 1),
named_struct('test', 'numeric/source chronology', 'passed', max(CASE WHEN day = 10 THEN group_order END)
  > max(CASE WHEN day = 1 THEN group_order END)),
named_struct('test', 'episodes isolated', 'passed', count_if(episode_id = 9002 AND marker_kind = 'action' AND group_order = 0) = 1),
named_struct('test', 'no overlapping action markers', 'passed', count_if(marker_kind = 'action') =
  count(DISTINCT CASE WHEN marker_kind = 'action' THEN named_struct('episode', episode_id, 'group', group_id, 'player', display_player_id) END))
)) AS check FROM timeline)
'@
$query = 'WITH ' + $inputs + ', timeline AS (' + $synthetic + ') ' + $assertions
$queryFile = Join-Path $root '.databricks/timeline-tests.sql'
[System.IO.Directory]::CreateDirectory((Split-Path $queryFile)) | Out-Null
[System.IO.File]::WriteAllText($queryFile, $query, $utf8)
& "$PSScriptRoot/Invoke-Sql.ps1" -SqlFile $queryFile -Profile $Profile -WarehouseId $WarehouseId -OutputFile '.databricks/timeline-tests-result.json'
$result = Get-Content -Raw -Encoding UTF8 .databricks/timeline-tests-result.json | ConvertFrom-Json
$failed = @($result.result.data_array | Where-Object { $_[1] -ne 'true' })
if ($failed.Count) { throw ('Timeline assertions failed: ' + ($failed | ConvertTo-Json -Compress)) }
Write-Host ('Passed ' + $result.result.row_count + ' timeline SQL assertions.')

# Preserve all other dashboard pages and ensure the embedded specification is
# exactly the editable Vega-Lite source; validate all timeline field references.
$dashboard = Get-Content -Raw -Encoding UTF8 src/werewolf_dashboard.lvdash.json | ConvertFrom-Json
$page = $dashboard.pages | Where-Object displayName -eq 'Game progression'
$chart = $page.layout.widget | Where-Object { $_.spec.widgetType -eq 'custom-vega-viz' }
$vega = Get-Content -Raw -Encoding UTF8 src/game_timeline.vl.json | ConvertFrom-Json
if (($vega | ConvertTo-Json -Depth 100 -Compress) -ne (($chart.spec.jsonSpec.spec | ConvertFrom-Json) | ConvertTo-Json -Depth 100 -Compress)) { throw 'Embedded Vega-Lite differs from source file' }
if (-not $chart.queries[0].query.disaggregated) { throw 'Timeline must not aggregate actions' }
if ($dashboard.datasets.config.source -contains 'workspace.gold.game_action') { throw 'Obsolete game_action dataset remains' }
Write-Host 'Dashboard definition checks passed.'
& "$PSScriptRoot/Test-TimelineDashboard.ps1"
