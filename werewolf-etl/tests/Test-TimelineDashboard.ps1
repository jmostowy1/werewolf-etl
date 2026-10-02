param([string]$DashboardPath = (Join-Path (Split-Path $PSScriptRoot) 'src/werewolf_dashboard.lvdash.json'))
$ErrorActionPreference = 'Stop'
$dashboard = [System.IO.File]::ReadAllText((Resolve-Path $DashboardPath)) | ConvertFrom-Json
$charts = @($dashboard.pages.layout.widget | Where-Object { $_.name -eq '81c74bd7' })
if ($charts.Count -ne 1) { throw 'Expected one game timeline widget.' }
$chart = $charts[0]
if ($chart.spec.version -ne 1 -or $chart.spec.widgetType -ne 'custom-vega-viz') {
    throw 'Timeline must use the custom-vega-viz v1 wrapper.'
}
if ($chart.spec.jsonSpec.type -ne 'vega-lite' -or $chart.spec.jsonSpec.spec -isnot [string]) {
    throw 'jsonSpec must contain a serialized Vega-Lite specification.'
}
$queries = @($chart.queries | Where-Object { $_.name -eq $chart.spec.data.queryName })
if ($queries.Count -ne 1) { throw 'data.queryName must identify exactly one widget query.' }
$query = $queries[0].query
if (-not $query.disaggregated) { throw 'Timeline query must preserve individual actions.' }
if (@($dashboard.datasets | Where-Object name -eq $query.datasetName).Count -ne 1) {
    throw 'Timeline query references a missing dataset.'
}
$bindings = @($chart.spec.encodings.fields.fieldName | Where-Object { $_ })
if ($bindings.Count -eq 0) { throw 'Missing required spec.encodings.fields: Databricks rejects this wrapper.' }
foreach ($field in $bindings) {
    if (@($query.fields | Where-Object name -eq $field).Count -ne 1) {
        throw "Encoded field '$field' must identify exactly one query field."
    }
}
$vega = $chart.spec.jsonSpec.spec | ConvertFrom-Json
if ($vega.data.name -ne 'databricks_query') { throw 'Vega data must use the databricks_query alias.' }
# This chart has no calculated output columns outside the bound input schema.
# Check direct field references and expression reads, including tooltip columns.
$references = @([regex]::Matches($chart.spec.jsonSpec.spec, '"field"\s*:\s*"([^"]+)"') | ForEach-Object { $_.Groups[1].Value })
$references += @([regex]::Matches($chart.spec.jsonSpec.spec, '\bdatum\.([a-zA-Z_][a-zA-Z_0-9]*)') | ForEach-Object { $_.Groups[1].Value })
foreach ($field in ($references | Select-Object -Unique)) {
    if ($field -notin $bindings) { throw "Vega reads unbound field '$field'." }
}
if ($vega.layer -and $chart.spec.jsonSpec.spec -match 'databricks_mark_selection') {
    throw 'Layered chart uses a selection known to break the Databricks renderer.'
}
$source = [System.IO.File]::ReadAllText((Join-Path (Split-Path $PSScriptRoot) 'src/game_timeline.vl.json')) | ConvertFrom-Json
if (($vega | ConvertTo-Json -Depth 100 -Compress) -ne ($source | ConvertTo-Json -Depth 100 -Compress)) {
    throw 'Embedded Vega-Lite differs from the standalone source.'
}
Write-Host ('Timeline dashboard checks passed: ' + $bindings.Count + ' bound columns; valid query, wrapper, and Vega references.')
