param()
$ErrorActionPreference = 'Stop'
$bundleRoot = Split-Path $PSScriptRoot
$specPath = Join-Path $bundleRoot 'src/game_timeline.vl.json'
$dashboardPath = Join-Path $bundleRoot 'src/werewolf_dashboard.lvdash.json'
$spec = [System.IO.File]::ReadAllText($specPath) | ConvertFrom-Json
$dashboard = [System.IO.File]::ReadAllText($dashboardPath) | ConvertFrom-Json
$charts = @($dashboard.pages.layout.widget | Where-Object { $_.name -eq '81c74bd7' })
if ($charts.Count -ne 1) { throw 'Expected exactly one game timeline chart (81c74bd7).' }
$charts[0].spec.jsonSpec.spec = $spec | ConvertTo-Json -Depth 100 -Compress
# Databricks validates this outer binding before it evaluates the Vega-Lite JSON.
# A query field alone is not enough: every available column needs an encoding.
$boundQuery = @($charts[0].queries | Where-Object { $_.name -eq $charts[0].spec.data.queryName })
if ($boundQuery.Count -ne 1) { throw 'Timeline data.queryName must identify one widget query.' }
$encodings = [pscustomobject]@{
    fields = @($boundQuery[0].query.fields | ForEach-Object { [pscustomobject]@{fieldName = $_.name} })
}
$charts[0].spec | Add-Member -MemberType NoteProperty -Name encodings -Value $encodings -Force
$utf8 = New-Object System.Text.UTF8Encoding($false)
[System.IO.File]::WriteAllText($dashboardPath, ($dashboard | ConvertTo-Json -Depth 100) + "`n", $utf8)
Write-Host 'Embedded game_timeline.vl.json in werewolf_dashboard.lvdash.json.'
