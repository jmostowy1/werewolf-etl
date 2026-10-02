param(
    [Parameter(Mandatory=$true)][string]$SqlFile,
    [string]$Profile = 'werewolf',
    [string]$WarehouseId = '3f5e230aa22fb0a1',
    [string]$Catalog = 'workspace',
    [string]$Schema = 'gold',
    [string]$OutputFile = '.databricks/timeline-sql-result.json',
    [switch]$Quiet
)
$ErrorActionPreference = 'Stop'
$utf8 = New-Object System.Text.UTF8Encoding($false)
$requestPath = Join-Path (Get-Location) '.databricks/timeline-sql-request.json'
$request = @{
    warehouse_id = $WarehouseId; catalog = $Catalog; schema = $Schema
    statement = [System.IO.File]::ReadAllText((Resolve-Path $SqlFile))
    wait_timeout = '10s'; row_limit = 10000
}
[System.IO.Directory]::CreateDirectory((Split-Path $requestPath)) | Out-Null
[System.IO.File]::WriteAllText($requestPath, ($request | ConvertTo-Json -Depth 10), $utf8)
$raw = databricks api post /api/2.0/sql/statements --json ('@' + $requestPath) -p $Profile
if ($LASTEXITCODE -ne 0) { throw 'SQL submission failed' }
$result = ($raw -join "`n") | ConvertFrom-Json
while ($result.status.state -in @('PENDING', 'RUNNING')) {
    Write-Host ('SQL ' + $result.statement_id + ': ' + $result.status.state)
    Start-Sleep -Seconds 5
    $raw = databricks api get ('/api/2.0/sql/statements/' + $result.statement_id) -p $Profile
    if ($LASTEXITCODE -ne 0) { throw 'SQL status request failed' }
    $result = ($raw -join "`n") | ConvertFrom-Json
}
[System.IO.File]::WriteAllText((Join-Path (Get-Location) $OutputFile), ($raw -join "`n"), $utf8)
if ($result.status.state -ne 'SUCCEEDED') { throw ($result.status | ConvertTo-Json -Depth 10) }
if ($Quiet) { Write-Host ('SQL succeeded: ' + $result.result.row_count + ' rows.') }
else { $result.result.data_array | ForEach-Object { ConvertTo-Json -InputObject $_ -Compress } }
if ($result.manifest.truncated) { Write-Warning 'Result rows truncated; do not use for completeness assertions.' }
