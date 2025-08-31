param(
  [Parameter(Mandatory=$true)][string]$ListPath,
  [int]$Max = 120,
  [int]$PauseMs = 500
)

$ErrorActionPreference = "Stop"

# Expect to run from project root
if (-not (Test-Path ".\.venv\Scripts\python.exe")) {
  throw "Run this from the project root where .\.venv\Scripts\python.exe exists."
}

$python   = ".\.venv\Scripts\python.exe"
$discover = "scripts\discover.py"

if (-not (Test-Path $discover)) {
  throw "scripts\discover.py not found."
}

$roots = Get-Content -Path $ListPath | Where-Object { $_ -and $_ -notmatch '^\s*#' }

foreach ($r in $roots) {
  Write-Host ">>> Discover $r" -ForegroundColor Cyan
  & $python $discover $r --max $Max
  Start-Sleep -Milliseconds $PauseMs
}
