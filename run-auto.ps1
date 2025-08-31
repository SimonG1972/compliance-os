# run-auto.ps1
$ErrorActionPreference = "Stop"
Set-Location "C:\Users\scgla\OneDrive\Desktop\compliance-os"

# Optional venv activation:
# & ".\.venv\Scripts\Activate.ps1"

# Ensure logs dir
New-Item -ItemType Directory -Force -Path ".\logs" | Out-Null
$ts = Get-Date -Format "yyyyMMdd_HHmmss"
$log = ".\logs\auto_$ts.log"

# Tweak args if desired
$cmd = "python -m src.cli auto --static-limit 120 --js-limit 25 --js-timeout-ms 45000"
Write-Host "Running: $cmd"
& cmd.exe /c "$cmd 1>>$log 2>&1"
Write-Host "Log written to $log"
