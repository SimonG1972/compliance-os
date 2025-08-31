# run_discover_tier1.ps1
# Loops through Tier 1 social media seeds and runs discovery on each

$python = ".\.venv\Scripts\python.exe"
$script = "scripts\discover.py"
$seeds = Get-Content ".\seeds_social_tier1.txt"

foreach ($root in $seeds) {
    Write-Host ">>> Discovering $root ..." -ForegroundColor Cyan
    & $python $script $root --max 200
    Start-Sleep -Seconds 3   # small pause between runs
}
