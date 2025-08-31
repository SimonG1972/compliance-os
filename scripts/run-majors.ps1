Param(
  [string]$CsvPath = ".\scripts\platforms.csv",
  [int]$BatchSize = 6
)

$ErrorActionPreference = "Stop"

function Run-Cli {
  param([string[]]$ArgsArray)
  Write-Host (">> python -m src.cli {0}" -f ($ArgsArray -join ' ')) -ForegroundColor Cyan
  & python -m src.cli @ArgsArray
}

function Get-Field {
  param(
    [pscustomobject]$Row,
    [string[]]$Names
  )
  foreach ($n in $Names) {
    if ($Row.PSObject.Properties.Name -contains $n) {
      $v = $Row.$n
      if ($null -ne $v) {
        $s = "$v".Trim()
        if ($s.Length -gt 0) { return $s }
      }
    }
  }
  return ""
}

if (-not (Test-Path $CsvPath)) {
  Write-Host ("Seed file not found: {0}" -f $CsvPath) -ForegroundColor Red
  exit 1
}

$rows = Import-Csv $CsvPath
if (-not $rows -or $rows.Count -eq 0) {
  Write-Host ("No rows in {0}" -f $CsvPath) -ForegroundColor Red
  exit 1
}

$index = 0
while ($index -lt $rows.Count) {
  $batchEnd = [Math]::Min($index + $BatchSize - 1, $rows.Count - 1)
  $batch = $rows[$index..$batchEnd]

  Write-Host ""
  Write-Host ("=== Batch starting at index {0} (size up to {1}) ===" -f $index, $BatchSize) -ForegroundColor Yellow

  foreach ($r in $batch) {
    $domain = Get-Field -Row $r -Names @('Domain','domain','Host','host','Homepage','homepage','Site','site','Base','base')
    $brand  = Get-Field -Row $r -Names @('Brand','brand','Name','name','Platform','platform')

    if (-not $domain) {
      Write-Host "Skipping row (no domain found in expected columns)." -ForegroundColor DarkYellow
      continue
    }

    Write-Host ""
    $brandLabel = if ($brand) { $brand } else { "<no brand>" }
    Write-Host ("---- {0} - {1} ----" -f $brandLabel, $domain) -ForegroundColor Green

    # 1) discovery by homepage (no forced 'www.'; lets subdomains work)
    try {
      Run-Cli @('discover','--homepage',("https://{0}" -f $domain))
    } catch {
      Write-Host ("discover --homepage failed for {0} (continuing)" -f $domain) -ForegroundColor DarkYellow
    }

    # 2) discovery by brand
    try {
      if ($brand) {
        Run-Cli @('discover','--brand',$brand)
      }
    } catch {
      Write-Host ("discover --brand failed for {0} (continuing)" -f $brand) -ForegroundColor DarkYellow
    }

    # (Intentionally no --site query; CLI does not support it)
  }

  # 3) fetch & index after each batch
  try { Run-Cli @('fetch-all') } catch { Write-Host "fetch-all failed (continuing)" -ForegroundColor DarkYellow }
  try { Run-Cli @('reindex') }   catch { Write-Host "reindex failed (continuing)"   -ForegroundColor DarkYellow }

  # 4) quick sanity search
  try { Run-Cli @('search','terms OR privacy OR community') } catch { Write-Host "search sanity check failed (continuing)" -ForegroundColor DarkYellow }

  $index += $BatchSize
}

Write-Host ""
Write-Host "=== Done. You now have a searchable corpus for the majors. ===" -ForegroundColor Yellow
Write-Host 'Try: python -m src.cli search "endorsement OR counterfeit OR privacy"' -ForegroundColor Cyan
