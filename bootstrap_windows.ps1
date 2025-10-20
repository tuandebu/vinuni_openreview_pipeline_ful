param(
  [string]$Venue = "",
  [string]$PaperId = "",
  [int]$Limit = 50,
  [string]$Outdir = "data\output",
  [switch]$WithPdfs,
  [switch]$Summary
)

# 0) Always run from the script's folder
Set-Location -Path $PSScriptRoot

# 1) Clear PYTHONPATH for this session to avoid D:\Lib pollution
if ($env:PYTHONPATH) { Remove-Item Env:PYTHONPATH -ErrorAction SilentlyContinue }

# 2) Make venv if missing
if (-not (Test-Path ".\.venv\Scripts\python.exe")) {
  if (Get-Command py -ErrorAction SilentlyContinue) { py -m venv .venv } else { python -m venv .venv }
}

$py = ".\.venv\Scripts\python.exe"

# 3) Ensure pip is available
try {
  & $py -m ensurepip --upgrade | Out-Null
} catch {}

# Fallback: if pip still missing, try get-pip.py when available
$hasPip = $false
try {
  & $py -m pip --version | Out-Null
  $hasPip = $true
} catch { $hasPip = $false }

if (-not $hasPip) {
  if (Test-Path ".\get-pip.py") {
    & $py .\get-pip.py
  } else {
    Write-Host "pip not found. Please temporarily download get-pip.py from https://bootstrap.pypa.io/get-pip.py"
    exit 1
  }
}

# 4) Install deps
& $py -m pip install --upgrade pip setuptools wheel
& $py -m pip install -r requirements.txt

# 5) Build args and run
$common = @("--outdir", $Outdir)
if ($WithPdfs) { $common += "--with-pdfs" }
if ($Summary)  { $common += "--summary-csv" }

if ($PaperId -ne "") {
  & $py ".\run.py" "--paper-id" $PaperId @common
} elseif ($Venue -ne "") {
  & $py ".\run.py" "--venue" $Venue "--limit" $Limit @common
} else {
  Write-Host "Provide -Venue or -PaperId"
  exit 1
}
