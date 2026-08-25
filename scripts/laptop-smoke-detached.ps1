[CmdletBinding()]
param(
    [string]$ProfilePath = "state/laptop-profile-s4u.json",
    [ValidateSet("CurrentUser", "LocalMachine")]
    [string]$ProfileScope = "CurrentUser",
    [ValidateSet(5, 30)]
    [int]$SmokeMinutes
)

$ErrorActionPreference = "Stop"
Set-StrictMode -Version Latest

$root = Split-Path -Parent $PSScriptRoot
Set-Location $root
if ($ProfileScope -ceq "LocalMachine") {
    . "$PSScriptRoot/laptop-load-s4u-env.ps1" -ProfilePath $ProfilePath
} else {
    . "$PSScriptRoot/laptop-load-env.ps1" -ProfilePath $ProfilePath
}
. "$PSScriptRoot/laptop-disable-shadow-telegram.ps1"

$env:IPEG_LAPTOP_SMOKE_RUN_ID = [Guid]::NewGuid().ToString("N")
$env:IPEG_STATE_PATH = [IO.Path]::GetFullPath(
    (Join-Path $root "state/laptop/smoke/$env:IPEG_LAPTOP_SMOKE_RUN_ID/ipeg.sqlite3")
)
$env:IPEG_PARQUET_DIR = [IO.Path]::GetFullPath(
    (Join-Path $root "data/laptop/smoke/$env:IPEG_LAPTOP_SMOKE_RUN_ID/market")
)
$python = Join-Path $root ".venv/Scripts/python.exe"
$runner = Join-Path $root "scripts/laptop_smoke_runner.py"
$manifestPath = Join-Path $root "state/laptop/native-runtime-manifest.json"
foreach ($required in @($python, $runner)) {
    if (-not (Test-Path -LiteralPath $required -PathType Leaf)) {
        throw "Detached smoke prerequisite is missing: $required"
    }
}

$timeService = Get-Service W32Time
if ($timeService.Status -ne "Running" -or $timeService.StartType -ne "Automatic") {
    throw "W32Time must be Running with Automatic startup"
}
& $python -m pip check
if ($LASTEXITCODE -ne 0) { throw "pip check failed" }
& $python scripts/check_lock.py --lock requirements.lock --pyproject pyproject.toml
if ($LASTEXITCODE -ne 0) { throw "dependency lock check failed" }
& $python -m interexchange_perp_grid.cli native-runtime-manifest `
    --output $manifestPath --repo-root $root --config "$root/config/defaults.yaml"
if ($LASTEXITCODE -ne 0) { throw "native runtime manifest failed" }
$manifest = Get-Content -LiteralPath $manifestPath -Raw | ConvertFrom-Json
$env:IPEG_RELEASE_SHA = [string]$manifest.release_sha
$env:IPEG_CONTAINER_IMAGE_DIGEST = [string]$manifest.artifact_digest
$env:IPEG_NATIVE_RUNTIME_MANIFEST = $manifestPath

foreach ($venue in @("bybit", "okx")) {
    & $python -m interexchange_perp_grid.cli private-probe --venue $venue --authenticated
    if ($LASTEXITCODE -ne 0) { throw "$venue private capability probe failed" }
}

$lockPath = Join-Path $root "state/laptop/qualification-smoke.lock"
$existingLock = Get-Item -LiteralPath $lockPath -ErrorAction SilentlyContinue
if ($null -ne $existingLock) {
    if (((Get-Date) - $existingLock.LastWriteTime).TotalSeconds -lt 120) {
        throw "A detached qualification smoke launch is already locked"
    }
    $oldOwner = (Get-Content -LiteralPath $lockPath -Raw).Trim().Split("|")[0]
    if ($oldOwner -notmatch "^[0-9a-f]{32}$") {
        throw "Detached qualification smoke lock identity is invalid"
    }
    $oldRunner = @(
        Get-CimInstance Win32_Process |
            Where-Object {
                $_.Name -ieq "python.exe" -and
                ([string]$_.CommandLine) -like "*laptop_smoke_runner.py*" -and
                ([string]$_.CommandLine) -like "*--run-id*$oldOwner*"
            }
    )
    if ($oldRunner.Count -gt 0) {
        throw "A detached qualification smoke runner is still active"
    }
    Remove-Item -LiteralPath $lockPath -Force
}
$lockStream = $null
$spawned = $false
try {
    $lockStream = [IO.File]::Open(
        $lockPath,
        [IO.FileMode]::CreateNew,
        [IO.FileAccess]::Write,
        [IO.FileShare]::None
    )
    $lockBytes = [Text.Encoding]::ASCII.GetBytes(
        "$env:IPEG_LAPTOP_SMOKE_RUN_ID|0"
    )
    $lockStream.Write($lockBytes, 0, $lockBytes.Length)
    $lockStream.Flush($true)
    $lockStream.Dispose()
    $lockStream = $null
} catch [IO.IOException] {
    throw "A detached qualification smoke launch is already locked"
}

try {
    $process = Start-Process -FilePath $python `
        -ArgumentList @(
            $runner, "--repo-root", $root, "--minutes", $SmokeMinutes,
            "--run-id", $env:IPEG_LAPTOP_SMOKE_RUN_ID
        ) `
        -WorkingDirectory $root `
        -WindowStyle Hidden `
        -PassThru
    $spawned = $true
    [IO.File]::WriteAllText(
        $lockPath,
        "$env:IPEG_LAPTOP_SMOKE_RUN_ID|$($process.Id)",
        [Text.Encoding]::ASCII
    )
    Write-Host "Detached non-accepting $SmokeMinutes-minute smoke runner started."
    Write-Host "PID=$($process.Id)"
    Write-Host "RUN_ID=$env:IPEG_LAPTOP_SMOKE_RUN_ID"
    Write-Host "STATE=$env:IPEG_STATE_PATH"
    Write-Host "Live remains disabled."
} finally {
    if ($null -ne $lockStream) { $lockStream.Dispose() }
    if (-not $spawned -and (Test-Path -LiteralPath $lockPath -PathType Leaf)) {
        $owner = Get-Content -LiteralPath $lockPath -Raw
        if ($owner.Trim().StartsWith("$env:IPEG_LAPTOP_SMOKE_RUN_ID|")) {
            Remove-Item -LiteralPath $lockPath -Force
        }
    }
}
