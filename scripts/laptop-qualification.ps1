[CmdletBinding()]
param(
    [string]$ProfilePath = "state/laptop-profile.clixml",
    [ValidateSet("CurrentUser", "LocalMachine")]
    [string]$ProfileScope = "CurrentUser"
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

$laptopState = Join-Path $root "state/laptop"
$laptopTemp = Join-Path $laptopState "tmp"
New-Item -ItemType Directory -Path $laptopTemp -Force | Out-Null
$env:TEMP = $laptopTemp
$env:TMP = $laptopTemp

$timeService = Get-Service W32Time
if ($timeService.Status -ne "Running" -or $timeService.StartType -ne "Automatic") {
    throw "W32Time must be Running with Automatic startup"
}

$python = Join-Path $root ".venv/Scripts/python.exe"
if (-not (Test-Path -LiteralPath $python -PathType Leaf)) {
    throw "Exact Python 3.12 environment is missing: $python"
}

& $python -m pip check
if ($LASTEXITCODE -ne 0) { throw "pip check failed" }
& $python scripts/check_lock.py --lock requirements.lock --pyproject pyproject.toml
if ($LASTEXITCODE -ne 0) { throw "dependency lock check failed" }
& $python -m interexchange_perp_grid.cli native-runtime-manifest `
    --output $env:IPEG_NATIVE_RUNTIME_MANIFEST `
    --repo-root $root `
    --config "$root/config/defaults.yaml"
if ($LASTEXITCODE -ne 0) { throw "native runtime manifest failed" }

$manifest = Get-Content -LiteralPath $env:IPEG_NATIVE_RUNTIME_MANIFEST -Raw | ConvertFrom-Json
$env:IPEG_RELEASE_SHA = [string]$manifest.release_sha
$env:IPEG_CONTAINER_IMAGE_DIGEST = [string]$manifest.artifact_digest

# The locked laptop route is Bybit -> OKX. Binance USD-M is the first
# alternate in GOAL.md and may be geographically unavailable; it must not
# block qualification of the required private pair.
foreach ($venue in @("bybit", "okx")) {
    & $python -m interexchange_perp_grid.cli private-probe `
        --venue $venue `
        --authenticated
    if ($LASTEXITCODE -ne 0) { throw "$venue private capability probe failed" }
}

$replayProof = Join-Path $laptopState "replay-proof.json"
$runtimeEvidence = Join-Path $laptopState "qualification-runtime.json"
$qualificationEvidence = Join-Path $root "state/qualification.json"
& $python -m interexchange_perp_grid.cli replay-proof `
    --output $replayProof `
    --repo-root $root `
    --config "$root/config/defaults.yaml"
if ($LASTEXITCODE -ne 0) { throw "exact-head replay proof failed" }

if (-not ("LaptopSleepGuard" -as [type])) {
    Add-Type @"
using System;
using System.Runtime.InteropServices;
public static class LaptopSleepGuard {
    [DllImport("kernel32.dll", SetLastError = true)]
    public static extern uint SetThreadExecutionState(uint flags);
}
"@
}
$continuous = [uint32]0x80000000
$systemRequired = [uint32]0x00000001
$awayModeRequired = [uint32]0x00000040
$armed = [LaptopSleepGuard]::SetThreadExecutionState(
    $continuous -bor $systemRequired -bor $awayModeRequired
)
if ($armed -eq 0) { throw "Windows sleep prevention could not be armed" }

Write-Host "Starting exact native 24-hour shadow qualification."
Write-Host "Keep AC power and network connected. Closing this terminal interrupts qualification."
Write-Host "Live remains disabled; no real order can be submitted in this phase."
try {
    & $python -m interexchange_perp_grid.cli laptop-qualification-run `
        --maximum-hours 30 `
        --repo-root $root `
        --config "$root/config/defaults.yaml"
    if ($LASTEXITCODE -ne 0) { throw "native qualification service failed" }

    $statusOutput = @(
        & $python -m interexchange_perp_grid.cli qualification-epoch-status `
            --config "$root/config/defaults.yaml"
    )
    if ($LASTEXITCODE -ne 0) { throw "final qualification status is unavailable" }
    $statusJson = $statusOutput |
        Where-Object { $_ -is [string] -and $_.TrimStart().StartsWith("{") } |
        Select-Object -Last 1
    if (-not $statusJson) { throw "qualification status is not machine-readable" }
    $status = $statusJson | ConvertFrom-Json
    if ([string]$status.epoch_status -ne "FINALIZED") {
        throw "qualification epoch did not finalize"
    }

    & $python -m interexchange_perp_grid.cli qualification-runtime `
        --epoch-id ([string]$status.epoch_id) `
        --route "BTC:bybit>okx" `
        --container-image-digest $env:IPEG_CONTAINER_IMAGE_DIGEST `
        --replay-proof $replayProof `
        --output $runtimeEvidence `
        --repo-root $root `
        --config "$root/config/defaults.yaml"
    if ($LASTEXITCODE -ne 0) { throw "qualification runtime evidence failed" }

    & $python -m interexchange_perp_grid.cli qualify `
        --evidence $qualificationEvidence `
        --runtime-evidence $runtimeEvidence `
        --repo-root $root `
        --config "$root/config/defaults.yaml"
    if ($LASTEXITCODE -ne 0) { throw "final laptop qualification failed closed" }
    Write-Host "Native laptop qualification accepted: $qualificationEvidence"
} finally {
    [void][LaptopSleepGuard]::SetThreadExecutionState($continuous)
}
