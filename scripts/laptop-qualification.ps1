[CmdletBinding()]
param(
    [string]$ProfilePath = "state/laptop-profile.clixml",
    [ValidateSet("CurrentUser", "LocalMachine")]
    [string]$ProfileScope = "CurrentUser",
    [switch]$OwnerException12h,
    [switch]$Smoke5m,
    [switch]$Smoke30m
)

$ErrorActionPreference = "Stop"
Set-StrictMode -Version Latest

throw "Legacy qualification is non-authoritative in Aggressive Fast Live V2; use scripts/laptop-fast-live.ps1."

$root = Split-Path -Parent $PSScriptRoot
Set-Location $root
if ($ProfileScope -ceq "LocalMachine") {
    . "$PSScriptRoot/laptop-load-s4u-env.ps1" -ProfilePath $ProfilePath
} else {
    . "$PSScriptRoot/laptop-load-env.ps1" -ProfilePath $ProfilePath
}

# Qualification is a non-interactive shadow workload. A Telegram poller adds no
# evidence, can conflict with an owner-facing instance, and may expose a bot
# token through third-party HTTP request logging. Keep the encrypted profile
# intact while removing Telegram from this process before any Python command.
. "$PSScriptRoot/laptop-disable-shadow-telegram.ps1"

if ($Smoke5m -and $Smoke30m) { throw "Choose exactly one smoke duration" }
$smokeMinutes = if ($Smoke5m) { 5 } elseif ($Smoke30m) { 30 } else { 0 }
$qualificationLockName = if ($smokeMinutes -gt 0) {
    "qualification-smoke.lock"
} else {
    "qualification.lock"
}
$qualificationLockPath = Join-Path $root "state/laptop/$qualificationLockName"
$qualificationLockOwner = [Guid]::NewGuid().ToString("N")
$qualificationLockStream = $null
try {
    New-Item -ItemType Directory -Path (Split-Path -Parent $qualificationLockPath) `
        -Force | Out-Null
    $qualificationLockStream = [IO.File]::Open(
        $qualificationLockPath,
        [IO.FileMode]::CreateNew,
        [IO.FileAccess]::Write,
        [IO.FileShare]::None
    )
    $qualificationLockBytes = [Text.Encoding]::ASCII.GetBytes(
        "$qualificationLockOwner|$PID"
    )
    $qualificationLockStream.Write(
        $qualificationLockBytes,
        0,
        $qualificationLockBytes.Length
    )
    $qualificationLockStream.Flush($true)
} catch [IO.IOException] {
    throw "Another laptop qualification runner owns the shared qualification state"
} finally {
    if ($null -ne $qualificationLockStream) { $qualificationLockStream.Dispose() }
}
trap {
    if (Test-Path -LiteralPath $qualificationLockPath -PathType Leaf) {
        $owner = Get-Content -LiteralPath $qualificationLockPath -Raw
        if ($owner.Trim().StartsWith("$qualificationLockOwner|")) {
            Remove-Item -LiteralPath $qualificationLockPath -Force
        }
    }
    throw $_
}

if ($smokeMinutes -gt 0) {
    $env:IPEG_LAPTOP_SMOKE_RUN_ID = [Guid]::NewGuid().ToString("N")
    $env:IPEG_STATE_PATH = [IO.Path]::GetFullPath(
        (Join-Path $root "state/laptop/smoke/$env:IPEG_LAPTOP_SMOKE_RUN_ID/ipeg.sqlite3")
    )
    $env:IPEG_PARQUET_DIR = [IO.Path]::GetFullPath(
        (Join-Path $root "data/laptop/smoke/$env:IPEG_LAPTOP_SMOKE_RUN_ID/market")
    )
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

if ($smokeMinutes -eq 0) {
    $branch = (& git branch --show-current).Trim()
    $head = (& git rev-parse HEAD).Trim().ToLowerInvariant()
    $originMain = (& git rev-parse refs/remotes/origin/main).Trim().ToLowerInvariant()
    $dirty = @(& git status --porcelain --untracked-files=no)
    if (
        $LASTEXITCODE -ne 0 -or $branch -cne "main" -or
        $head -cne $originMain -or $head -cne $env:IPEG_RELEASE_SHA
    ) {
        throw "qualification requires clean exact local main matching fetched origin/main"
    }
    if ($dirty.Count -gt 0) { throw "qualification requires a clean tracked checkout" }
}

$ownerExceptionConfirmation = $null
if ($smokeMinutes -gt 0) {
    Write-Host "Preparing isolated non-qualifying $smokeMinutes-minute laptop rehearsal."
} elseif ($OwnerException12h) {
    $ownerExceptionConfirmation = [Environment]::GetEnvironmentVariable(
        "IPEG_LAPTOP_12H_OWNER_EXCEPTION",
        "Process"
    )
    if ([string]::IsNullOrWhiteSpace($ownerExceptionConfirmation)) {
        $ownerExceptionConfirmation = Read-Host (
            "For the one-time laptop-only 12-hour qualification, type " +
            "I_ACCEPT_LAPTOP_12H_QUALIFICATION_EXCEPTION"
        )
    }
    if ($ownerExceptionConfirmation -cne "I_ACCEPT_LAPTOP_12H_QUALIFICATION_EXCEPTION") {
        throw "The laptop-only 12-hour qualification exception was not confirmed"
    }
}

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

if ($OwnerException12h) {
    Write-Host "Starting exact one-time native 12-hour laptop shadow qualification."
    Write-Host "The standard and future VPS qualification policy remains 24 hours."
} else {
    Write-Host "Starting exact native 24-hour shadow qualification."
}
Write-Host "Keep AC power and network connected. Closing this terminal interrupts qualification."
Write-Host "Live remains disabled; no real order can be submitted in this phase."
try {
    if ($smokeMinutes -gt 0) {
        Write-Host "Starting isolated non-qualifying $smokeMinutes-minute laptop rehearsal."
        & $python -m interexchange_perp_grid.cli laptop-qualification-smoke-run `
            --minutes $smokeMinutes `
            --repo-root $root `
            --config "$root/config/defaults.yaml"
        if ($LASTEXITCODE -ne 0) { throw "$smokeMinutes-minute qualification rehearsal failed" }
        Write-Host "$smokeMinutes-minute rehearsal passed; it cannot authorize canary or live."
        return
    } elseif ($OwnerException12h) {
        $env:IPEG_LAPTOP_12H_OWNER_EXCEPTION = $ownerExceptionConfirmation
        & $python -m interexchange_perp_grid.cli laptop-qualification-run `
            --maximum-hours 18 `
            --laptop-owner-exception-12h `
            --repo-root $root `
            --config "$root/config/defaults.yaml"
    } else {
        & $python -m interexchange_perp_grid.cli laptop-qualification-run `
            --maximum-hours 30 `
            --repo-root $root `
            --config "$root/config/defaults.yaml"
    }
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

    if ($OwnerException12h) {
        & $python -m interexchange_perp_grid.cli qualify `
            --evidence $qualificationEvidence `
            --runtime-evidence $runtimeEvidence `
            --laptop-owner-exception-12h `
            --repo-root $root `
            --config "$root/config/defaults.yaml"
    } else {
        & $python -m interexchange_perp_grid.cli qualify `
            --evidence $qualificationEvidence `
            --runtime-evidence $runtimeEvidence `
            --repo-root $root `
            --config "$root/config/defaults.yaml"
    }
    if ($LASTEXITCODE -ne 0) { throw "final laptop qualification failed closed" }
    Write-Host "Native laptop qualification accepted: $qualificationEvidence"
} finally {
    Remove-Item Env:IPEG_LAPTOP_12H_OWNER_EXCEPTION -ErrorAction SilentlyContinue
    Remove-Item Env:IPEG_LAPTOP_SMOKE_RUN_ID -ErrorAction SilentlyContinue
    if (Test-Path -LiteralPath $qualificationLockPath -PathType Leaf) {
        $owner = Get-Content -LiteralPath $qualificationLockPath -Raw
        if ($owner.Trim().StartsWith("$qualificationLockOwner|")) {
            Remove-Item -LiteralPath $qualificationLockPath -Force
        }
    }
    [void][LaptopSleepGuard]::SetThreadExecutionState($continuous)
}
