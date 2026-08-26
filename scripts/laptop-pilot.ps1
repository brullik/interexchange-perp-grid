[CmdletBinding()]
param(
    [string]$ProfilePath = "state/laptop-profile.clixml",
    [string]$QualificationPath = "state/qualification.json",
    [ValidateSet("CurrentUser", "LocalMachine")]
    [string]$ProfileScope = "CurrentUser",
    [switch]$Aggressive
)

$ErrorActionPreference = "Stop"
Set-StrictMode -Version Latest

throw "Legacy qualification-based canary is disabled; use scripts/laptop-fast-live.ps1 -Action canary."

$root = Split-Path -Parent $PSScriptRoot
Set-Location $root
if ($ProfileScope -ceq "LocalMachine") {
    . "$PSScriptRoot/laptop-load-s4u-env.ps1" -ProfilePath $ProfilePath
} else {
    . "$PSScriptRoot/laptop-load-env.ps1" -ProfilePath $ProfilePath
}

$timeService = Get-Service W32Time
if ($timeService.Status -ne "Running" -or $timeService.StartType -ne "Automatic") {
    throw "W32Time must be Running with Automatic startup"
}

$python = Join-Path $root ".venv/Scripts/python.exe"
if (-not (Test-Path -LiteralPath $python -PathType Leaf)) {
    throw "Exact Python 3.12 environment is missing: $python"
}
$qualification = [System.IO.Path]::GetFullPath((Join-Path $root $QualificationPath))
if (-not (Test-Path -LiteralPath $qualification -PathType Leaf)) {
    throw "Final accepted laptop qualification is missing: $qualification"
}
$qualificationPayload = Get-Content -LiteralPath $qualification -Raw | ConvertFrom-Json
$qualificationDuration = [int]$qualificationPayload.policy.minimum_duration_seconds
$ownerException12h = $qualificationDuration -eq 43200
if ($qualificationDuration -notin @(43200, 86400)) {
    throw "Laptop qualification policy is neither the standard nor approved exception"
}
$ownerExceptionConfirmation = $null
if ($ownerException12h) {
    $ownerExceptionConfirmation = Read-Host (
        "For this one-time laptop-only 12-hour qualification, type " +
        "I_ACCEPT_LAPTOP_12H_QUALIFICATION_EXCEPTION"
    )
    if ($ownerExceptionConfirmation -cne "I_ACCEPT_LAPTOP_12H_QUALIFICATION_EXCEPTION") {
        throw "The laptop-only 12-hour qualification exception was not confirmed"
    }
}

& $python -m interexchange_perp_grid.cli native-runtime-manifest `
    --output $env:IPEG_NATIVE_RUNTIME_MANIFEST `
    --repo-root $root `
    --config "$root/config/defaults.yaml"
if ($LASTEXITCODE -ne 0) { throw "native runtime manifest verification failed" }
$manifest = Get-Content -LiteralPath $env:IPEG_NATIVE_RUNTIME_MANIFEST -Raw | ConvertFrom-Json
$env:IPEG_RELEASE_SHA = [string]$manifest.release_sha
$env:IPEG_CONTAINER_IMAGE_DIGEST = [string]$manifest.artifact_digest

$consent = Read-Host "For this one minimum-notional real canary, type I_ACCEPT_LIVE_CANARY_RISK"
if ($consent -cne "I_ACCEPT_LIVE_CANARY_RISK") {
    throw "Short-lived live-canary consent was not provided"
}
$unlock = Read-Host "Local live unlock secret" -AsSecureString
if ($unlock.Length -eq 0) { throw "Local live unlock secret is required" }

$runId = [DateTime]::UtcNow.ToString("yyyyMMddTHHmmssZ")
$outputDirectory = Join-Path $root "artifacts/runtime/laptop-pilot/$runId"
New-Item -ItemType Directory -Path $outputDirectory -Force | Out-Null
$reportPath = Join-Path $outputDirectory "report.json"
$failurePath = Join-Path $outputDirectory "wrapper-failure.json"
$aggressiveIntentPath = Join-Path $outputDirectory "aggressive-live-intent.json"
$aggressiveCanaryEvidencePath = Join-Path $root "state/aggressive-canary-stage.json"
$aggressiveBindingPath = Join-Path $root "state/aggressive-qualification.json"
$aggressiveRuntimePath = Join-Path $root "state/laptop/native-runtime-manifest.json"
$aggressiveModelPath = Join-Path $root "state/aggressive-historical-model.json"
$aggressiveHistoryPath = Join-Path $root "data/reference-history"
$aggressiveQualificationGridPath = Join-Path $root "state/aggressive-grid.sqlite3"
$aggressiveLiveGridPath = Join-Path $outputDirectory "aggressive-live-grid.sqlite3"
$aggressiveProfilePath = Join-Path $root "config/AGGRESSIVE_SYMBIOSIS_V1.yaml"

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

$serviceProcess = $null
$entryAttempted = $false
try {
    if ($ownerException12h) {
        $env:IPEG_LAPTOP_12H_OWNER_EXCEPTION = $ownerExceptionConfirmation
    }
    $riskOutput = @(
        & $python -m interexchange_perp_grid.cli risk-stage-status `
            --config "$root/config/defaults.yaml"
    )
    if ($LASTEXITCODE -ne 0) { throw "risk stage status is unavailable" }
    $riskJson = $riskOutput |
        Where-Object { $_ -is [string] -and $_.TrimStart().StartsWith("{") } |
        Select-Object -Last 1
    if (-not $riskJson) { throw "risk stage status is not machine-readable" }
    $risk = $riskJson | ConvertFrom-Json
    if ([string]$risk.state.stage -eq "shadow") {
        if ($ownerException12h) {
            & $python -m interexchange_perp_grid.cli risk-stage-promote `
                --expected-current shadow `
                --target canary `
                --actor laptop-owner `
                --confirmation PROMOTE:canary `
                --qualification $qualification `
                --container-image-digest $env:IPEG_CONTAINER_IMAGE_DIGEST `
                --laptop-owner-exception-12h `
                --repo-root $root `
                --config "$root/config/defaults.yaml"
        } else {
            & $python -m interexchange_perp_grid.cli risk-stage-promote `
                --expected-current shadow `
                --target canary `
                --actor laptop-owner `
                --confirmation PROMOTE:canary `
                --qualification $qualification `
                --container-image-digest $env:IPEG_CONTAINER_IMAGE_DIGEST `
                --repo-root $root `
                --config "$root/config/defaults.yaml"
        }
        if ($LASTEXITCODE -ne 0) { throw "risk stage promotion to canary failed" }
    } elseif ([string]$risk.state.stage -ne "canary") {
        throw "first laptop pilot requires the locked canary risk stage"
    }

    $serviceStdout = Join-Path $outputDirectory "service.stdout.log"
    $serviceStderr = Join-Path $outputDirectory "service.stderr.log"
    $serviceReceipt = Join-Path $outputDirectory "service-receipt.json"
    $serviceArguments = @(
        "-m", "interexchange_perp_grid.cli", "run-for",
        "--duration-seconds", "33000",
        "--receipt", $serviceReceipt,
        "--config", "$root/config/defaults.yaml"
    )
    $env:IPEG_MODE = "shadow"
    $env:IPEG_LIVE_ENABLED = "false"
    $serviceStartedAt = [DateTime]::UtcNow
    $serviceProcess = Start-Process `
        -FilePath $python `
        -ArgumentList $serviceArguments `
        -PassThru `
        -WindowStyle Hidden `
        -RedirectStandardOutput $serviceStdout `
        -RedirectStandardError $serviceStderr
    $healthy = $false
    for ($attempt = 0; $attempt -lt 60; $attempt++) {
        if ($serviceProcess.HasExited) { throw "laptop safety supervisor stopped during startup" }
        & $python -m interexchange_perp_grid.cli health `
            --config "$root/config/defaults.yaml" *> $null
        if ($LASTEXITCODE -eq 0) {
            $healthy = $true
            break
        }
        Start-Sleep -Seconds 1
    }
    if (-not $healthy) { throw "laptop safety supervisor did not become healthy" }

    Write-Host "In Telegram send /challenge, then /confirm_live <returned-token>."
    $telegramReady = Read-Host "After the bot confirms live_confirmed_until, type CONFIRMED"
    if ($telegramReady -cne "CONFIRMED") { throw "Telegram live challenge was not confirmed" }
    if (([DateTime]::UtcNow - $serviceStartedAt).TotalSeconds -gt 600) {
        throw "Canary was not armed within the bounded ten-minute preparation window"
    }

    $startedAt = [DateTime]::UtcNow
    if ($Aggressive) {
        $referenceSince = [DateTime]::UtcNow.AddMinutes(-4).ToString("o")
        & $python -m interexchange_perp_grid.cli reference-history-proof `
            --venue-a bybit --venue-b okx --base BTC --since $referenceSince --limit 4 `
            --output-root $aggressiveHistoryPath --profile $aggressiveProfilePath `
            --config "$root/config/defaults.yaml"
        if ($LASTEXITCODE -ne 0) { throw "aggressive reference refresh failed closed" }
        & $python -m interexchange_perp_grid.cli aggressive-live-intent-once `
            --binding $aggressiveBindingPath `
            --qualification $qualification `
            --runtime-manifest $aggressiveRuntimePath `
            --model $aggressiveModelPath `
            --history-root $aggressiveHistoryPath `
            --grid $aggressiveLiveGridPath `
            --qualification-grid $aggressiveQualificationGridPath `
            --profile $aggressiveProfilePath `
            --output $aggressiveIntentPath `
            --config "$root/config/defaults.yaml"
        if ($LASTEXITCODE -ne 0) { throw "aggressive canary intent failed closed" }
    }
    $env:IPEG_LOCAL_UNLOCK_SECRET = Convert-Secret $unlock
    $env:IPEG_MODE = "live"
    $env:IPEG_LIVE_ENABLED = "true"
    Write-Host "Queuing at most one minimum-notional paired canary through all runtime gates."
    $entryAttempted = $true
    if ($Aggressive) {
        $canaryOutput = @(
            & $python -m interexchange_perp_grid.cli canary-run `
                --confirmation I_ACCEPT_LIVE_CANARY_RISK `
                --qualification $qualification `
                --repo-root $root `
                --aggressive-intent $aggressiveIntentPath `
                --aggressive-binding $aggressiveBindingPath `
                --runtime-manifest $aggressiveRuntimePath `
                --aggressive-model $aggressiveModelPath `
                --aggressive-grid $aggressiveQualificationGridPath `
                --aggressive-profile $aggressiveProfilePath `
                --config "$root/config/defaults.yaml"
        )
    } else {
        $canaryOutput = @(
            & $python -m interexchange_perp_grid.cli canary-run `
                --confirmation I_ACCEPT_LIVE_CANARY_RISK `
                --qualification $qualification `
                --repo-root $root `
                --config "$root/config/defaults.yaml"
        )
    }
    $canaryExit = $LASTEXITCODE
    $canaryOutput | Set-Content -LiteralPath (Join-Path $outputDirectory "canary.log")
    $canaryJson = $canaryOutput |
        Where-Object { $_ -is [string] -and $_.TrimStart().StartsWith("{") } |
        Select-Object -Last 1
    if (-not $canaryJson) { throw "canary did not return machine-readable evidence" }
    $canary = $canaryJson | ConvertFrom-Json
    if ($canaryExit -ne 0 -or $canary.success -ne $true) {
        throw "minimum-notional canary failed closed"
    }

    $env:IPEG_MODE = "shadow"
    $env:IPEG_LIVE_ENABLED = "false"
    Remove-Item Env:IPEG_LOCAL_UNLOCK_SECRET -ErrorAction SilentlyContinue
    Remove-Item Env:IPEG_LAPTOP_12H_OWNER_EXCEPTION -ErrorAction SilentlyContinue
    Write-Host "Canary is queued. The sole safety supervisor owns submission and recovery."
    Write-Host "The command will finish only after the bounded service interval and evidence audit."
    $serviceProcess.WaitForExit()
    if ($serviceProcess.ExitCode -ne 0) { throw "9-hour safety-supervisor interval failed" }
    $endedAt = [DateTime]::UtcNow

    & $python -m interexchange_perp_grid.cli laptop-pilot-report `
        --started-at $startedAt.ToString("o") `
        --ended-at $endedAt.ToString("o") `
        --qualification $qualification `
        --service-receipt $serviceReceipt `
        --output $reportPath `
        --repo-root $root `
        --config "$root/config/defaults.yaml"
    if ($LASTEXITCODE -ne 0) { throw "laptop pilot evidence failed closed" }
    if ($Aggressive) {
        & $python -m interexchange_perp_grid.cli aggressive-laptop-stage-report `
            --stage canary `
            --binding $aggressiveBindingPath `
            --started-at $startedAt.ToString("o") `
            --ended-at $endedAt.ToString("o") `
            --post-flat-service-seconds 0 `
            --output $aggressiveCanaryEvidencePath `
            --config "$root/config/defaults.yaml"
        if ($LASTEXITCODE -ne 0) { throw "aggressive canary evidence failed closed" }
    }
} catch {
    [ordered]@{
        schema_version = 1
        status = "FAIL"
        observed_at = [DateTime]::UtcNow.ToString("o")
        failure = "$($_.Exception.GetType().Name): $($_.Exception.Message)"
    } | ConvertTo-Json | Set-Content -LiteralPath $failurePath
    throw
} finally {
    $env:IPEG_MODE = "shadow"
    $env:IPEG_LIVE_ENABLED = "false"
    Remove-Item Env:IPEG_LOCAL_UNLOCK_SECRET -ErrorAction SilentlyContinue
    Remove-Item Env:IPEG_LAPTOP_12H_OWNER_EXCEPTION -ErrorAction SilentlyContinue
    if (
        $serviceProcess -and
        -not $serviceProcess.HasExited -and
        -not $entryAttempted
    ) {
        Stop-Process -Id $serviceProcess.Id -Force -ErrorAction SilentlyContinue
    }
    [void][LaptopSleepGuard]::SetThreadExecutionState($continuous)
    Write-Host "Laptop pilot report: $reportPath"
}
