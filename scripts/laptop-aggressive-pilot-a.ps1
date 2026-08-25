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
if ($env:IPEG_MODE -cne "shadow" -or $env:IPEG_LIVE_ENABLED -cne "false") {
    throw "Aggressive pilot_a must start in shadow mode with live disabled"
}

$python = Join-Path $root ".venv/Scripts/python.exe"
$config = Join-Path $root "config/defaults.yaml"
$strategyProfile = Join-Path $root "config/AGGRESSIVE_SYMBIOSIS_V1.yaml"
$qualification = Join-Path $root "state/qualification.json"
$binding = Join-Path $root "state/aggressive-qualification.json"
$canaryEvidence = Join-Path $root "state/aggressive-canary-stage.json"
$runtimeManifest = Join-Path $root "state/laptop/native-runtime-manifest.json"
$model = Join-Path $root "state/aggressive-historical-model.json"
$history = Join-Path $root "data/reference-history"
$qualificationGrid = Join-Path $root "state/aggressive-grid.sqlite3"
if (-not (Test-Path -LiteralPath $python -PathType Leaf)) {
    throw "Exact Python 3.12 environment is missing"
}
foreach ($required in @($qualification, $binding, $canaryEvidence, $runtimeManifest, $model)) {
    if (-not (Test-Path -LiteralPath $required -PathType Leaf)) {
        throw "Aggressive pilot_a prerequisite is missing: $required"
    }
}
$qualificationPayload = Get-Content -LiteralPath $qualification -Raw | ConvertFrom-Json
if ([int]$qualificationPayload.policy.minimum_duration_seconds -ne 86400) {
    throw "pilot_a requires the standard 24-hour qualification; the one-time 12-hour exception authorizes only one canary"
}
$timeService = Get-Service W32Time
if ($timeService.Status -ne "Running" -or $timeService.StartType -ne "Automatic") {
    throw "W32Time must be Running with Automatic startup"
}

$confirmation = Read-Host "For one-route five-level pilot_a with hard risk <=5 USDT, type I_ACCEPT_AGGRESSIVE_PILOT_A_RISK"
if ($confirmation -cne "I_ACCEPT_AGGRESSIVE_PILOT_A_RISK") {
    throw "Aggressive pilot_a owner confirmation was not provided"
}
$unlock = Read-Host "Local live unlock secret" -AsSecureString
if ($unlock.Length -eq 0) { throw "Local live unlock secret is required" }

& $python -m interexchange_perp_grid.cli aggressive-laptop-promote-pilot-a `
    --canary-evidence $canaryEvidence --binding $binding `
    --confirmation I_ACCEPT_AGGRESSIVE_PILOT_A_RISK --actor laptop-owner --config $config
if ($LASTEXITCODE -ne 0) { throw "aggressive pilot_a promotion failed closed" }

$runId = [DateTime]::UtcNow.ToString("yyyyMMddTHHmmssZ")
$outputDirectory = Join-Path $root "artifacts/runtime/aggressive-pilot-a/$runId"
New-Item -ItemType Directory -Path $outputDirectory -Force | Out-Null
$intentPath = Join-Path $outputDirectory "live-intent.json"
$liveGrid = Join-Path $outputDirectory "live-grid.sqlite3"
$serviceStdout = Join-Path $outputDirectory "service.stdout.log"
$serviceStderr = Join-Path $outputDirectory "service.stderr.log"
$postFlatReceipt = Join-Path $outputDirectory "post-flat-service.json"
$pilotEvidence = Join-Path $root "state/aggressive-pilot-a-stage.json"
$failurePath = Join-Path $outputDirectory "failure.json"
$serviceProcess = $null
$entryAttempted = $false
$pilotStartedAt = [DateTime]::UtcNow
$pilotDeadline = $pilotStartedAt.AddHours(24)

try {
    $serviceProcess = Start-Process -FilePath $python -ArgumentList @(
        "-m", "interexchange_perp_grid.cli", "run", "--config", $config
    ) -PassThru -WindowStyle Hidden `
        -RedirectStandardOutput $serviceStdout -RedirectStandardError $serviceStderr
    $healthy = $false
    for ($attempt = 0; $attempt -lt 60; $attempt++) {
        if ($serviceProcess.HasExited) { throw "pilot safety supervisor stopped during startup" }
        & $python -m interexchange_perp_grid.cli health --config $config *> $null
        if ($LASTEXITCODE -eq 0) { $healthy = $true; break }
        Start-Sleep -Seconds 1
    }
    if (-not $healthy) { throw "pilot safety supervisor did not become healthy" }

    Write-Host "In Telegram send /challenge, then /confirm_live <returned-token>."
    $telegramReady = Read-Host "After the bot confirms live_confirmed_until, type CONFIRMED"
    if ($telegramReady -cne "CONFIRMED") { throw "Telegram live challenge was not confirmed" }

    $env:IPEG_LOCAL_UNLOCK_SECRET = Convert-Secret $unlock
    while ([DateTime]::UtcNow -lt $pilotDeadline) {
        $since = [DateTime]::UtcNow.AddMinutes(-4).ToString("o")
        & $python -m interexchange_perp_grid.cli reference-history-proof `
            --venue-a bybit --venue-b okx --base BTC --since $since --limit 4 `
            --output-root $history --profile $strategyProfile --config $config *> $null
        if ($LASTEXITCODE -ne 0) { throw "pilot reference refresh failed closed" }

        $intentOutput = @(
            & $python -m interexchange_perp_grid.cli aggressive-live-intent-once `
                --binding $binding --qualification $qualification `
                --runtime-manifest $runtimeManifest --model $model --history-root $history `
                --grid $liveGrid --qualification-grid $qualificationGrid `
                --profile $strategyProfile --output $intentPath --stage pilot_a --config $config
        )
        $intentExit = $LASTEXITCODE
        $intentOutput | Add-Content -LiteralPath (Join-Path $outputDirectory "intent.log")
        if ($intentExit -eq 0) {
            $entryAttempted = $true
            $env:IPEG_MODE = "live"
            $env:IPEG_LIVE_ENABLED = "true"
            try {
                $queuedOutput = @(
                    & $python -m interexchange_perp_grid.cli canary-run `
                        --confirmation I_ACCEPT_AGGRESSIVE_PILOT_A_RISK `
                        --qualification $qualification --repo-root $root `
                        --aggressive-intent $intentPath --aggressive-binding $binding `
                        --aggressive-stage pilot_a --runtime-manifest $runtimeManifest `
                        --aggressive-model $model --aggressive-grid $qualificationGrid `
                        --aggressive-profile $strategyProfile --config $config
                )
                $queuedExit = $LASTEXITCODE
                $queuedOutput | Add-Content -LiteralPath (Join-Path $outputDirectory "queued.log")
                if ($queuedExit -ne 0) { throw "pilot tranche failed closed before journal ownership" }
                $queuedJson = $queuedOutput |
                    Where-Object { $_ -is [string] -and $_.TrimStart().StartsWith("{") } |
                    Select-Object -Last 1
                if (-not $queuedJson -or ($queuedJson | ConvertFrom-Json).success -ne $true) {
                    throw "pilot tranche was not durably queued"
                }
            } finally {
                $env:IPEG_MODE = "shadow"
                $env:IPEG_LIVE_ENABLED = "false"
            }
        } elseif ($intentExit -ne 3) {
            throw "pilot intent evaluation failed unexpectedly"
        }

        $progressOutput = @(
            & $python -m interexchange_perp_grid.cli aggressive-laptop-stage-progress `
                --stage pilot_a --started-at $pilotStartedAt.ToString("o") `
                --binding $binding --config $config
        )
        if ($LASTEXITCODE -ne 0) { throw "pilot journal progress is unavailable" }
        $progressJson = $progressOutput |
            Where-Object { $_ -is [string] -and $_.TrimStart().StartsWith("{") } |
            Select-Object -Last 1
        if (-not $progressJson) { throw "pilot journal progress is not machine-readable" }
        $progress = $progressJson | ConvertFrom-Json
        $levels = @($progress.completed_level_indices | ForEach-Object { [int]$_ })
        if (
            $progress.stable_flat -eq $true -and
            [int]$progress.active_action_count -eq 0 -and
            ($levels -join ",") -ceq "1,2,3,4,5"
        ) { break }
        Start-Sleep -Seconds 60
    }
    if ([DateTime]::UtcNow -ge $pilotDeadline) {
        throw "pilot_a did not complete all five levels within the hard 24-hour holding window"
    }

    if ($serviceProcess -and -not $serviceProcess.HasExited) {
        Stop-Process -Id $serviceProcess.Id
        $serviceProcess.WaitForExit()
    }
    $serviceProcess = Start-Process -FilePath $python -ArgumentList @(
        "-m", "interexchange_perp_grid.cli", "run-for",
        "--duration-seconds", "28800", "--receipt", $postFlatReceipt,
        "--config", $config
    ) -PassThru -WindowStyle Hidden `
        -RedirectStandardOutput (Join-Path $outputDirectory "post-flat.stdout.log") `
        -RedirectStandardError (Join-Path $outputDirectory "post-flat.stderr.log")
    $serviceProcess.WaitForExit()
    if ($serviceProcess.ExitCode -ne 0) { throw "eight-hour post-FLAT service failed" }
    $pilotEndedAt = [DateTime]::UtcNow
    & $python -m interexchange_perp_grid.cli aggressive-laptop-stage-report `
        --stage pilot_a --started-at $pilotStartedAt.ToString("o") `
        --ended-at $pilotEndedAt.ToString("o") --post-flat-service-seconds 28800 `
        --binding $binding --service-receipt $postFlatReceipt `
        --output $pilotEvidence --config $config
    if ($LASTEXITCODE -ne 0) { throw "aggressive pilot_a evidence failed closed" }
    Write-Host "Aggressive pilot_a evidence: $pilotEvidence"
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
    if ($serviceProcess -and -not $serviceProcess.HasExited -and -not $entryAttempted) {
        Stop-Process -Id $serviceProcess.Id -ErrorAction SilentlyContinue
    }
}
