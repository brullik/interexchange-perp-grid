[CmdletBinding()]
param(
    [Parameter(Mandatory = $true)]
    [ValidateSet("verify", "onboard", "preflight", "canary", "pilot", "status", "stop")]
    [string]$Action,
    [string]$ProfilePath = "state/laptop-profile.clixml",
    [ValidateSet("CurrentUser", "LocalMachine")]
    [string]$ProfileScope = "CurrentUser"
)

$ErrorActionPreference = "Stop"
Set-StrictMode -Version Latest

$root = Split-Path -Parent $PSScriptRoot
Set-Location $root
$python = Join-Path $root ".venv/Scripts/python.exe"
$config = Join-Path $root "config/defaults.yaml"
$strategyProfile = Join-Path $root "config/AGGRESSIVE_FAST_LIVE_V2.yaml"
$runtimeManifest = Join-Path $root "state/laptop/native-runtime-manifest.json"
$history = Join-Path $root "data/reference-history"
$model = Join-Path $root "state/aggressive-historical-model.json"
$grid = Join-Path $root "state/aggressive-fast-live-grid.sqlite3"
$intent = Join-Path $root "state/aggressive-fast-live-intent.json"
$preflight = Join-Path $root "state/fast-live-preflight.json"
$canaryEvidence = Join-Path $root "state/fast-live-canary.json"
$pilotEvidence = Join-Path $root "state/fast-live-pilot.json"
$acceptance = Join-Path $root "state/laptop-fast-live-acceptance.json"
$supervisorPid = Join-Path $root "state/laptop-fast-live-supervisor.pid"

function Require-Python {
    if (-not (Test-Path -LiteralPath $python -PathType Leaf)) {
        throw "Exact project Python environment is missing: $python"
    }
}

function Load-LaptopEnvironment {
    if ($ProfileScope -ceq "LocalMachine") {
        . "$PSScriptRoot/laptop-load-s4u-env.ps1" -ProfilePath $ProfilePath
    } else {
        . "$PSScriptRoot/laptop-load-env.ps1" -ProfilePath $ProfilePath
    }
    if ($env:IPEG_MODE -cne "shadow" -or $env:IPEG_LIVE_ENABLED -cne "false") {
        throw "Fast-live must start in shadow mode with live=false"
    }
}

function Assert-TimeService {
    $service = Get-Service W32Time
    if ($service.Status -ne "Running" -or $service.StartType -ne "Automatic") {
        throw "W32Time must be Running with Automatic startup"
    }
}

function Convert-Secret([Security.SecureString]$Value) {
    $pointer = [Runtime.InteropServices.Marshal]::SecureStringToBSTR($Value)
    try {
        return [Runtime.InteropServices.Marshal]::PtrToStringBSTR($pointer)
    } finally {
        [Runtime.InteropServices.Marshal]::ZeroFreeBSTR($pointer)
    }
}

function Invoke-Checked([string[]]$Arguments, [string]$Failure) {
    & $python @Arguments
    if ($LASTEXITCODE -ne 0) { throw $Failure }
}

function Build-ExactRuntime {
    Invoke-Checked @(
        "-m", "interexchange_perp_grid.cli", "native-runtime-manifest",
        "--output", $runtimeManifest, "--repo-root", $root, "--config", $config
    ) "Exact native runtime manifest failed closed"
}

function Ensure-HistoryAndModel {
    $historyEnd = [DateTime]::UtcNow.Date
    $historyStart = $historyEnd.AddDays(-31)
    Invoke-Checked @(
        "-m", "interexchange_perp_grid.cli", "reference-history-proof",
        "--venue-a", "bybit", "--venue-b", "okx", "--base", "BTC",
        "--since", $historyStart.ToString("o"), "--end", $historyEnd.ToString("o"),
        "--limit", "1000", "--output-root", $history,
        "--profile", $strategyProfile, "--config", $config
    ) "Exact 31-day closed-minute history acquisition failed closed"
    Invoke-Checked @(
        "-m", "interexchange_perp_grid.cli", "aggressive-model-proof",
        "--venue-a", "bybit", "--venue-b", "okx", "--base", "BTC",
        "--start", $historyStart.ToString("o"), "--end", $historyEnd.ToString("o"),
        "--history-root", $history, "--artifact", $model,
        "--profile", $strategyProfile, "--config", $config
    ) "Fast-live historical model failed closed"
}

function Start-SafetySupervisor {
    if (Test-Path -LiteralPath $supervisorPid -PathType Leaf) {
        $existingId = [int](Get-Content -LiteralPath $supervisorPid -Raw)
        $existing = Get-Process -Id $existingId -ErrorAction SilentlyContinue
        if ($null -ne $existing) { return }
        Remove-Item -LiteralPath $supervisorPid -Force
    }
    $logRoot = Join-Path $root "state/laptop/fast-live"
    New-Item -ItemType Directory -Path $logRoot -Force | Out-Null
    $stamp = [DateTime]::UtcNow.ToString("yyyyMMddTHHmmssfffZ")
    $process = Start-Process -FilePath $python -ArgumentList @(
        "-m", "interexchange_perp_grid.cli", "run", "--config", $config
    ) -PassThru -WindowStyle Hidden `
        -RedirectStandardOutput (Join-Path $logRoot "supervisor-$stamp.stdout.log") `
        -RedirectStandardError (Join-Path $logRoot "supervisor-$stamp.stderr.log")
    $process.Id | Set-Content -LiteralPath $supervisorPid -Encoding ascii
    for ($attempt = 0; $attempt -lt 60; $attempt++) {
        if ($process.HasExited) { throw "Fast-live safety supervisor stopped during startup" }
        & $python -m interexchange_perp_grid.cli health --config $config *> $null
        if ($LASTEXITCODE -eq 0) { return }
        Start-Sleep -Seconds 1
    }
    throw "Fast-live safety supervisor did not become healthy"
}

function Get-FastStatus {
    $lines = @(& $python -m interexchange_perp_grid.cli fast-live-status --config $config)
    if ($LASTEXITCODE -ne 0) { throw "Fast-live durable status is unavailable" }
    $json = $lines | Where-Object {
        $_ -is [string] -and $_.TrimStart().StartsWith("{")
    } | Select-Object -Last 1
    if (-not $json) { throw "Fast-live status is not machine-readable" }
    return $json | ConvertFrom-Json
}

function Wait-StableFlat([int]$PriorCompleted, [int]$TimeoutSeconds) {
    $deadline = [DateTime]::UtcNow.AddSeconds($TimeoutSeconds)
    do {
        $snapshot = Get-FastStatus
        if (
            [int]$snapshot.active_action_count -eq 0 -and
            [int]$snapshot.completed_fast_live_round_trips -gt $PriorCompleted -and
            $snapshot.recovery_required -ne $true
        ) { return $snapshot }
        Start-Sleep -Seconds 2
    } while ([DateTime]::UtcNow -lt $deadline)
    throw "Fast-live action did not reach exchange-verified stable FLAT before its deadline"
}

function Invoke-Preflight {
    Build-ExactRuntime
    Ensure-HistoryAndModel
    $stage = if (Test-Path -LiteralPath $canaryEvidence -PathType Leaf) { "pilot_a" } else { "canary" }
    Invoke-Checked @(
        "-m", "interexchange_perp_grid.cli", "fast-live-stage-select",
        "--target", $stage, "--actor", "laptop-fast-live-wrapper", "--config", $config
    ) "Fast-live risk stage selection failed closed"
    $since = [DateTime]::UtcNow.AddMinutes(-4).ToString("o")
    Invoke-Checked @(
        "-m", "interexchange_perp_grid.cli", "reference-history-proof",
        "--venue-a", "bybit", "--venue-b", "okx", "--base", "BTC",
        "--since", $since, "--limit", "4", "--output-root", $history,
        "--profile", $strategyProfile, "--config", $config
    ) "Current reference minute refresh failed closed"
    Invoke-Checked @(
        "-m", "interexchange_perp_grid.cli", "aggressive-fast-live-intent-once",
        "--runtime-manifest", $runtimeManifest, "--output", $intent,
        "--model", $model, "--history-root", $history, "--grid", $grid,
        "--profile", $strategyProfile, "--stage", $stage,
        "--repo-root", $root, "--config", $config
    ) "No current profitable exact-bound fast-live intent is available"
    Invoke-Checked @(
        "-m", "interexchange_perp_grid.cli", "fast-live-preflight",
        "--intent", $intent, "--preflight", $preflight, "--model", $model,
        "--grid", $grid, "--profile", $strategyProfile,
        "--runtime-manifest", $runtimeManifest, "--stage", $stage,
        "--repo-root", $root, "--config", $config
    ) "FAST_LIVE_PREFLIGHT failed closed"
}

function Invoke-LiveEntry([string]$Stage) {
    if (-not (Test-Path -LiteralPath $preflight -PathType Leaf)) {
        throw "Current single-use FAST_LIVE_PREFLIGHT is missing"
    }
    $phrase = if ($Stage -ceq "canary") {
        "I_ACCEPT_LIVE_CANARY_RISK"
    } else {
        "I_ACCEPT_AGGRESSIVE_PILOT_A_RISK"
    }
    $consent = Read-Host "Type $phrase"
    if ($consent -cne $phrase) { throw "Exact owner live-money confirmation was not provided" }
    $unlock = Read-Host "Local live unlock secret" -AsSecureString
    if ($unlock.Length -eq 0) { throw "Local live unlock secret is required" }
    try {
        Start-SafetySupervisor
        Write-Host "In Telegram send /challenge, then /confirm_live <returned-token>."
        $telegramReady = Read-Host "After the bot confirms live_confirmed_until, type CONFIRMED"
        if ($telegramReady -cne "CONFIRMED") { throw "Telegram live challenge was not confirmed" }
        $before = Get-FastStatus
        $env:IPEG_LOCAL_UNLOCK_SECRET = Convert-Secret $unlock
        $env:IPEG_MODE = "live"
        $env:IPEG_LIVE_ENABLED = "true"
        $command = if ($Stage -ceq "canary") { "fast-live-canary" } else { "fast-live-pilot" }
        $output = @(& $python -m interexchange_perp_grid.cli $command `
            --confirmation $phrase --intent $intent --preflight $preflight `
            --model $model --grid $grid --profile $strategyProfile `
            --runtime-manifest $runtimeManifest --repo-root $root --config $config)
        $exitCode = $LASTEXITCODE
        $env:IPEG_MODE = "shadow"
        $env:IPEG_LIVE_ENABLED = "false"
        Remove-Item Env:IPEG_LOCAL_UNLOCK_SECRET -ErrorAction SilentlyContinue
        if ($exitCode -ne 0) { throw "Fast-live $Stage failed closed before durable ownership" }
        $timeout = if ($Stage -ceq "canary") { 900 } else { 86400 }
        $flat = Wait-StableFlat -PriorCompleted ([int]$before.completed_fast_live_round_trips) `
            -TimeoutSeconds $timeout
        $evidencePath = if ($Stage -ceq "canary") { $canaryEvidence } else { $pilotEvidence }
        [ordered]@{
            schema_version = 1
            stage = $Stage
            completed_at = [DateTime]::UtcNow.ToString("o")
            stable_flat = $true
            active_action_count = [int]$flat.active_action_count
            completed_fast_live_round_trips = [int]$flat.completed_fast_live_round_trips
            production_submit_scope = "owner_confirmed_$Stage"
        } | ConvertTo-Json | Set-Content -LiteralPath $evidencePath -Encoding utf8
        Write-Host "Fast-live $Stage completed with exchange-verified stable FLAT."
        if ($Stage -ceq "pilot") {
            Invoke-Checked @(
                "-m", "interexchange_perp_grid.cli", "fast-live-acceptance",
                "--runtime-manifest", $runtimeManifest, "--model", $model,
                "--grid", $grid, "--profile", $strategyProfile,
                "--preflight", $preflight, "--canary-evidence", $canaryEvidence,
                "--pilot-evidence", $pilotEvidence, "--output", $acceptance,
                "--repo-root", $root, "--config", $config
            ) "Fast-live laptop acceptance failed closed"
        }
    } finally {
        if ($null -ne $unlock) { $unlock.Dispose() }
    }
}

try {
    Require-Python
    switch ($Action) {
        "verify" {
            & "$PSScriptRoot/laptop-aggressive.ps1" -Mode verify
            if ($LASTEXITCODE -ne 0) { throw "Laptop verification failed" }
        }
        "onboard" {
            & "$PSScriptRoot/laptop-onboard.ps1" -OutputPath $ProfilePath
            if ($LASTEXITCODE -ne 0) { throw "Laptop onboarding failed closed" }
        }
        "preflight" {
            Load-LaptopEnvironment
            Assert-TimeService
            Invoke-Preflight
        }
        "canary" {
            Load-LaptopEnvironment
            Assert-TimeService
            Invoke-LiveEntry -Stage "canary"
        }
        "pilot" {
            Load-LaptopEnvironment
            Assert-TimeService
            if (-not (Test-Path -LiteralPath $canaryEvidence -PathType Leaf)) {
                throw "A genuine stable-FLAT canary is required before pilot"
            }
            Invoke-LiveEntry -Stage "pilot"
        }
        "status" {
            Load-LaptopEnvironment
            Get-FastStatus | ConvertTo-Json -Depth 20
        }
        "stop" {
            Load-LaptopEnvironment
            Invoke-Checked @(
                "-m", "interexchange_perp_grid.cli", "fast-live-runtime-control",
                "--action", "pause", "--config", $config
            ) "New-entry pause failed closed"
            $snapshot = Get-FastStatus
            if ([int]$snapshot.active_action_count -gt 0) {
                Write-Host "New entries paused; safety supervisor remains alive for recovery/FLAT."
                break
            }
            if (Test-Path -LiteralPath $supervisorPid -PathType Leaf) {
                $processId = [int](Get-Content -LiteralPath $supervisorPid -Raw)
                Stop-Process -Id $processId -ErrorAction SilentlyContinue
                Remove-Item -LiteralPath $supervisorPid -Force -ErrorAction SilentlyContinue
            }
            Write-Host "Fast-live is paused, FLAT, and its local supervisor is stopped."
        }
    }
} finally {
    $env:IPEG_MODE = "shadow"
    $env:IPEG_LIVE_ENABLED = "false"
    foreach ($name in @(
        "IPEG_LOCAL_UNLOCK_SECRET", "IPEG_TELEGRAM_BOT_TOKEN",
        "IPEG_BINANCEUSDM_API_KEY", "IPEG_BINANCEUSDM_API_SECRET",
        "IPEG_BINANCEUSDM_API_PASSWORD", "IPEG_BYBIT_API_KEY", "IPEG_BYBIT_API_SECRET",
        "IPEG_BYBIT_API_PASSWORD", "IPEG_OKX_API_KEY", "IPEG_OKX_API_SECRET",
        "IPEG_OKX_API_PASSWORD"
    )) {
        Remove-Item "Env:$name" -ErrorAction SilentlyContinue
    }
}
