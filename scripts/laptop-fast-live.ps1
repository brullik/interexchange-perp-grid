[CmdletBinding()]
param(
    [Parameter(Mandatory = $true)]
    [ValidateSet("verify", "onboard", "preflight", "canary", "pilot", "status", "stop")]
    [string]$Action,
    [string]$ProfilePath = "state/laptop-profile.clixml",
    [ValidateSet("CurrentUser", "LocalMachine")]
    [string]$ProfileScope = "CurrentUser",
    [switch]$SupervisorReadinessSelfTest
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
$independentReview = Join-Path $root "state/fast-live-independent-review.json"
$acceptance = Join-Path $root "state/laptop-fast-live-acceptance.json"
$supervisorPid = Join-Path $root "state/laptop-fast-live-supervisor.pid"
$supervisorHandshake = Join-Path $root "state/laptop-fast-live-supervisor-runtime.json"
$currentUserProfilePath = if ($ProfileScope -ceq "LocalMachine") {
    "state/laptop-profile.clixml"
} else {
    $ProfilePath
}
$effectiveProfilePath = if (
    $ProfileScope -ceq "LocalMachine" -and
    $ProfilePath -ceq "state/laptop-profile.clixml"
) {
    "state/laptop-profile-s4u.json"
} else {
    $ProfilePath
}

function Require-Python {
    if (-not (Test-Path -LiteralPath $python -PathType Leaf)) {
        throw "Exact project Python environment is missing: $python"
    }
}

function Load-LaptopEnvironment {
    if ($ProfileScope -ceq "LocalMachine") {
        . "$PSScriptRoot/laptop-load-s4u-env.ps1" -ProfilePath $effectiveProfilePath
    } else {
        . "$PSScriptRoot/laptop-load-env.ps1" -ProfilePath $effectiveProfilePath
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

function Test-SupervisorReadinessEvidence($Health, $Handshake) {
    try {
        $readyAt = [DateTimeOffset]::Parse([string]$Handshake.ready_at)
        $serviceHeartbeat = [DateTimeOffset]::Parse([string]$Health.heartbeat_at)
        $supervisorHeartbeat = [DateTimeOffset]::Parse([string]$Health.supervisor_heartbeat_at)
    } catch {
        return $false
    }
    return (
        $Health.status -ceq "PASS" -and
        [int]$Health.starts -eq [int]$Handshake.service_starts -and
        $serviceHeartbeat -ge $readyAt -and
        $supervisorHeartbeat -ge $readyAt
    )
}

function Assert-SafetySupervisorReady($Process, $Handshake) {
    if ($null -eq $Process -or $Process.HasExited) {
        throw "Fast-live safety supervisor is not alive"
    }
    $healthLines = @(& $python -m interexchange_perp_grid.cli health --config $config)
    $healthExit = $LASTEXITCODE
    $healthJson = $healthLines | Where-Object {
        $_ -is [string] -and $_.TrimStart().StartsWith("{")
    } | Select-Object -Last 1
    if ($healthExit -ne 0 -or -not $healthJson) {
        throw "Fast-live safety supervisor health is not current"
    }
    $health = $healthJson | ConvertFrom-Json
    if (-not (Test-SupervisorReadinessEvidence -Health $health -Handshake $Handshake)) {
        throw "Fast-live safety supervisor readiness does not match this process incarnation"
    }
    $Process.Refresh()
    if ($Process.HasExited) {
        throw "Fast-live safety supervisor exited after its readiness check"
    }
}

function Move-StaleSupervisorHandshake(
    [string]$PidPath,
    [string]$HandshakePath,
    [string]$QuarantineRoot
) {
    if (
        (Test-Path -LiteralPath $PidPath -PathType Leaf) -or
        -not (Test-Path -LiteralPath $HandshakePath -PathType Leaf)
    ) { return $false }
    try {
        $orphanHandshake = Get-Content -LiteralPath $HandshakePath -Raw | ConvertFrom-Json
        $orphanPid = [int]$orphanHandshake.pid
        if ($orphanPid -le 0) { throw "invalid PID" }
    } catch {
        throw "Unpaired safety-supervisor handshake is malformed; new entry remains blocked"
    }
    if ($null -ne (Get-Process -Id $orphanPid -ErrorAction SilentlyContinue)) {
        throw "Unpaired safety-supervisor handshake references a live process; new entry remains blocked"
    }
    New-Item -ItemType Directory -Path $QuarantineRoot -Force | Out-Null
    $destination = Join-Path $QuarantineRoot (
        "supervisor-runtime-{0}-{1}.stale.json" -f `
            [DateTime]::UtcNow.ToString("yyyyMMddTHHmmssfffZ"),
            [Guid]::NewGuid().ToString("N")
    )
    Move-Item -LiteralPath $HandshakePath -Destination $destination
    return $true
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
    if (-not (Test-Path -LiteralPath $runtimeManifest -PathType Leaf)) {
        throw "Exact runtime manifest is required before starting the safety supervisor"
    }
    $expectedRuntime = Get-Content -LiteralPath $runtimeManifest -Raw | ConvertFrom-Json
    $artifactDigest = [string]$expectedRuntime.artifact_digest
    if ($artifactDigest -notmatch '^sha256:[0-9a-f]{64}$') {
        throw "Exact native runtime artifact identity is invalid"
    }
    $runtimeManifestSha256 = $artifactDigest.Substring(7)
    $logRoot = Join-Path $root "state/laptop/fast-live"
    New-Item -ItemType Directory -Path $logRoot -Force | Out-Null
    $null = Move-StaleSupervisorHandshake `
        -PidPath $supervisorPid `
        -HandshakePath $supervisorHandshake `
        -QuarantineRoot (Join-Path $logRoot "quarantine")
    if (Test-Path -LiteralPath $supervisorPid -PathType Leaf) {
        $identity = Get-Content -LiteralPath $supervisorPid -Raw | ConvertFrom-Json
        $existing = Get-Process -Id ([int]$identity.pid) -ErrorAction SilentlyContinue
        $handshake = if (Test-Path -LiteralPath $supervisorHandshake -PathType Leaf) {
            Get-Content -LiteralPath $supervisorHandshake -Raw | ConvertFrom-Json
        } else { $null }
        $handshakeArgs = if ($null -ne $handshake) { @($handshake.argv) } else { @() }
        $sameIncarnation = (
            $null -ne $existing -and
            $null -ne $handshake -and
            $existing.StartTime.ToUniversalTime().ToString("o") -ceq [string]$identity.start_time -and
            [IO.Path]::GetFullPath($existing.Path) -ceq [IO.Path]::GetFullPath([string]$identity.path) -and
            [int]$handshake.pid -eq [int]$identity.pid -and
            [IO.Path]::GetFullPath([string]$handshake.executable) -ceq [IO.Path]::GetFullPath($python) -and
            [IO.Path]::GetFullPath([string]$handshake.working_directory) -ceq [IO.Path]::GetFullPath($root)
        )
        $matches = (
            $sameIncarnation -and
            $handshakeArgs -contains "run" -and
            $handshakeArgs -contains "--runtime-manifest" -and
            $handshakeArgs -contains $runtimeManifest -and
            $handshakeArgs -contains "--runtime-handshake" -and
            $handshakeArgs -contains $supervisorHandshake -and
            $handshakeArgs -contains "--repo-root" -and
            $handshakeArgs -contains $root -and
            [string]$handshake.release_sha -ceq [string]$expectedRuntime.release_sha -and
            [string]$handshake.source_sha256 -ceq [string]$expectedRuntime.source_sha256 -and
            [string]$handshake.config_sha256 -ceq [string]$expectedRuntime.config_sha256 -and
            [string]$handshake.runtime_manifest_sha256 -ceq $runtimeManifestSha256 -and
            [string]$identity.runtime_manifest_sha256 -ceq $runtimeManifestSha256
        )
        if ($matches) {
            Assert-SafetySupervisorReady -Process $existing -Handshake $handshake
            return
        }
        if ($null -ne $existing) {
            if (-not $sameIncarnation) {
                throw "Existing Python process cannot be proven to be the recorded safety supervisor"
            }
            $status = Get-FastStatus
            if ([int]$status.active_action_count -gt 0) {
                throw "Stale supervisor identity has an active action; new entry remains blocked"
            }
            Stop-Process -Id $existing.Id
            Wait-Process -Id $existing.Id -Timeout 15 -ErrorAction SilentlyContinue
        }
        Remove-Item -LiteralPath $supervisorPid -Force
        Remove-Item -LiteralPath $supervisorHandshake -Force -ErrorAction SilentlyContinue
    }
    $stamp = [DateTime]::UtcNow.ToString("yyyyMMddTHHmmssfffZ")
    $process = Start-Process -FilePath $python -ArgumentList @(
        "-m", "interexchange_perp_grid.cli", "run",
        "--runtime-manifest", $runtimeManifest,
        "--runtime-handshake", $supervisorHandshake,
        "--repo-root", $root, "--config", $config
    ) -PassThru -WindowStyle Hidden -WorkingDirectory $root `
        -RedirectStandardOutput (Join-Path $logRoot "supervisor-$stamp.stdout.log") `
        -RedirectStandardError (Join-Path $logRoot "supervisor-$stamp.stderr.log")
    [ordered]@{
        pid = $process.Id
        start_time = $process.StartTime.ToUniversalTime().ToString("o")
        path = [IO.Path]::GetFullPath($process.Path)
        runtime_manifest_sha256 = $runtimeManifestSha256
    } | ConvertTo-Json -Compress | Set-Content -LiteralPath $supervisorPid -Encoding utf8
    for ($attempt = 0; $attempt -lt 60; $attempt++) {
        if ($process.HasExited) { throw "Fast-live safety supervisor stopped during startup" }
        if (-not (Test-Path -LiteralPath $supervisorHandshake -PathType Leaf)) {
            Start-Sleep -Seconds 1
            continue
        }
        $startedHandshake = Get-Content -LiteralPath $supervisorHandshake -Raw | ConvertFrom-Json
        if (
            [int]$startedHandshake.pid -ne $process.Id -or
            [string]$startedHandshake.release_sha -cne [string]$expectedRuntime.release_sha -or
            [string]$startedHandshake.source_sha256 -cne [string]$expectedRuntime.source_sha256 -or
            [string]$startedHandshake.config_sha256 -cne [string]$expectedRuntime.config_sha256 -or
            [string]$startedHandshake.runtime_manifest_sha256 -cne $runtimeManifestSha256
        ) { throw "Fast-live safety supervisor runtime handshake mismatch" }
        try {
            Assert-SafetySupervisorReady -Process $process -Handshake $startedHandshake
            return
        } catch {
            if ($process.HasExited) { throw }
        }
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

function Restore-CanaryEvidence {
    if (Test-Path -LiteralPath $canaryEvidence -PathType Leaf) { return }
    $status = Get-FastStatus
    $canaries = @($status.completed_fast_live_actions | Where-Object {
        $_.stage -ceq "canary"
    })
    if ($canaries.Count -gt 1) {
        throw "Multiple completed canaries are ambiguous; no new live action is allowed"
    }
    if ($canaries.Count -eq 1) {
        Invoke-Checked @(
            "-m", "interexchange_perp_grid.cli", "fast-live-stage-report",
            "--stage", "canary",
            "--pair-action-id", [string]$canaries[0].pair_action_id,
            "--model", $model, "--grid", $grid, "--profile", $strategyProfile,
            "--preflight", $preflight, "--runtime-manifest", $runtimeManifest,
            "--output", $canaryEvidence, "--config", $config
        ) "Completed canary evidence recovery failed closed"
    }
}

function Finalize-FastLiveAcceptance {
    if (Test-Path -LiteralPath $acceptance -PathType Leaf) { return }
    foreach ($required in @($canaryEvidence, $pilotEvidence, $preflight)) {
        if (-not (Test-Path -LiteralPath $required -PathType Leaf)) {
            throw "Fast-live acceptance recovery is missing exact durable evidence: $required"
        }
    }
    Invoke-Checked @(
        "-m", "interexchange_perp_grid.cli", "fast-live-acceptance",
        "--runtime-manifest", $runtimeManifest, "--model", $model,
        "--grid", $grid, "--profile", $strategyProfile,
        "--preflight", $preflight, "--canary-evidence", $canaryEvidence,
        "--pilot-evidence", $pilotEvidence,
        "--independent-review", $independentReview, "--output", $acceptance,
        "--repo-root", $root, "--config", $config
    ) "Fast-live laptop acceptance failed closed"
}

function Restore-PilotEvidenceAndAcceptance {
    if (Test-Path -LiteralPath $acceptance -PathType Leaf) { return }
    if (Test-Path -LiteralPath $pilotEvidence -PathType Leaf) {
        Finalize-FastLiveAcceptance
        return
    }
    $status = Get-FastStatus
    $pilots = @($status.completed_fast_live_actions | Where-Object {
        $_.stage -ceq "pilot_a"
    })
    if ($pilots.Count -eq 0) { return }
    if (-not (Test-Path -LiteralPath $preflight -PathType Leaf)) {
        throw "Completed pilot exists without its exact preflight; no new live action is allowed"
    }
    $currentPreflight = Get-Content -LiteralPath $preflight -Raw | ConvertFrom-Json
    $matching = @($pilots | Where-Object {
        $_.activation_hash -ceq [string]$currentPreflight.preflight_sha256
    })
    if ($matching.Count -ne 1) {
        throw "Completed pilot recovery is ambiguous; no new live action is allowed"
    }
    Invoke-Checked @(
        "-m", "interexchange_perp_grid.cli", "fast-live-stage-report",
        "--stage", "pilot_a",
        "--pair-action-id", [string]$matching[0].pair_action_id,
        "--model", $model, "--grid", $grid, "--profile", $strategyProfile,
        "--preflight", $preflight, "--runtime-manifest", $runtimeManifest,
        "--output", $pilotEvidence, "--config", $config
    ) "Completed pilot evidence recovery failed closed"
    Finalize-FastLiveAcceptance
}

function Invoke-Preflight {
    Invoke-Checked @(
        "-m", "interexchange_perp_grid.cli", "fast-live-runtime-control",
        "--action", "resume", "--config", $config
    ) "Fast-live resume failed closed"
    Build-ExactRuntime
    Restore-CanaryEvidence
    Restore-PilotEvidenceAndAcceptance
    if (Test-Path -LiteralPath $acceptance -PathType Leaf) {
        throw "Fast-live laptop acceptance is already complete; no new entry is allowed"
    }
    Ensure-HistoryAndModel
    $stage = if (Test-Path -LiteralPath $canaryEvidence -PathType Leaf) { "pilot_a" } else { "canary" }
    Invoke-Checked @(
        "-m", "interexchange_perp_grid.cli", "fast-live-stage-select",
        "--target", $stage, "--actor", "laptop-fast-live-wrapper", "--config", $config
    ) "Fast-live risk stage selection failed closed"
    $utcNow = [DateTime]::UtcNow
    $since = [DateTime]::new(
        $utcNow.Year, $utcNow.Month, $utcNow.Day, $utcNow.Hour, $utcNow.Minute, 0,
        [DateTimeKind]::Utc
    ).AddMinutes(-4).ToString("o")
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
        Start-SafetySupervisor
        $before = Get-FastStatus
        $env:IPEG_LOCAL_UNLOCK_SECRET = Convert-Secret $unlock
        $env:IPEG_MODE = "live"
        $env:IPEG_LIVE_ENABLED = "true"
        $command = if ($Stage -ceq "canary") { "fast-live-canary" } else { "fast-live-pilot" }
        try {
            $output = @(& $python -m interexchange_perp_grid.cli $command `
                --confirmation $phrase --intent $intent --preflight $preflight `
                --model $model --grid $grid --profile $strategyProfile `
                --runtime-manifest $runtimeManifest --repo-root $root --config $config)
            $exitCode = $LASTEXITCODE
        } finally {
            $env:IPEG_MODE = "shadow"
            $env:IPEG_LIVE_ENABLED = "false"
            Remove-Item Env:IPEG_LOCAL_UNLOCK_SECRET -ErrorAction SilentlyContinue
        }
        if ($exitCode -ne 0) { throw "Fast-live $Stage failed closed before durable ownership" }
        $resultJson = $output | Where-Object {
            $_ -is [string] -and $_.TrimStart().StartsWith("{")
        } | Select-Object -Last 1
        if (-not $resultJson) { throw "Fast-live $Stage result is not machine-readable" }
        $entry = $resultJson | ConvertFrom-Json
        if ($entry.success -ne $true -or -not $entry.queued_pair_action_id) {
            throw "Fast-live $Stage lacks durable pair ownership"
        }
        $timeout = if ($Stage -ceq "canary") { 900 } else { 86400 }
        $null = Wait-StableFlat -PriorCompleted ([int]$before.completed_fast_live_round_trips) `
            -TimeoutSeconds $timeout
        $evidencePath = if ($Stage -ceq "canary") { $canaryEvidence } else { $pilotEvidence }
        $reportStage = if ($Stage -ceq "canary") { "canary" } else { "pilot_a" }
        Invoke-Checked @(
            "-m", "interexchange_perp_grid.cli", "fast-live-stage-report",
            "--stage", $reportStage,
            "--pair-action-id", [string]$entry.queued_pair_action_id,
            "--model", $model, "--grid", $grid, "--profile", $strategyProfile,
            "--preflight", $preflight, "--runtime-manifest", $runtimeManifest,
            "--output", $evidencePath, "--config", $config
        ) "Fast-live $Stage authoritative stable-FLAT report failed closed"
        Write-Host "Fast-live $Stage completed with exchange-verified stable FLAT."
        if ($Stage -ceq "pilot") {
            Finalize-FastLiveAcceptance
        }
    } finally {
        if ($null -ne $unlock) { $unlock.Dispose() }
    }
}

if ($SupervisorReadinessSelfTest) {
    $now = [DateTimeOffset]::UtcNow
    $handshake = [pscustomobject]@{ ready_at = $now.ToString("o"); service_starts = 7 }
    $matching = [pscustomobject]@{
        status = "PASS"
        starts = 7
        heartbeat_at = $now.AddSeconds(1).ToString("o")
        supervisor_heartbeat_at = $now.AddSeconds(1).ToString("o")
    }
    $stale = [pscustomobject]@{
        status = "PASS"
        starts = 7
        heartbeat_at = $now.AddSeconds(1).ToString("o")
        supervisor_heartbeat_at = $now.AddSeconds(-1).ToString("o")
    }
    if (
        -not (Test-SupervisorReadinessEvidence -Health $matching -Handshake $handshake) -or
        (Test-SupervisorReadinessEvidence -Health $stale -Handshake $handshake)
    ) {
        throw "Supervisor readiness evidence self-test failed"
    }
    $testRoot = Join-Path ([IO.Path]::GetTempPath()) (
        "ipeg-supervisor-selftest-{0}" -f [Guid]::NewGuid().ToString("N")
    )
    try {
        New-Item -ItemType Directory -Path $testRoot | Out-Null
        $testPid = Join-Path $testRoot "missing.pid"
        $testHandshake = Join-Path $testRoot "stale-handshake.json"
        $testQuarantine = Join-Path $testRoot "quarantine"
        [IO.File]::WriteAllText($testHandshake, '{"pid":2147483647}')
        if (
            -not (Move-StaleSupervisorHandshake `
                -PidPath $testPid `
                -HandshakePath $testHandshake `
                -QuarantineRoot $testQuarantine) -or
            (Test-Path -LiteralPath $testHandshake) -or
            @((Get-ChildItem -LiteralPath $testQuarantine -File)).Count -ne 1
        ) {
            throw "Supervisor stale-handshake quarantine self-test failed"
        }
    } finally {
        if (Test-Path -LiteralPath $testRoot) {
            Remove-Item -LiteralPath $testRoot -Recurse -Force
        }
    }
    Write-Host "Supervisor readiness evidence self-test PASS"
    exit 0
}

try {
    Require-Python
    switch ($Action) {
        "verify" {
            & "$PSScriptRoot/laptop-aggressive.ps1" -Mode verify
            if ($LASTEXITCODE -ne 0) { throw "Laptop verification failed" }
        }
        "onboard" {
            & "$PSScriptRoot/laptop-onboard.ps1" -OutputPath $currentUserProfilePath
            if ($LASTEXITCODE -ne 0) { throw "Laptop onboarding failed closed" }
            if ($ProfileScope -ceq "LocalMachine") {
                & "$PSScriptRoot/laptop-migrate-s4u-profile.ps1" `
                    -InputPath $currentUserProfilePath -OutputPath $effectiveProfilePath
                if ($LASTEXITCODE -ne 0) { throw "LocalMachine profile migration failed closed" }
            }
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
            Build-ExactRuntime
            Restore-CanaryEvidence
            Restore-PilotEvidenceAndAcceptance
            if (Test-Path -LiteralPath $acceptance -PathType Leaf) {
                Write-Host "Completed pilot recovered; laptop Fast Live acceptance is finalized."
                break
            }
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
                $identity = Get-Content -LiteralPath $supervisorPid -Raw | ConvertFrom-Json
                $process = Get-Process -Id ([int]$identity.pid) -ErrorAction SilentlyContinue
                $handshake = if (Test-Path -LiteralPath $supervisorHandshake -PathType Leaf) {
                    Get-Content -LiteralPath $supervisorHandshake -Raw | ConvertFrom-Json
                } else { $null }
                if (
                    $null -ne $process -and
                    $null -ne $handshake -and
                    $process.StartTime.ToUniversalTime().ToString("o") -ceq [string]$identity.start_time -and
                    [IO.Path]::GetFullPath($process.Path) -ceq [IO.Path]::GetFullPath([string]$identity.path) -and
                    [int]$handshake.pid -eq [int]$identity.pid -and
                    [string]$handshake.runtime_manifest_sha256 -ceq [string]$identity.runtime_manifest_sha256
                ) {
                    Stop-Process -Id $process.Id
                }
                Remove-Item -LiteralPath $supervisorPid -Force -ErrorAction SilentlyContinue
                Remove-Item -LiteralPath $supervisorHandshake -Force -ErrorAction SilentlyContinue
            }
            Write-Host "Fast-live is paused, FLAT, and its local supervisor is stopped."
        }
    }
} finally {
    $env:IPEG_MODE = "shadow"
    $env:IPEG_LIVE_ENABLED = "false"
    foreach ($name in @(
        "IPEG_LOCAL_UNLOCK_SECRET", "IPEG_LOCAL_UNLOCK_VERIFIER", "IPEG_TELEGRAM_BOT_TOKEN",
        "IPEG_BINANCEUSDM_API_KEY", "IPEG_BINANCEUSDM_API_SECRET",
        "IPEG_BINANCEUSDM_API_PASSWORD", "IPEG_BYBIT_API_KEY", "IPEG_BYBIT_API_SECRET",
        "IPEG_BYBIT_API_PASSWORD", "IPEG_OKX_API_KEY", "IPEG_OKX_API_SECRET",
        "IPEG_OKX_API_PASSWORD"
    )) {
        Remove-Item "Env:$name" -ErrorAction SilentlyContinue
    }
}
