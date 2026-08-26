[CmdletBinding()]
param(
    [Parameter(Mandatory = $true)]
    [ValidateSet("verify", "shadow", "smoke5", "smoke30", "qualify", "canary", "pilot", "status", "stop")]
    [string]$Mode,
    [string]$ProfilePath = "state/laptop-profile.clixml",
    [ValidateSet("CurrentUser", "LocalMachine")]
    [string]$ProfileScope = "CurrentUser",
    [int]$ShadowMinutes = 0,
    [switch]$OwnerException12h
)

$ErrorActionPreference = "Stop"
Set-StrictMode -Version Latest

$root = Split-Path -Parent $PSScriptRoot
Set-Location $root
$python = Join-Path $root ".venv/Scripts/python.exe"
$config = Join-Path $root "config/defaults.yaml"
$profile = Join-Path $root "config/AGGRESSIVE_SYMBIOSIS_V1.yaml"
$model = Join-Path $root "state/aggressive-historical-model.json"
$history = Join-Path $root "data/reference-history"
$grid = Join-Path $root "state/aggressive-grid.sqlite3"
$runtime = Join-Path $root "state/aggressive-shadow-runtime.json"
$pidFile = Join-Path $root "state/aggressive-shadow.pid"

function Require-Python {
    if (-not (Test-Path -LiteralPath $python -PathType Leaf)) {
        throw "Exact Python 3.12 environment is missing: $python"
    }
}

function Load-LaptopEnvironment {
    if ($ProfileScope -ceq "LocalMachine") {
        . "$PSScriptRoot/laptop-load-s4u-env.ps1" -ProfilePath $ProfilePath
    } else {
        . "$PSScriptRoot/laptop-load-env.ps1" -ProfilePath $ProfilePath
    }
    if ($env:IPEG_MODE -cne "shadow" -or $env:IPEG_LIVE_ENABLED -cne "false") {
        throw "Aggressive laptop workflow requires shadow mode and live=false"
    }
}

function Invoke-Verify {
    Require-Python
    & $python -c "import sys; raise SystemExit(0 if sys.version_info[:2] == (3, 12) else 3)"
    if ($LASTEXITCODE -ne 0) { throw "exact CPython 3.12 is required" }
    & $python -m pip check
    if ($LASTEXITCODE -ne 0) { throw "installed dependency check failed" }
    & $python scripts/check_lock.py --lock requirements.lock --pyproject pyproject.toml
    if ($LASTEXITCODE -ne 0) { throw "dependency lock check failed" }
    & $python -m ruff format --check src tests
    if ($LASTEXITCODE -ne 0) { throw "Ruff format check failed" }
    & $python -m ruff check src tests
    if ($LASTEXITCODE -ne 0) { throw "Ruff check failed" }
    & $python -m mypy src
    if ($LASTEXITCODE -ne 0) { throw "strict mypy failed" }
    & $python -m pytest
    if ($LASTEXITCODE -ne 0) { throw "pytest failed" }
    & $python -m interexchange_perp_grid.cli doctor --config $config
    if ($LASTEXITCODE -ne 0) { throw "shadow doctor failed" }
    & $python -m interexchange_perp_grid.cli native-runtime-manifest `
        --output "state/native-runtime-manifest.json" --repo-root $root --config $config
    if ($LASTEXITCODE -ne 0) { throw "native runtime manifest failed" }
}

function Ensure-HistoricalModel {
    Require-Python
    # Use closed UTC days only.  One extra day protects the 180-day target
    # against the currently open day without weakening the 90-day live floor.
    $historyEnd = [DateTime]::UtcNow.Date
    $historyStart = $historyEnd.AddDays(-181)
    & $python -m interexchange_perp_grid.cli reference-history-proof `
        --venue-a bybit --venue-b okx --base BTC `
        --since $historyStart.ToString("o") --end $historyEnd.ToString("o") --limit 1000 `
        --output-root $history --profile $profile --config $config
    if ($LASTEXITCODE -ne 0) { throw "exact paginated historical acquisition failed closed" }
    & $python -m interexchange_perp_grid.cli aggressive-model-proof `
        --venue-a bybit --venue-b okx --base BTC `
        --start $historyStart.ToString("o") --end $historyEnd.ToString("o") `
        --history-root $history --artifact $model --profile $profile --config $config
    if ($LASTEXITCODE -ne 0) { throw "exact historical model build failed closed" }
}

function Invoke-ShadowCycle {
    $since = [DateTime]::UtcNow.AddMinutes(-4).ToString("o")
    & $python -m interexchange_perp_grid.cli reference-history-proof `
        --venue-a bybit --venue-b okx --base BTC --since $since --limit 4 `
        --output-root $history --profile $profile --config $config
    if ($LASTEXITCODE -ne 0) { throw "current reference minute refresh failed" }
    $result = @(
        & $python -m interexchange_perp_grid.cli aggressive-shadow-once `
            --model $model --history-root $history --grid $grid --profile $profile `
            --config $config --timeout 30
    )
    if ($LASTEXITCODE -ne 0) { throw "aggressive public shadow cycle failed closed" }
    $json = $result | Where-Object { $_ -is [string] -and $_.TrimStart().StartsWith("{") } |
        Select-Object -Last 1
    if (-not $json) { throw "aggressive public shadow cycle returned no JSON evidence" }
    $json | Set-Content -LiteralPath $runtime -Encoding utf8
}

switch ($Mode) {
    "verify" { Invoke-Verify }
    "shadow" {
        Require-Python
        Load-LaptopEnvironment
        if (-not (Test-Path -LiteralPath $model -PathType Leaf)) {
            Ensure-HistoricalModel
        }
        if ($ShadowMinutes -lt 0) { throw "ShadowMinutes cannot be negative" }
        [Environment]::ProcessId | Set-Content -LiteralPath $pidFile -Encoding ascii
        $started = [DateTime]::UtcNow
        try {
            do {
                Invoke-ShadowCycle
                if ($ShadowMinutes -gt 0 -and
                    [DateTime]::UtcNow -ge $started.AddMinutes($ShadowMinutes)) { break }
                Start-Sleep -Seconds 60
            } while ($true)
        } finally {
            Remove-Item -LiteralPath $pidFile -Force -ErrorAction SilentlyContinue
        }
    }
    "qualify" {
        if ($OwnerException12h) {
            $scheduledProfile = if ($ProfileScope -ceq "LocalMachine") {
                $ProfilePath
            } else {
                "state/laptop-profile-s4u.json"
            }
            & "$PSScriptRoot/laptop-qualification-scheduled.ps1" `
                -ProfilePath $scheduledProfile -ProfileScope LocalMachine
            if (-not $?) { throw "detached 12-hour qualification failed to start" }
            return
        }
        Ensure-HistoricalModel
        & "$PSScriptRoot/laptop-qualification.ps1" -ProfilePath $ProfilePath `
            -ProfileScope $ProfileScope
        if ($LASTEXITCODE -ne 0) { throw "base laptop qualification failed closed" }
        & $python -m interexchange_perp_grid.cli aggressive-qualification-bind `
            --qualification "state/qualification.json" `
            --runtime-manifest "state/laptop/native-runtime-manifest.json" `
            --model $model --grid $grid --profile $profile `
            --output "state/aggressive-qualification.json"
        if ($LASTEXITCODE -ne 0) { throw "aggressive qualification binding failed closed" }
    }
    "smoke30" {
        & "$PSScriptRoot/laptop-smoke-detached.ps1" -ProfilePath $ProfilePath `
            -ProfileScope $ProfileScope -SmokeMinutes 30
        if ($LASTEXITCODE -ne 0) { throw "30-minute qualification rehearsal failed closed" }
    }
    "smoke5" {
        & "$PSScriptRoot/laptop-smoke-detached.ps1" -ProfilePath $ProfilePath `
            -ProfileScope $ProfileScope -SmokeMinutes 5
        if ($LASTEXITCODE -ne 0) { throw "5-minute qualification rehearsal failed closed" }
    }
    "canary" {
        Write-Host "Canary requires a separate, explicit live-money authorization at execution time."
        & $python -m interexchange_perp_grid.cli aggressive-qualification-check `
            --binding "state/aggressive-qualification.json" `
            --qualification "state/qualification.json" `
            --runtime-manifest "state/laptop/native-runtime-manifest.json" `
            --model $model --grid $grid --profile $profile
        if ($LASTEXITCODE -ne 0) { throw "aggressive qualification is missing or stale" }
        & "$PSScriptRoot/laptop-pilot.ps1" -ProfilePath $ProfilePath `
            -ProfileScope $ProfileScope -Aggressive
        if ($LASTEXITCODE -ne 0) { throw "aggressive canary failed closed" }
    }
    "pilot" {
        Write-Host "pilot_a requires separate owner authorization and a standard 24-hour qualification."
        & "$PSScriptRoot/laptop-aggressive-pilot-a.ps1" -ProfilePath $ProfilePath `
            -ProfileScope $ProfileScope
        if ($LASTEXITCODE -ne 0) { throw "aggressive pilot_a failed closed" }
    }
    "status" {
        $payload = [ordered]@{
            running = $false
            process_id = $null
            evidence = $null
            qualification = $null
            live_enabled = $false
        }
        if (Test-Path -LiteralPath $pidFile -PathType Leaf) {
            $processId = [int](Get-Content -LiteralPath $pidFile -Raw)
            $payload.process_id = $processId
            $payload.running = $null -ne (Get-Process -Id $processId -ErrorAction SilentlyContinue)
        }
        if (Test-Path -LiteralPath $runtime -PathType Leaf) {
            $payload.evidence = Get-Content -LiteralPath $runtime -Raw | ConvertFrom-Json
        }
        Require-Python
        $qualification = @(
            & $python -m interexchange_perp_grid.cli qualification-epoch-status --config $config
        )
        if ($LASTEXITCODE -eq 0) {
            $qualificationJson = $qualification |
                Where-Object { $_ -is [string] -and $_.TrimStart().StartsWith("{") } |
                Select-Object -Last 1
            if ($qualificationJson) {
                $payload.qualification = $qualificationJson | ConvertFrom-Json
            }
        }
        $payload | ConvertTo-Json -Depth 20
    }
    "stop" {
        if (-not (Test-Path -LiteralPath $pidFile -PathType Leaf)) {
            Write-Host "Aggressive shadow is not running."
            break
        }
        $processId = [int](Get-Content -LiteralPath $pidFile -Raw)
        $process = Get-Process -Id $processId -ErrorAction SilentlyContinue
        if ($null -ne $process) { Stop-Process -Id $processId }
        Remove-Item -LiteralPath $pidFile -Force -ErrorAction SilentlyContinue
        Write-Host "Aggressive shadow stopped; any interrupted qualification is invalid."
    }
}
