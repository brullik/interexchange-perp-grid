[CmdletBinding()]
param(
    [string]$ProfilePath = "state/laptop-profile-s4u.json",
    [ValidateSet("CurrentUser", "LocalMachine")]
    [string]$ProfileScope = "CurrentUser",
    [ValidateSet(5, 30)]
    [int]$SmokeMinutes = 0,
    [switch]$OwnerException12h,
    [switch]$WaitForRunner,
    [string]$RunId = ""
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
. "$PSScriptRoot/laptop-process-lifecycle.ps1"

if (($SmokeMinutes -eq 0) -eq (-not $OwnerException12h)) {
    throw "Choose exactly one detached smoke or 12-hour qualification mode"
}
if ($WaitForRunner -and -not $OwnerException12h) {
    throw "Only the scheduled 12-hour qualification may own and wait for its runner"
}
if ($RunId) {
    if ($RunId -cnotmatch "^[0-9a-f]{32}$") {
        throw "Detached qualification run id must be 32 lowercase hexadecimal characters"
    }
    $env:IPEG_LAPTOP_RUN_ID = $RunId
} else {
    $env:IPEG_LAPTOP_RUN_ID = [Guid]::NewGuid().ToString("N")
}
$runKind = if ($OwnerException12h) { "qualification" } else { "smoke" }
if (-not $OwnerException12h) {
    $env:IPEG_LAPTOP_SMOKE_RUN_ID = $env:IPEG_LAPTOP_RUN_ID
    $env:IPEG_STATE_PATH = [IO.Path]::GetFullPath(
        (Join-Path $root "state/laptop/smoke/$env:IPEG_LAPTOP_RUN_ID/ipeg.sqlite3")
    )
    $env:IPEG_PARQUET_DIR = [IO.Path]::GetFullPath(
        (Join-Path $root "data/laptop/smoke/$env:IPEG_LAPTOP_RUN_ID/market")
    )
} else {
    $env:IPEG_LAPTOP_12H_OWNER_EXCEPTION = "I_ACCEPT_LAPTOP_12H_QUALIFICATION_EXCEPTION"
}
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

if ($OwnerException12h) {
    $branch = (& git branch --show-current).Trim()
    if ($LASTEXITCODE -ne 0 -or $branch -cne "main") {
        throw "12-hour qualification requires the local main branch"
    }
    $head = (& git rev-parse HEAD).Trim().ToLowerInvariant()
    $originMain = (& git rev-parse refs/remotes/origin/main).Trim().ToLowerInvariant()
    $dirty = @(& git status --porcelain --untracked-files=no)
    if ($LASTEXITCODE -ne 0 -or $head -cne $originMain -or $head -cne $env:IPEG_RELEASE_SHA) {
        throw "local main, fetched origin/main and native release must match exactly"
    }
    if ($dirty.Count -gt 0) { throw "12-hour qualification requires a clean tracked checkout" }
}

$lockName = if ($OwnerException12h) { "qualification.lock" } else { "qualification-smoke.lock" }
$lockPath = Join-Path $root "state/laptop/$lockName"
$existingLock = Get-Item -LiteralPath $lockPath -ErrorAction SilentlyContinue
if ($null -ne $existingLock) {
    if (((Get-Date) - $existingLock.LastWriteTime).TotalSeconds -lt 120) {
        throw "A detached qualification smoke launch is already locked"
    }
    $oldLockParts = (Get-Content -LiteralPath $lockPath -Raw).Trim().Split("|")
    $oldOwner = $oldLockParts[0]
    if ($oldOwner -notmatch "^[0-9a-f]{32}$") {
        throw "Detached qualification smoke lock identity is invalid"
    }
    if ($oldLockParts.Count -eq 2 -and $oldLockParts[1] -match "^[1-9][0-9]*$") {
        $oldHost = Get-Process -Id ([int]$oldLockParts[1]) -ErrorAction SilentlyContinue
        if ($null -ne $oldHost) { throw "A qualification lock owner is still active" }
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
$activeRunner = @(
    Get-CimInstance Win32_Process |
        Where-Object {
            $commandLine = [string]$_.CommandLine
            (
                $_.Name -ieq "python.exe" -and
                (
                    ($OwnerException12h -and $commandLine -like "*interexchange_perp_grid*") -or
                    ($commandLine -like "*laptop_smoke_runner.py*" -and
                        ((-not $OwnerException12h -and $commandLine -like "*--minutes*") -or
                         ($OwnerException12h -and $commandLine -like "*--qualification-12h*"))) -or
                    ($OwnerException12h -and $commandLine -like "*laptop-qualification-run*")
                )
            ) -or (
                $OwnerException12h -and $_.Name -ieq "powershell.exe" -and
                $commandLine -like "*laptop-qualification.ps1*"
            )
        }
)
if ($activeRunner.Count -gt 0) {
    throw "A detached $runKind runner is already active"
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
        "$env:IPEG_LAPTOP_RUN_ID|0"
    )
    $lockStream.Write($lockBytes, 0, $lockBytes.Length)
    $lockStream.Flush($true)
    $lockStream.Dispose()
    $lockStream = $null
} catch [IO.IOException] {
    throw "A detached qualification smoke launch is already locked"
}

try {
    foreach ($venue in @("bybit", "okx")) {
        & $python -m interexchange_perp_grid.cli private-probe --venue $venue --authenticated
        if ($LASTEXITCODE -ne 0) { throw "$venue private capability probe failed" }
    }
    $runnerArguments = @($runner, "--repo-root", $root)
    if ($OwnerException12h) { $runnerArguments += "--qualification-12h" }
    else { $runnerArguments += @("--minutes", $SmokeMinutes) }
    $runnerArguments += @("--run-id", $env:IPEG_LAPTOP_RUN_ID)
    $process = Start-Process -FilePath $python `
        -ArgumentList $runnerArguments `
        -WorkingDirectory $root `
        -WindowStyle Hidden `
        -PassThru
    $spawned = $true
    if ($OwnerException12h) {
        Write-Host "Detached one-time 12-hour laptop qualification runner started."
    } else {
        Write-Host "Detached non-accepting $SmokeMinutes-minute smoke runner started."
    }
    Write-Host "PID=$($process.Id)"
    Write-Host "RUN_ID=$env:IPEG_LAPTOP_RUN_ID"
    Write-Host "STATE=$env:IPEG_STATE_PATH"
    Write-Host "Live remains disabled."
    if ($WaitForRunner) {
        Wait-IpegOwnedProcess -Process $process -FailureLabel "Owned qualification runner"
    }
} finally {
    if ($null -ne $lockStream) { $lockStream.Dispose() }
    if (-not $spawned -and (Test-Path -LiteralPath $lockPath -PathType Leaf)) {
        $owner = Get-Content -LiteralPath $lockPath -Raw
        if ($owner.Trim().StartsWith("$env:IPEG_LAPTOP_RUN_ID|")) {
            Remove-Item -LiteralPath $lockPath -Force
        }
    }
}
