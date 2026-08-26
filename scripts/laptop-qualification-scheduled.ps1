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
$bootstrap = Join-Path $PSScriptRoot "laptop-smoke-detached.ps1"
if (-not (Test-Path -LiteralPath $bootstrap -PathType Leaf)) {
    throw "Qualification bootstrap is missing: $bootstrap"
}

$runId = [Guid]::NewGuid().ToString("N")
$taskName = "IPEG-Laptop-Qualification-Once"
$existing = Get-ScheduledTask -TaskName $taskName -ErrorAction SilentlyContinue
if ($null -ne $existing) {
    $existingInfo = Get-ScheduledTaskInfo -TaskName $taskName
    $triggerStart = [DateTime]::Parse([string]$existing.Triggers[0].StartBoundary)
    $terminalResult = $existingInfo.LastTaskResult -notin @(267009, 267011)
    $completed = $existingInfo.LastRunTime -ge $triggerStart -and $terminalResult
    if (
        $existing.State -in @("Running", "Queued") -or
        -not $completed
    ) {
        throw "The independent qualification bootstrap task is pending or running"
    }
    Unregister-ScheduledTask -TaskName $taskName -Confirm:$false
}

$profileFullPath = if ([IO.Path]::IsPathRooted($ProfilePath)) {
    [IO.Path]::GetFullPath($ProfilePath)
} else {
    [IO.Path]::GetFullPath((Join-Path $root $ProfilePath))
}
$windowsPowerShell = Join-Path $env:SystemRoot "System32/WindowsPowerShell/v1.0/powershell.exe"
if (-not (Test-Path -LiteralPath $windowsPowerShell -PathType Leaf)) {
    throw "Windows PowerShell task host is missing: $windowsPowerShell"
}

function Quote-TaskArgument {
    param([Parameter(Mandatory = $true)][string]$Value)
    if ($Value.Contains('"')) { throw "Task argument contains an unsupported quote" }
    return '"' + $Value + '"'
}

$arguments = @(
    "-NoProfile",
    "-NonInteractive",
    "-ExecutionPolicy", "Bypass",
    "-File", (Quote-TaskArgument $bootstrap),
    "-ProfilePath", (Quote-TaskArgument $profileFullPath),
    "-ProfileScope", $ProfileScope,
    "-OwnerException12h",
    "-RunId", $runId
) -join " "
$startAt = (Get-Date).AddSeconds(20)
$action = New-ScheduledTaskAction `
    -Execute $windowsPowerShell -Argument $arguments -WorkingDirectory $root
$trigger = New-ScheduledTaskTrigger -Once -At $startAt
$principal = New-ScheduledTaskPrincipal `
    -UserId ([Security.Principal.WindowsIdentity]::GetCurrent().Name) `
    -LogonType Interactive -RunLevel Limited
$settings = New-ScheduledTaskSettingsSet `
    -AllowStartIfOnBatteries -DontStopIfGoingOnBatteries -StartWhenAvailable `
    -MultipleInstances IgnoreNew -ExecutionTimeLimit (New-TimeSpan -Minutes 10)

Register-ScheduledTask -TaskName $taskName -Action $action -Trigger $trigger `
    -Principal $principal -Settings $settings | Out-Null

[pscustomobject]@{
    task_name = $taskName
    run_id = $runId
    starts_at = $startAt.ToUniversalTime().ToString("o")
    live_enabled = $false
} | ConvertTo-Json -Compress
