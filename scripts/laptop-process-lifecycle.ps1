Set-StrictMode -Version Latest

function Wait-IpegOwnedProcess {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true)]
        [System.Diagnostics.Process]$Process,
        [Parameter(Mandatory = $true)]
        [string]$FailureLabel
    )

    $Process.WaitForExit()
    $Process.Refresh()
    if ($Process.ExitCode -ne 0) {
        throw "$FailureLabel failed closed with exit $($Process.ExitCode)"
    }
}
