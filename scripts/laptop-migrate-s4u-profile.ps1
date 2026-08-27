[CmdletBinding()]
param(
    [string]$InputPath = "state/laptop-profile.clixml",
    [string]$OutputPath = "state/laptop-profile-s4u.json"
)

$ErrorActionPreference = "Stop"
Set-StrictMode -Version Latest

if ([Environment]::OSVersion.Platform -ne [PlatformID]::Win32NT) {
    throw "laptop-migrate-s4u-profile.ps1 requires Windows DPAPI"
}

Add-Type -AssemblyName System.Security

function Convert-Secret([Security.SecureString]$Value) {
    $pointer = [Runtime.InteropServices.Marshal]::SecureStringToBSTR($Value)
    try {
        return [Runtime.InteropServices.Marshal]::PtrToStringBSTR($pointer)
    } finally {
        [Runtime.InteropServices.Marshal]::ZeroFreeBSTR($pointer)
    }
}

function Clear-Bytes([byte[]]$Value) {
    if ($null -ne $Value) {
        [Array]::Clear($Value, 0, $Value.Length)
    }
}

function Test-BytesEqual([byte[]]$Left, [byte[]]$Right) {
    if ($Left.Length -ne $Right.Length) {
        return $false
    }
    $difference = 0
    for ($index = 0; $index -lt $Left.Length; $index++) {
        $difference = $difference -bor ($Left[$index] -bxor $Right[$index])
    }
    return $difference -eq 0
}

function Resolve-LaptopPath([string]$Root, [string]$Path) {
    if ([IO.Path]::IsPathRooted($Path)) {
        return [IO.Path]::GetFullPath($Path)
    }
    return [IO.Path]::GetFullPath((Join-Path $Root $Path))
}

function Set-OwnerOnlyAcl([string]$Path) {
    $identity = [Security.Principal.WindowsIdentity]::GetCurrent()
    $security = [Security.AccessControl.FileSecurity]::new()
    $security.SetOwner($identity.User)
    $security.SetAccessRuleProtection($true, $false)
    $rule = [Security.AccessControl.FileSystemAccessRule]::new(
        $identity.User,
        [Security.AccessControl.FileSystemRights]::FullControl,
        [Security.AccessControl.AccessControlType]::Allow
    )
    $security.AddAccessRule($rule)
    [IO.File]::SetAccessControl($Path, $security)

    $verified = [IO.File]::GetAccessControl(
        $Path,
        [Security.AccessControl.AccessControlSections]::Access
    )
    $rules = @($verified.GetAccessRules(
        $true,
        $false,
        [Security.Principal.SecurityIdentifier]
    ))
    if (
        -not $verified.AreAccessRulesProtected -or
        $rules.Count -ne 1 -or
        $rules[0].IdentityReference -ne $identity.User -or
        $rules[0].AccessControlType -ne [Security.AccessControl.AccessControlType]::Allow -or
        ($rules[0].FileSystemRights -band [Security.AccessControl.FileSystemRights]::FullControl) -ne
            [Security.AccessControl.FileSystemRights]::FullControl
    ) {
        throw "S4U profile ACL is not restricted to the current Windows identity"
    }
}

$root = Split-Path -Parent $PSScriptRoot
$input = Resolve-LaptopPath $root $InputPath
$output = Resolve-LaptopPath $root $OutputPath
if (-not (Test-Path -LiteralPath $input -PathType Leaf)) {
    throw "Encrypted current-user laptop profile is missing: $input"
}

$serialized = [IO.File]::ReadAllText($input, [Text.Encoding]::UTF8)
$profile = [Management.Automation.PSSerializer]::Deserialize($serialized)
if (
    $profile.SchemaVersion -ne 2 -or
    $profile.FastLiveRoute -cne "BTC:bybit>okx" -or
    [string]$profile.LocalLiveUnlockVerifier -notmatch '^pbkdf2-sha256\$600000\$[A-Za-z0-9+/]+={0,2}\$[A-Za-z0-9+/]+={0,2}$'
) {
    throw "Encrypted current-user laptop profile identity is invalid"
}

$clearBytes = $null
$cipherBytes = $null
$roundTripBytes = $null
$entropy = [byte[]]::new(32)
$temporary = "$output.tmp"
$backup = "$output.backup"
try {
    [Security.Cryptography.RandomNumberGenerator]::Create().GetBytes($entropy)
    $plain = [ordered]@{
        SchemaVersion = 2
        FastLiveRoute = "BTC:bybit>okx"
        LocalLiveUnlockVerifier = [string]$profile.LocalLiveUnlockVerifier
        TelegramOwnerChatId = [string]$profile.TelegramOwnerChatId
        TelegramBotToken = Convert-Secret $profile.TelegramBotToken
        BinanceUsdmApiKey = Convert-Secret $profile.BinanceUsdmApiKey
        BinanceUsdmApiSecret = Convert-Secret $profile.BinanceUsdmApiSecret
        BinanceUsdmApiPassword = Convert-Secret $profile.BinanceUsdmApiPassword
        BybitApiKey = Convert-Secret $profile.BybitApiKey
        BybitApiSecret = Convert-Secret $profile.BybitApiSecret
        BybitApiPassword = Convert-Secret $profile.BybitApiPassword
        OkxApiKey = Convert-Secret $profile.OkxApiKey
        OkxApiSecret = Convert-Secret $profile.OkxApiSecret
        OkxApiPassword = Convert-Secret $profile.OkxApiPassword
    }
    $plainJson = $plain | ConvertTo-Json -Compress
    $clearBytes = [Text.Encoding]::UTF8.GetBytes($plainJson)
    $cipherBytes = [Security.Cryptography.ProtectedData]::Protect(
        $clearBytes,
        $entropy,
        [Security.Cryptography.DataProtectionScope]::LocalMachine
    )
    $roundTripBytes = [Security.Cryptography.ProtectedData]::Unprotect(
        $cipherBytes,
        $entropy,
        [Security.Cryptography.DataProtectionScope]::LocalMachine
    )
    if (-not (Test-BytesEqual $clearBytes $roundTripBytes)) {
        throw "Local-machine DPAPI round-trip failed"
    }

    $envelope = [ordered]@{
        schema_version = 2
        protection_scope = "LocalMachine"
        entropy = [Convert]::ToBase64String($entropy)
        ciphertext = [Convert]::ToBase64String($cipherBytes)
    }
    $parent = Split-Path -Parent $output
    New-Item -ItemType Directory -Path $parent -Force | Out-Null
    [IO.File]::WriteAllText($temporary, "", [Text.UTF8Encoding]::new($false))
    Set-OwnerOnlyAcl $temporary
    [IO.File]::WriteAllText(
        $temporary,
        ($envelope | ConvertTo-Json -Compress),
        [Text.UTF8Encoding]::new($false)
    )
    if ([IO.File]::Exists($output)) {
        # File.Replace preserves the destination DACL.  Remove every foreign ACE
        # before replacement so machine-wide DPAPI ciphertext is never installed
        # behind a permissive pre-existing ACL.
        Set-OwnerOnlyAcl $output
        if ([IO.File]::Exists($backup)) {
            [IO.File]::Delete($backup)
        }
        [IO.File]::Replace($temporary, $output, $backup)
        [IO.File]::Delete($backup)
    } else {
        [IO.File]::Move($temporary, $output)
    }
    Set-OwnerOnlyAcl $output
} finally {
    Clear-Bytes $clearBytes
    Clear-Bytes $cipherBytes
    Clear-Bytes $roundTripBytes
    Clear-Bytes $entropy
    $plainJson = $null
    $plain = $null
    $serialized = $null
    if ([IO.File]::Exists($temporary)) {
        [IO.File]::Delete($temporary)
    }
    if ([IO.File]::Exists($backup)) {
        [IO.File]::Delete($backup)
    }
}

Write-Host "Encrypted local-machine DPAPI profile stored outside Git: $output"
Write-Host "ACL is restricted to this Windows identity; live remains disabled."
