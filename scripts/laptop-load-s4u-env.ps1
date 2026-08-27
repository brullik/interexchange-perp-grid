[CmdletBinding()]
param(
    [string]$ProfilePath = "state/laptop-profile-s4u.json",
    [string]$StatePath = "state/laptop/ipeg.sqlite3",
    [string]$MarketPath = "data/laptop/market"
)

$ErrorActionPreference = "Stop"
Set-StrictMode -Version Latest

if ([Environment]::OSVersion.Platform -ne [PlatformID]::Win32NT) {
    throw "laptop-load-s4u-env.ps1 requires Windows DPAPI"
}

Add-Type -AssemblyName System.Security

function Clear-Bytes([byte[]]$Value) {
    if ($null -ne $Value) {
        [Array]::Clear($Value, 0, $Value.Length)
    }
}

function Resolve-LaptopPath([string]$Root, [string]$Path) {
    if ([IO.Path]::IsPathRooted($Path)) {
        return [IO.Path]::GetFullPath($Path)
    }
    return [IO.Path]::GetFullPath((Join-Path $Root $Path))
}

$root = Split-Path -Parent $PSScriptRoot
$resolved = Resolve-LaptopPath $root $ProfilePath
if (-not (Test-Path -LiteralPath $resolved -PathType Leaf)) {
    throw "Encrypted S4U laptop profile is missing: $resolved"
}

$identity = [Security.Principal.WindowsIdentity]::GetCurrent()
$acl = [IO.File]::GetAccessControl(
    $resolved,
    [Security.AccessControl.AccessControlSections]::Access
)
if (-not $acl.AreAccessRulesProtected) {
    throw "Encrypted S4U laptop profile ACL inheritance must be disabled"
}
$rules = $acl.GetAccessRules(
    $true,
    $false,
    [Security.Principal.SecurityIdentifier]
)
foreach ($rule in $rules) {
    if (
        $rule.AccessControlType -eq [Security.AccessControl.AccessControlType]::Allow -and
        $rule.IdentityReference -ne $identity.User
    ) {
        throw "Encrypted S4U laptop profile ACL permits another identity"
    }
}

$envelope = Get-Content -LiteralPath $resolved -Raw | ConvertFrom-Json
if (
    $envelope.schema_version -ne 2 -or
    $envelope.protection_scope -cne "LocalMachine"
) {
    throw "Encrypted S4U laptop profile envelope is invalid"
}

$entropy = $null
$cipherBytes = $null
$clearBytes = $null
try {
    $entropy = [Convert]::FromBase64String([string]$envelope.entropy)
    $cipherBytes = [Convert]::FromBase64String([string]$envelope.ciphertext)
    $clearBytes = [Security.Cryptography.ProtectedData]::Unprotect(
        $cipherBytes,
        $entropy,
        [Security.Cryptography.DataProtectionScope]::LocalMachine
    )
    $plainJson = [Text.Encoding]::UTF8.GetString($clearBytes)
    $profile = $plainJson | ConvertFrom-Json
    if (
        $profile.SchemaVersion -ne 2 -or
        $profile.FastLiveRoute -cne "BTC:bybit>okx" -or
        [string]$profile.LocalLiveUnlockVerifier -notmatch '^pbkdf2-sha256\$600000\$[A-Za-z0-9+/]+={0,2}\$[A-Za-z0-9+/]+={0,2}$'
    ) {
        throw "Encrypted S4U laptop profile identity is invalid"
    }
    foreach ($required in @(
        "TelegramOwnerChatId",
        "TelegramBotToken",
        "BinanceUsdmApiKey",
        "BinanceUsdmApiSecret",
        "BybitApiKey",
        "BybitApiSecret",
        "OkxApiKey",
        "OkxApiSecret",
        "OkxApiPassword"
    )) {
        if ([string]::IsNullOrWhiteSpace([string]$profile.$required)) {
            throw "Encrypted S4U laptop profile is missing required field $required"
        }
    }

    $env:IPEG_MODE = "shadow"
    $env:IPEG_LIVE_ENABLED = "false"
    $env:IPEG_OWNER_ONBOARDING_CONFIRMED = "true"
    $env:IPEG_LOCAL_UNLOCK_VERIFIER = [string]$profile.LocalLiveUnlockVerifier
    $env:IPEG_STATE_PATH = Resolve-LaptopPath $root $StatePath
    $env:IPEG_PARQUET_DIR = Resolve-LaptopPath $root $MarketPath
    $env:IPEG_RUNTIME_KIND = "native-python"
    $env:IPEG_NATIVE_RUNTIME_MANIFEST = [IO.Path]::GetFullPath(
        (Join-Path $root "state/laptop/native-runtime-manifest.json")
    )
    $env:IPEG_TELEGRAM_ENABLED = "true"
    $env:IPEG_TELEGRAM_OWNER_CHAT_ID = [string]$profile.TelegramOwnerChatId
    $env:IPEG_TELEGRAM_BOT_TOKEN = [string]$profile.TelegramBotToken
    $env:IPEG_BINANCEUSDM_API_KEY = [string]$profile.BinanceUsdmApiKey
    $env:IPEG_BINANCEUSDM_API_SECRET = [string]$profile.BinanceUsdmApiSecret
    $env:IPEG_BINANCEUSDM_API_PASSWORD = [string]$profile.BinanceUsdmApiPassword
    $env:IPEG_BYBIT_API_KEY = [string]$profile.BybitApiKey
    $env:IPEG_BYBIT_API_SECRET = [string]$profile.BybitApiSecret
    $env:IPEG_BYBIT_API_PASSWORD = [string]$profile.BybitApiPassword
    $env:IPEG_OKX_API_KEY = [string]$profile.OkxApiKey
    $env:IPEG_OKX_API_SECRET = [string]$profile.OkxApiSecret
    $env:IPEG_OKX_API_PASSWORD = [string]$profile.OkxApiPassword
} finally {
    Clear-Bytes $clearBytes
    Clear-Bytes $cipherBytes
    Clear-Bytes $entropy
    $plainJson = $null
    $profile = $null
    $envelope = $null
}

Write-Host "Laptop S4U shadow environment loaded; live remains disabled."
