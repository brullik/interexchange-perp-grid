[CmdletBinding()]
param(
    [string]$ProfilePath = "state/laptop-profile.clixml",
    [string]$StatePath = "state/laptop/ipeg.sqlite3",
    [string]$MarketPath = "data/laptop/market"
)

$ErrorActionPreference = "Stop"
Set-StrictMode -Version Latest

if ([Environment]::OSVersion.Platform -ne [PlatformID]::Win32NT) {
    throw "laptop-load-env.ps1 requires Windows DPAPI"
}

function Convert-Secret([Security.SecureString]$Value) {
    $pointer = [Runtime.InteropServices.Marshal]::SecureStringToBSTR($Value)
    try {
        return [Runtime.InteropServices.Marshal]::PtrToStringBSTR($pointer)
    } finally {
        [Runtime.InteropServices.Marshal]::ZeroFreeBSTR($pointer)
    }
}

$root = Split-Path -Parent $PSScriptRoot
$resolved = [System.IO.Path]::GetFullPath((Join-Path $root $ProfilePath))
if (-not (Test-Path -LiteralPath $resolved -PathType Leaf)) {
    throw "Encrypted laptop onboarding file is missing: $resolved"
}
$serialized = [IO.File]::ReadAllText($resolved, [Text.Encoding]::UTF8)
$profile = [Management.Automation.PSSerializer]::Deserialize($serialized)
if ($profile.SchemaVersion -ne 1 -or $profile.QualificationRoute -cne "BTC:bybit>okx") {
    throw "Encrypted laptop onboarding identity is invalid"
}

$env:IPEG_MODE = "shadow"
$env:IPEG_LIVE_ENABLED = "false"
$env:IPEG_OWNER_ONBOARDING_CONFIRMED = "true"
$env:IPEG_QUALIFICATION_ROUTE = "BTC:bybit>okx"
$env:IPEG_STATE_PATH = [System.IO.Path]::GetFullPath((Join-Path $root $StatePath))
$env:IPEG_PARQUET_DIR = [System.IO.Path]::GetFullPath((Join-Path $root $MarketPath))
$env:IPEG_RUNTIME_KIND = "native-python"
$env:IPEG_NATIVE_RUNTIME_MANIFEST = [System.IO.Path]::GetFullPath(
    (Join-Path $root "state/laptop/native-runtime-manifest.json")
)
$env:IPEG_TELEGRAM_ENABLED = "true"
$env:IPEG_TELEGRAM_OWNER_CHAT_ID = [string]$profile.TelegramOwnerChatId
$env:IPEG_TELEGRAM_BOT_TOKEN = Convert-Secret $profile.TelegramBotToken
$env:IPEG_BINANCEUSDM_API_KEY = Convert-Secret $profile.BinanceUsdmApiKey
$env:IPEG_BINANCEUSDM_API_SECRET = Convert-Secret $profile.BinanceUsdmApiSecret
$env:IPEG_BINANCEUSDM_API_PASSWORD = Convert-Secret $profile.BinanceUsdmApiPassword
$env:IPEG_BYBIT_API_KEY = Convert-Secret $profile.BybitApiKey
$env:IPEG_BYBIT_API_SECRET = Convert-Secret $profile.BybitApiSecret
$env:IPEG_BYBIT_API_PASSWORD = Convert-Secret $profile.BybitApiPassword
$env:IPEG_OKX_API_KEY = Convert-Secret $profile.OkxApiKey
$env:IPEG_OKX_API_SECRET = Convert-Secret $profile.OkxApiSecret
$env:IPEG_OKX_API_PASSWORD = Convert-Secret $profile.OkxApiPassword

Write-Host "Laptop shadow environment loaded into this PowerShell process; live remains disabled."
