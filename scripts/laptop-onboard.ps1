[CmdletBinding()]
param(
    [string]$OutputPath = "state/laptop-profile.clixml"
)

$ErrorActionPreference = "Stop"
Set-StrictMode -Version Latest

if ([Environment]::OSVersion.Platform -ne [PlatformID]::Win32NT) {
    throw "laptop-onboard.ps1 requires Windows DPAPI"
}

function Confirm-Fact([string]$Prompt) {
    $answer = Read-Host "$Prompt Type CONFIRM"
    if ($answer -cne "CONFIRM") {
        throw "Owner onboarding confirmation was not provided; nothing was stored"
    }
}

function Read-RequiredSecret([string]$Prompt) {
    $value = Read-Host $Prompt -AsSecureString
    if ($value.Length -eq 0) {
        throw "$Prompt is required"
    }
    return $value
}

Confirm-Fact "Confirm Binance USD-M, Bybit and OKX use dedicated subaccounts."
Confirm-Fact "Confirm API permissions are limited to read and futures trading; withdrawal, transfer, wallet, address-book and API-management are disabled."
Confirm-Fact "Confirm every API key is restricted to this laptop's current public IP."
Confirm-Fact "Confirm all three subaccounts are funded for qualification and minimum-notional canary recovery."

$chatId = Read-Host "Telegram numeric owner chat ID"
if ($chatId -notmatch '^-?[0-9]+$') {
    throw "Telegram owner chat ID must be numeric"
}

$payload = [pscustomobject]@{
    SchemaVersion = 1
    QualificationRoute = "BTC:bybit>okx"
    TelegramOwnerChatId = $chatId
    TelegramBotToken = Read-RequiredSecret "Telegram bot token"
    BinanceUsdmApiKey = Read-RequiredSecret "BINANCEUSDM API key"
    BinanceUsdmApiSecret = Read-RequiredSecret "BINANCEUSDM API secret"
    BinanceUsdmApiPassword = Read-Host "BINANCEUSDM API password (empty when unused)" -AsSecureString
    BybitApiKey = Read-RequiredSecret "BYBIT API key"
    BybitApiSecret = Read-RequiredSecret "BYBIT API secret"
    BybitApiPassword = Read-Host "BYBIT API password (empty when unused)" -AsSecureString
    OkxApiKey = Read-RequiredSecret "OKX API key"
    OkxApiSecret = Read-RequiredSecret "OKX API secret"
    OkxApiPassword = Read-RequiredSecret "OKX API passphrase"
}

$root = Split-Path -Parent $PSScriptRoot
$resolved = [System.IO.Path]::GetFullPath((Join-Path $root $OutputPath))
$parent = Split-Path -Parent $resolved
New-Item -ItemType Directory -Path $parent -Force | Out-Null
$temporary = "$resolved.tmp"
try {
    $payload | Export-Clixml -LiteralPath $temporary -Force
    Move-Item -LiteralPath $temporary -Destination $resolved -Force
    $owner = [Security.Principal.WindowsIdentity]::GetCurrent().Name
    & icacls.exe $resolved /inheritance:r /grant:r "${owner}:(F)" | Out-Null
} finally {
    Remove-Item -LiteralPath $temporary -Force -ErrorAction SilentlyContinue
}

Write-Host "Encrypted current-user DPAPI credentials stored outside Git: $resolved"
Write-Host "Live remains disabled. Do not copy this file to another account or machine."
