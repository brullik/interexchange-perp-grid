[CmdletBinding()]
param(
    [string]$OutputPath = "state/laptop-profile.clixml"
)

$ErrorActionPreference = "Stop"
Set-StrictMode -Version Latest

if ([Environment]::OSVersion.Platform -ne [PlatformID]::Win32NT) {
    throw "laptop-onboard.ps1 requires Windows DPAPI"
}

Add-Type -AssemblyName System.Drawing
Add-Type -AssemblyName System.Windows.Forms

function Confirm-Fact([string]$Prompt) {
    $answer = Read-Host "$Prompt Type CONFIRM"
    if ($answer -cne "CONFIRM") {
        throw "Owner onboarding confirmation was not provided; nothing was stored"
    }
}

function Read-SecretDialog([string]$Prompt, [bool]$Required) {
    $form = [Windows.Forms.Form]::new()
    $form.Text = "Interexchange Perp Grid secure input"
    $form.ClientSize = [Drawing.Size]::new(620, 150)
    $form.FormBorderStyle = [Windows.Forms.FormBorderStyle]::FixedDialog
    $form.MaximizeBox = $false
    $form.MinimizeBox = $false
    $form.StartPosition = [Windows.Forms.FormStartPosition]::CenterScreen
    $form.TopMost = $true

    $label = [Windows.Forms.Label]::new()
    $label.AutoSize = $true
    $label.Location = [Drawing.Point]::new(16, 16)
    $label.Text = $Prompt

    $input = [Windows.Forms.TextBox]::new()
    $input.Location = [Drawing.Point]::new(16, 46)
    $input.Size = [Drawing.Size]::new(588, 27)
    $input.UseSystemPasswordChar = $true
    $input.ShortcutsEnabled = $true

    $ok = [Windows.Forms.Button]::new()
    $ok.DialogResult = [Windows.Forms.DialogResult]::OK
    $ok.Location = [Drawing.Point]::new(430, 100)
    $ok.Size = [Drawing.Size]::new(82, 30)
    $ok.Text = "OK"

    $cancel = [Windows.Forms.Button]::new()
    $cancel.DialogResult = [Windows.Forms.DialogResult]::Cancel
    $cancel.Location = [Drawing.Point]::new(522, 100)
    $cancel.Size = [Drawing.Size]::new(82, 30)
    $cancel.Text = "Cancel"

    $form.AcceptButton = $ok
    $form.CancelButton = $cancel
    $form.Controls.AddRange([Windows.Forms.Control[]]@($label, $input, $ok, $cancel))
    $form.Add_Shown({ $input.Focus() })

    $plain = $null
    try {
        if ($form.ShowDialog() -ne [Windows.Forms.DialogResult]::OK) {
            throw "$Prompt input was cancelled; nothing was stored"
        }
        $plain = $input.Text
        if ($Required -and $plain.Length -eq 0) {
            throw "$Prompt is required"
        }
        if ($plain.Length -eq 0) {
            return [Security.SecureString]::new()
        }
        return ConvertTo-SecureString -String $plain -AsPlainText -Force
    } finally {
        $input.Clear()
        $plain = $null
        $form.Dispose()
    }
}

function Read-RequiredSecret([string]$Prompt) {
    return Read-SecretDialog -Prompt $Prompt -Required $true
}

function Read-OptionalSecret([string]$Prompt) {
    return Read-SecretDialog -Prompt $Prompt -Required $false
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
    BinanceUsdmApiPassword = Read-OptionalSecret "BINANCEUSDM API password (empty when unused)"
    BybitApiKey = Read-RequiredSecret "BYBIT API key"
    BybitApiSecret = Read-RequiredSecret "BYBIT API secret"
    BybitApiPassword = Read-OptionalSecret "BYBIT API password (empty when unused)"
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
