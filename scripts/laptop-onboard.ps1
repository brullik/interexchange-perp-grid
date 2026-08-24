[CmdletBinding()]
param(
    [string]$OutputPath = "state/laptop-profile.clixml",
    [switch]$RuntimeSelfTest,
    [switch]$DialogSelfTest
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

function Convert-PlainTextToSecureString([string]$Value) {
    $secure = [Security.SecureString]::new()
    for ($index = 0; $index -lt $Value.Length; $index++) {
        $secure.AppendChar($Value[$index])
    }
    $secure.MakeReadOnly()
    return $secure
}

if ($RuntimeSelfTest) {
    $samples = @(
        '123456789:AA_ab-CD',
        ' !@#$%^&*()_+-=[]{};:",./<>?\|`~',
        'пароль 漢字 😀'
    )
    foreach ($sample in $samples) {
        $secure = Convert-PlainTextToSecureString -Value $sample
        $deserializedSecure = $null
        $pointer = [Runtime.InteropServices.Marshal]::SecureStringToBSTR($secure)
        try {
            $roundTrip = [Runtime.InteropServices.Marshal]::PtrToStringBSTR($pointer)
            if ($roundTrip -cne $sample) {
                throw "SecureString runtime round-trip failed"
            }

            $serializedSample = [Management.Automation.PSSerializer]::Serialize(
                [pscustomobject]@{ Secret = $secure }
            )
            $deserialized = [Management.Automation.PSSerializer]::Deserialize(
                $serializedSample
            )
            $deserializedSecure = $deserialized.Secret
            $deserializedPointer = [Runtime.InteropServices.Marshal]::SecureStringToBSTR(
                $deserializedSecure
            )
            try {
                $deserializedRoundTrip = [Runtime.InteropServices.Marshal]::PtrToStringBSTR(
                    $deserializedPointer
                )
                if ($deserializedRoundTrip -cne $sample) {
                    throw "DPAPI serializer round-trip failed"
                }
            } finally {
                [Runtime.InteropServices.Marshal]::ZeroFreeBSTR($deserializedPointer)
                $deserializedRoundTrip = $null
            }
        } finally {
            [Runtime.InteropServices.Marshal]::ZeroFreeBSTR($pointer)
            $roundTrip = $null
            $serializedSample = $null
            if ($null -ne $deserializedSecure) {
                $deserializedSecure.Dispose()
            }
            $secure.Dispose()
        }
    }
    Write-Host "SecureString and DPAPI serializer round-trip PASS"
    return
}

function Read-SecretDialog([string]$Prompt, [bool]$Required) {
    $form = [Windows.Forms.Form]::new()
    $form.Name = "IpegSecretDialog"
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

    $secretTextBox = [Windows.Forms.TextBox]::new()
    $secretTextBox.Name = "SecretTextBox"
    $secretTextBox.Location = [Drawing.Point]::new(16, 46)
    $secretTextBox.Size = [Drawing.Size]::new(588, 27)
    $secretTextBox.UseSystemPasswordChar = $true
    $secretTextBox.ShortcutsEnabled = $true

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
    $form.Controls.AddRange(
        [Windows.Forms.Control[]]@($label, $secretTextBox, $ok, $cancel)
    )
    $form.ActiveControl = $secretTextBox

    $plain = $null
    try {
        if ($form.ShowDialog() -ne [Windows.Forms.DialogResult]::OK) {
            throw "$Prompt input was cancelled; nothing was stored"
        }
        $plain = $secretTextBox.Text
        if ($Required -and $plain.Length -eq 0) {
            throw "$Prompt is required"
        }
        if ($plain.Length -eq 0) {
            return [Security.SecureString]::new()
        }
        return Convert-PlainTextToSecureString -Value $plain
    } finally {
        $secretTextBox.Clear()
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

if ($DialogSelfTest) {
    $timer = [Windows.Forms.Timer]::new()
    $timer.Interval = 50
    $timer.Add_Tick({
        $dialog = [Windows.Forms.Application]::OpenForms["IpegSecretDialog"]
        if ($null -eq $dialog) {
            return
        }
        $field = $dialog.Controls["SecretTextBox"]
        if ($null -eq $field) {
            return
        }
        $field.Text = '123456789:AA_ab-CD/+= пароль 漢字 😀'
        $dialog.DialogResult = [Windows.Forms.DialogResult]::OK
    })
    $timer.Start()
    $dialogSecure = $null
    try {
        $dialogSecure = Read-SecretDialog -Prompt "Dialog runtime self-test" -Required $true
        $dialogPointer = [Runtime.InteropServices.Marshal]::SecureStringToBSTR($dialogSecure)
        try {
            $dialogRoundTrip = [Runtime.InteropServices.Marshal]::PtrToStringBSTR(
                $dialogPointer
            )
            if ($dialogRoundTrip -cne '123456789:AA_ab-CD/+= пароль 漢字 😀') {
                throw "Secret dialog runtime round-trip failed"
            }
        } finally {
            [Runtime.InteropServices.Marshal]::ZeroFreeBSTR($dialogPointer)
            $dialogRoundTrip = $null
        }
    } finally {
        $timer.Stop()
        $timer.Dispose()
        if ($null -ne $dialogSecure) {
            $dialogSecure.Dispose()
        }
    }
    Write-Host "Secret dialog runtime round-trip PASS"
    return
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
    $serialized = [Management.Automation.PSSerializer]::Serialize($payload)
    [IO.File]::WriteAllText(
        $temporary,
        $serialized,
        [Text.UTF8Encoding]::new($false)
    )
    if ([IO.File]::Exists($resolved)) {
        [IO.File]::Replace($temporary, $resolved, $null)
    } else {
        [IO.File]::Move($temporary, $resolved)
    }
    $owner = [Security.Principal.WindowsIdentity]::GetCurrent().Name
    & icacls.exe $resolved /inheritance:r /grant:r "${owner}:(F)" | Out-Null
} finally {
    if ([IO.File]::Exists($temporary)) {
        [IO.File]::Delete($temporary)
    }
}

Write-Host "Encrypted current-user DPAPI credentials stored outside Git: $resolved"
Write-Host "Live remains disabled. Do not copy this file to another account or machine."
