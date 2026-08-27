[CmdletBinding()]
param(
    [string]$OutputPath = "state/laptop-profile.clixml",
    [switch]$RuntimeSelfTest,
    [switch]$DialogSelfTest,
    [switch]$UnlockVerifierSelfTest,
    [switch]$AtomicReplaceSelfTest
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

function Remove-FileWithRetry([string]$Path) {
    for ($attempt = 0; $attempt -lt 10; $attempt++) {
        if (-not [IO.File]::Exists($Path)) { return }
        try {
            [IO.File]::Delete($Path)
            return
        } catch [IO.IOException] {
            if ($attempt -eq 9) { throw }
            Start-Sleep -Milliseconds 50
        }
    }
}

function Remove-EmptyDirectoryWithRetry([string]$Path) {
    for ($attempt = 0; $attempt -lt 10; $attempt++) {
        if (-not [IO.Directory]::Exists($Path)) { return }
        try {
            [IO.Directory]::Delete($Path)
            return
        } catch [IO.IOException] {
            if ($attempt -eq 9) { throw }
            Start-Sleep -Milliseconds 50
        }
    }
}

function Set-OwnerOnlyAcl([string]$Path) {
    $identity = [Security.Principal.WindowsIdentity]::GetCurrent()
    $security = [Security.AccessControl.FileSecurity]::new()
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
        [Security.AccessControl.AccessControlSections]::Access -bor
            [Security.AccessControl.AccessControlSections]::Owner
    )
    $rules = @($verified.GetAccessRules(
        $true,
        $false,
        [Security.Principal.SecurityIdentifier]
    ))
    if (
        $verified.GetOwner([Security.Principal.SecurityIdentifier]) -ne $identity.User -or
        -not $verified.AreAccessRulesProtected -or
        $rules.Count -ne 1 -or
        $rules[0].IdentityReference -ne $identity.User -or
        $rules[0].AccessControlType -ne [Security.AccessControl.AccessControlType]::Allow -or
        ($rules[0].FileSystemRights -band [Security.AccessControl.FileSystemRights]::FullControl) -ne
            [Security.AccessControl.FileSystemRights]::FullControl
    ) {
        throw "Credential profile ACL is not restricted to the current Windows identity"
    }
}

function Write-AtomicUtf8File(
    [string]$Destination,
    [string]$Value,
    [scriptblock]$AclHardener
) {
    $parent = Split-Path -Parent $Destination
    New-Item -ItemType Directory -Path $parent -Force | Out-Null
    $temporary = "$Destination.tmp"
    $backup = "$Destination.bak"
    $discard = "$Destination.discard"
    $hadDestination = [IO.File]::Exists($Destination)
    $originalAcl = if ($hadDestination) {
        [IO.File]::GetAccessControl(
            $Destination,
            [Security.AccessControl.AccessControlSections]::Access
        )
    } else { $null }
    $installed = $false
    $transactionClosed = $false
    if ([IO.File]::Exists($backup) -or [IO.File]::Exists($discard)) {
        throw "Previous credential profile transaction requires fail-closed recovery"
    }
    try {
        [IO.File]::WriteAllText(
            $temporary,
            $Value,
            [Text.UTF8Encoding]::new($false)
        )
        if ($hadDestination) {
            # Windows PowerShell 5.1 rejects a null backup path in this overload.
            # Retain a real same-directory backup until ACL verification succeeds.
            [IO.File]::Replace($temporary, $Destination, $backup)
        } else {
            [IO.File]::Move($temporary, $Destination)
        }
        $installed = $true
        & $AclHardener $Destination
        Remove-FileWithRetry -Path $backup
        $transactionClosed = $true
    } catch {
        $failure = $_.Exception
        if ($installed) {
            try {
                if ($hadDestination) {
                    if (-not [IO.File]::Exists($backup)) {
                        throw "Credential profile rollback backup is missing"
                    }
                    [IO.File]::Replace($backup, $Destination, $discard)
                    if ($null -ne $originalAcl) {
                        [IO.File]::SetAccessControl($Destination, $originalAcl)
                    }
                    Remove-FileWithRetry -Path $discard
                } else {
                    Remove-FileWithRetry -Path $Destination
                }
                $transactionClosed = $true
            } catch {
                throw "Credential profile update failed and rollback failed closed: $($_.Exception.Message)"
            }
        }
        throw $failure
    } finally {
        Remove-FileWithRetry -Path $temporary
        if ($transactionClosed) {
            Remove-FileWithRetry -Path $backup
            Remove-FileWithRetry -Path $discard
        }
    }
}

if ($AtomicReplaceSelfTest) {
    $testRoot = Join-Path ([IO.Path]::GetTempPath()) (
        "ipeg-onboard-atomic-" + [Guid]::NewGuid().ToString("N")
    )
    $testPath = Join-Path $testRoot "profile.clixml"
    $realAclHardener = { param([string]$Path) Set-OwnerOnlyAcl -Path $Path }
    $failingAclHardener = { param([string]$Path) throw "injected ACL hardening failure" }
    try {
        Write-AtomicUtf8File `
            -Destination $testPath -Value "first" -AclHardener $realAclHardener
        try {
            Write-AtomicUtf8File `
                -Destination $testPath -Value "must-rollback" `
                -AclHardener $failingAclHardener
            throw "Existing-profile ACL failure did not fail closed"
        } catch {
            if ($_.Exception.Message -notlike "*injected ACL hardening failure*") { throw }
        }
        if ([IO.File]::ReadAllText($testPath) -cne "first") {
            throw "Existing-profile ACL failure did not restore the old profile"
        }
        Remove-FileWithRetry -Path $testPath
        try {
            Write-AtomicUtf8File `
                -Destination $testPath -Value "must-delete" `
                -AclHardener $failingAclHardener
            throw "New-profile ACL failure did not fail closed"
        } catch {
            if ($_.Exception.Message -notlike "*injected ACL hardening failure*") { throw }
        }
        if ([IO.File]::Exists($testPath)) {
            throw "New profile survived failed ACL hardening"
        }
        Write-AtomicUtf8File `
            -Destination $testPath -Value "first" -AclHardener $realAclHardener
        Write-AtomicUtf8File `
            -Destination $testPath -Value "second" -AclHardener $realAclHardener
        if ([IO.File]::ReadAllText($testPath) -cne "second") {
            throw "Atomic existing-profile replacement self-test failed"
        }
        if (
            [IO.File]::Exists("$testPath.tmp") -or
            [IO.File]::Exists("$testPath.bak") -or
            [IO.File]::Exists("$testPath.discard")
        ) {
            throw "Atomic existing-profile replacement left temporary material"
        }
    } finally {
        Remove-FileWithRetry -Path $testPath
        Remove-EmptyDirectoryWithRetry -Path $testRoot
    }
    Write-Host "Atomic existing-profile replacement PASS"
    return
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

function New-UnlockVerifierFromPlain([string]$Value) {
    $scalarCount = 0
    for ($index = 0; $index -lt $Value.Length; $index++) {
        if (
            [char]::IsHighSurrogate($Value[$index]) -and
            $index + 1 -lt $Value.Length -and
            [char]::IsLowSurrogate($Value[$index + 1])
        ) {
            $index++
        }
        $scalarCount++
    }
    if ($scalarCount -lt 16) {
        throw "Local live unlock secret must contain at least 16 characters"
    }
    $salt = [byte[]]::new(32)
    $derived = $null
    $kdf = $null
    try {
        [Security.Cryptography.RandomNumberGenerator]::Create().GetBytes($salt)
        $kdf = [Security.Cryptography.Rfc2898DeriveBytes]::new(
            $Value,
            $salt,
            600000,
            [Security.Cryptography.HashAlgorithmName]::SHA256
        )
        $derived = $kdf.GetBytes(32)
        return "pbkdf2-sha256`$600000`${0}`${1}" -f @(
            [Convert]::ToBase64String($salt),
            [Convert]::ToBase64String($derived)
        )
    } finally {
        if ($null -ne $derived) { [Array]::Clear($derived, 0, $derived.Length) }
        [Array]::Clear($salt, 0, $salt.Length)
        if ($null -ne $kdf) { $kdf.Dispose() }
    }
}

function New-LiveUnlockVerifier {
    $first = $null
    $second = $null
    $firstPointer = [IntPtr]::Zero
    $secondPointer = [IntPtr]::Zero
    try {
        $first = Read-RequiredSecret "Create local live unlock secret (minimum 16 characters)"
        $second = Read-RequiredSecret "Repeat local live unlock secret"
        $firstPointer = [Runtime.InteropServices.Marshal]::SecureStringToBSTR($first)
        $secondPointer = [Runtime.InteropServices.Marshal]::SecureStringToBSTR($second)
        $firstPlain = [Runtime.InteropServices.Marshal]::PtrToStringBSTR($firstPointer)
        $secondPlain = [Runtime.InteropServices.Marshal]::PtrToStringBSTR($secondPointer)
        if ($firstPlain -cne $secondPlain) {
            throw "Local live unlock secrets do not match"
        }
        return New-UnlockVerifierFromPlain -Value $firstPlain
    } finally {
        if ($firstPointer -ne [IntPtr]::Zero) {
            [Runtime.InteropServices.Marshal]::ZeroFreeBSTR($firstPointer)
        }
        if ($secondPointer -ne [IntPtr]::Zero) {
            [Runtime.InteropServices.Marshal]::ZeroFreeBSTR($secondPointer)
        }
        $firstPlain = $null
        $secondPlain = $null
        if ($null -ne $first) { $first.Dispose() }
        if ($null -ne $second) { $second.Dispose() }
    }
}

if ($UnlockVerifierSelfTest) {
    $verifier = New-UnlockVerifierFromPlain -Value "correct horse battery staple"
    if ($verifier -notmatch '^pbkdf2-sha256\$600000\$[A-Za-z0-9+/]+={0,2}\$[A-Za-z0-9+/]+={0,2}$') {
        throw "Local live unlock verifier self-test failed"
    }
    try {
        $supplementaryCharacter = [char]::ConvertFromUtf32(0x1F600)
        $null = New-UnlockVerifierFromPlain -Value ($supplementaryCharacter * 8)
        throw "Unicode scalar-length self-test failed"
    } catch {
        if ($_.Exception.Message -notlike "*at least 16 characters*") { throw }
    }
    Write-Host "Local live unlock verifier self-test PASS"
    return
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
Confirm-Fact "Confirm all three subaccounts are funded for minimum-notional canary and recovery."

$chatId = Read-Host "Telegram numeric owner chat ID"
if ($chatId -notmatch '^-?[0-9]+$') {
    throw "Telegram owner chat ID must be numeric"
}

$payload = [pscustomobject]@{
    SchemaVersion = 2
    FastLiveRoute = "BTC:bybit>okx"
    LocalLiveUnlockVerifier = New-LiveUnlockVerifier
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
$serialized = $null
try {
    $serialized = [Management.Automation.PSSerializer]::Serialize($payload)
    $aclHardener = { param([string]$Path) Set-OwnerOnlyAcl -Path $Path }
    Write-AtomicUtf8File `
        -Destination $resolved -Value $serialized -AclHardener $aclHardener
} finally {
    $serialized = $null
}

Write-Host "Encrypted current-user DPAPI credentials stored outside Git: $resolved"
Write-Host "Live remains disabled. Do not copy this file to another account or machine."
