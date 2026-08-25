$ErrorActionPreference = "Stop"
Set-StrictMode -Version Latest

$env:IPEG_TELEGRAM_ENABLED = "false"
Remove-Item Env:IPEG_TELEGRAM_BOT_TOKEN -ErrorAction SilentlyContinue
