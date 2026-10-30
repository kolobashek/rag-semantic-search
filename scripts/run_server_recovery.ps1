[CmdletBinding()]
param([Parameter(Mandatory = $true)][string]$Python)

$ErrorActionPreference = 'Stop'
$root = Split-Path -Parent $PSScriptRoot
Set-Location -LiteralPath $root
$env:PYTHONPATH = Join-Path $root 'src'
$env:PYTHONIOENCODING = 'utf-8'
$env:HF_HUB_OFFLINE = '1'
$env:TRANSFORMERS_OFFLINE = '1'
$env:HF_HUB_DISABLE_TELEMETRY = '1'
& $Python -m rag_catalog.cli.launcher recover --host 127.0.0.1 --port 8080 --proxy-task 'RAG Catalog HTTPS Proxy' | Out-Null
exit $LASTEXITCODE
