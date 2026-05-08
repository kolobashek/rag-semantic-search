#!/usr/bin/env pwsh
<#
.SYNOPSIS
    Build RAGSyncClientSetup.exe

.DESCRIPTION
    1. Runs PyInstaller to produce rag_sync_client.exe
    2. Runs Inno Setup compiler to wrap it in an installer

.PARAMETER InnoSetupPath
    Path to iscc.exe. Defaults to the standard Inno Setup 6 install location.

.PARAMETER SkipPyInstaller
    Skip PyInstaller step (use existing dist\rag_sync_client.exe).

.EXAMPLE
    .\build.ps1
    .\build.ps1 -InnoSetupPath "C:\Tools\innosetup\iscc.exe"
    .\build.ps1 -SkipPyInstaller
#>

param(
    [string]$InnoSetupPath = "C:\Program Files (x86)\Inno Setup 6\iscc.exe",
    [switch]$SkipPyInstaller
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$PackagingDir = $PSScriptRoot
$ProjectDir   = Split-Path -Parent $PackagingDir

Write-Host "=== RAG Sync Client — build installer ===" -ForegroundColor Cyan
Write-Host "Project : $ProjectDir"
Write-Host "Output  : $PackagingDir\dist\RAGSyncClientSetup.exe"
Write-Host ""

# ── Step 1: PyInstaller ──────────────────────────────────────────────────────
if (-not $SkipPyInstaller) {
    Write-Host "Step 1/2 — PyInstaller" -ForegroundColor Yellow

    $specFile = Join-Path $PackagingDir "rag_sync_client.spec"
    if (-not (Test-Path $specFile)) {
        Write-Error "Spec file not found: $specFile"
        exit 1
    }

    Push-Location $ProjectDir
    try {
        pyinstaller $specFile `
            --distpath "$PackagingDir\dist" `
            --workpath "$PackagingDir\build" `
            --noconfirm
        if ($LASTEXITCODE -ne 0) {
            Write-Error "PyInstaller failed (exit code $LASTEXITCODE)"
            exit 1
        }
    } finally {
        Pop-Location
    }

    $exePath = Join-Path $PackagingDir "dist\rag_sync_client.exe"
    if (-not (Test-Path $exePath)) {
        Write-Error "Expected exe not found: $exePath"
        exit 1
    }

    $sizeMB = [math]::Round((Get-Item $exePath).Length / 1MB, 1)
    Write-Host "  Built: $exePath ($sizeMB MB)" -ForegroundColor Green
} else {
    Write-Host "Step 1/2 — PyInstaller skipped" -ForegroundColor DarkGray
}

# ── Step 2: Inno Setup ───────────────────────────────────────────────────────
Write-Host ""
Write-Host "Step 2/2 — Inno Setup" -ForegroundColor Yellow

if (-not (Test-Path $InnoSetupPath)) {
    # Try to find iscc.exe in PATH
    $found = Get-Command iscc -ErrorAction SilentlyContinue
    if ($found) {
        $InnoSetupPath = $found.Source
    } else {
        Write-Warning "Inno Setup compiler not found at: $InnoSetupPath"
        Write-Warning "Install Inno Setup 6 from https://jrsoftware.org/isinfo.php"
        Write-Warning "or pass -InnoSetupPath to this script."
        Write-Host ""
        Write-Host "Standalone exe is available at:" -ForegroundColor Cyan
        Write-Host "  $PackagingDir\dist\rag_sync_client.exe"
        exit 0
    }
}

$issFile = Join-Path $PackagingDir "installer.iss"
Push-Location $PackagingDir
try {
    & $InnoSetupPath $issFile
    if ($LASTEXITCODE -ne 0) {
        Write-Error "Inno Setup failed (exit code $LASTEXITCODE)"
        exit 1
    }
} finally {
    Pop-Location
}

$installerPath = Join-Path $PackagingDir "dist\RAGSyncClientSetup.exe"
if (Test-Path $installerPath) {
    $sizeMB = [math]::Round((Get-Item $installerPath).Length / 1MB, 1)
    Write-Host ""
    Write-Host "=== Done ===" -ForegroundColor Green
    Write-Host "Installer : $installerPath ($sizeMB MB)" -ForegroundColor Green
} else {
    Write-Error "Installer not found after build: $installerPath"
    exit 1
}
