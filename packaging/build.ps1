#!/usr/bin/env pwsh
<#
.SYNOPSIS
    Build RAG Sync Client — exe installer (Inno Setup) + MSI (WiX 4)

.DESCRIPTION
    Step 1: PyInstaller → rag_sync_client.exe
    Step 2: Inno Setup  → dist/RAGSyncClientSetup.exe  (wizard installer)
    Step 3: WiX 4       → dist/RAGSyncClient.msi       (silent/enterprise MSI)

.PARAMETER InnoSetupPath
    Path to iscc.exe. Default: standard Inno Setup 6 location.

.PARAMETER SkipPyInstaller
    Skip PyInstaller (use existing dist\rag_sync_client.exe).

.PARAMETER SkipInno
    Skip Inno Setup step.

.PARAMETER SkipWix
    Skip WiX MSI step.

.EXAMPLE
    .\build.ps1
    .\build.ps1 -SkipPyInstaller          # rebuild installers only
    .\build.ps1 -SkipInno                 # PyInstaller + WiX only
    msiexec /i dist\RAGSyncClient.msi RAGSERVER=http://host:8080 RAGTOKEN=abc123
#>

param(
    [string]$InnoSetupPath = "C:\Program Files (x86)\Inno Setup 6\iscc.exe",
    [switch]$SkipPyInstaller,
    [switch]$SkipInno,
    [switch]$SkipWix
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$PackagingDir = $PSScriptRoot
$ProjectDir   = Split-Path -Parent $PackagingDir
$DistDir      = Join-Path $PackagingDir "dist"

Write-Host "=== RAG Sync Client — build ===" -ForegroundColor Cyan
Write-Host "Project   : $ProjectDir"
Write-Host "Output    : $DistDir"
Write-Host ""

# ── Resolve python executable ─────────────────────────────────────────────────
$PythonExe = $null
foreach ($candidate in @("python", "python3", "py")) {
    if (Get-Command $candidate -ErrorAction SilentlyContinue) {
        $PythonExe = $candidate; break
    }
}
if (-not $PythonExe) {
    Write-Error "Python not found. Install Python 3.10+ and add it to PATH."
    exit 1
}
Write-Host "Python    : $(& $PythonExe --version 2>&1)" -ForegroundColor DarkGray

# ── Step 1: PyInstaller ──────────────────────────────────────────────────────
if (-not $SkipPyInstaller) {
    Write-Host "Step 1/3 — PyInstaller" -ForegroundColor Yellow

    $specFile = Join-Path $PackagingDir "rag_sync_client.spec"
    if (-not (Test-Path $specFile)) { Write-Error "Spec not found: $specFile"; exit 1 }

    # Ensure PyInstaller is installed
    $piCheck = & $PythonExe -m PyInstaller --version 2>&1
    if ($LASTEXITCODE -ne 0) {
        Write-Host "  PyInstaller not found — installing..." -ForegroundColor DarkYellow
        & $PythonExe -m pip install pyinstaller --quiet
        if ($LASTEXITCODE -ne 0) { Write-Error "Failed to install PyInstaller"; exit 1 }
    }

    # Ensure watchdog is installed (required by the client)
    & $PythonExe -m pip install requests watchdog --quiet

    Push-Location $ProjectDir
    try {
        & $PythonExe -m PyInstaller $specFile `
            --distpath $DistDir `
            --workpath (Join-Path $PackagingDir "build") `
            --noconfirm
        if ($LASTEXITCODE -ne 0) { Write-Error "PyInstaller failed"; exit 1 }
    } finally { Pop-Location }

    $exePath = Join-Path $DistDir "rag_sync_client.exe"
    if (-not (Test-Path $exePath)) { Write-Error "Exe not found: $exePath"; exit 1 }
    $sizeMB = [math]::Round((Get-Item $exePath).Length / 1MB, 1)
    Write-Host "  OK: rag_sync_client.exe ($sizeMB MB)" -ForegroundColor Green
} else {
    Write-Host "Step 1/3 — PyInstaller skipped" -ForegroundColor DarkGray
}

$exePath = Join-Path $DistDir "rag_sync_client.exe"
if (-not (Test-Path $exePath)) {
    Write-Error "rag_sync_client.exe not found in $DistDir. Run without -SkipPyInstaller first."
    exit 1
}

# ── Step 2: Inno Setup (.exe wizard installer) ───────────────────────────────
if (-not $SkipInno) {
    Write-Host ""
    Write-Host "Step 2/3 — Inno Setup (.exe installer)" -ForegroundColor Yellow

    if (-not (Test-Path $InnoSetupPath)) {
        $found = Get-Command iscc -ErrorAction SilentlyContinue
        if ($found) { $InnoSetupPath = $found.Source }
    }

    if (Test-Path $InnoSetupPath) {
        Push-Location $PackagingDir
        try {
            & $InnoSetupPath "installer.iss"
            if ($LASTEXITCODE -ne 0) { Write-Error "Inno Setup failed"; exit 1 }
        } finally { Pop-Location }

        $innoOut = Join-Path $DistDir "RAGSyncClientSetup.exe"
        if (Test-Path $innoOut) {
            $sizeMB = [math]::Round((Get-Item $innoOut).Length / 1MB, 1)
            Write-Host "  OK: RAGSyncClientSetup.exe ($sizeMB MB)" -ForegroundColor Green
        }
    } else {
        Write-Warning "Inno Setup not found. Download from https://jrsoftware.org/isinfo.php"
        Write-Warning "Skipping EXE installer."
    }
} else {
    Write-Host "Step 2/3 — Inno Setup skipped" -ForegroundColor DarkGray
}

# ── Step 3: WiX 4 (.msi enterprise installer) ────────────────────────────────
if (-not $SkipWix) {
    Write-Host ""
    Write-Host "Step 3/3 — WiX 4 (.msi installer)" -ForegroundColor Yellow

    $wixCmd = Get-Command wix -ErrorAction SilentlyContinue
    if (-not $wixCmd) {
        Write-Warning "WiX 4 not found. Install with:"
        Write-Warning "  dotnet tool install --global wix"
        Write-Warning "  wix extension add WixToolset.UI.wixext"
        Write-Warning "Skipping MSI."
    } else {
        Push-Location $PackagingDir
        try {
            wix build installer.wxs `
                -ext WixToolset.UI.wixext `
                -o "$DistDir\RAGSyncClient.msi"
            if ($LASTEXITCODE -ne 0) { Write-Error "WiX build failed"; exit 1 }
        } finally { Pop-Location }

        $msiPath = Join-Path $DistDir "RAGSyncClient.msi"
        if (Test-Path $msiPath) {
            $sizeMB = [math]::Round((Get-Item $msiPath).Length / 1MB, 1)
            Write-Host "  OK: RAGSyncClient.msi ($sizeMB MB)" -ForegroundColor Green
        }
    }
} else {
    Write-Host "Step 3/3 — WiX skipped" -ForegroundColor DarkGray
}

Write-Host ""
Write-Host "=== Build complete ===" -ForegroundColor Green
Write-Host ""
Write-Host "Files in $DistDir :" -ForegroundColor Cyan
Get-ChildItem $DistDir -Filter "RAGSync*" | ForEach-Object {
    $mb = [math]::Round($_.Length / 1MB, 1)
    Write-Host "  $($_.Name) — $mb MB"
}
Write-Host ""
Write-Host "Silent MSI install example:" -ForegroundColor Cyan
Write-Host '  msiexec /i RAGSyncClient.msi /quiet RAGSERVER=http://host:8080 RAGTOKEN=abc123'
