param(
    [string]$Server = "",
    [string]$SourceExe = (Join-Path $PSScriptRoot "dist\RagCloudFiles.exe"),
    [string]$RootPath = (Join-Path $env:USERPROFILE "RAG Cloud Drive"),
    [switch]$Uninstall,
    [switch]$NoStart
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$InstallDir = Join-Path $env:LOCALAPPDATA "RAG Cloud Files"
$InstalledExe = Join-Path $InstallDir "RagCloudFiles.exe"
$RegistryPath = "HKCU:\Software\RAGCloudFiles"
$RunPath = "HKCU:\Software\Microsoft\Windows\CurrentVersion\Run"

function Stop-InstalledProvider {
    Get-Process -Name "RagCloudFiles" -ErrorAction SilentlyContinue | ForEach-Object {
        try {
            if ([string]::Equals($_.Path, $InstalledExe, [StringComparison]::OrdinalIgnoreCase)) {
                Stop-Process -Id $_.Id -Force
                Wait-Process -Id $_.Id -Timeout 10 -ErrorAction SilentlyContinue
            }
        } catch {
            Write-Warning "Не удалось остановить provider PID $($_.Id): $_"
        }
    }
}

if ($Uninstall) {
    Stop-InstalledProvider
    if (Test-Path -LiteralPath $InstalledExe) {
        & $InstalledExe --root $RootPath --unregister
    }
    Remove-ItemProperty -Path $RunPath -Name "RAGCloudFiles" -ErrorAction SilentlyContinue
    Remove-Item -LiteralPath $InstalledExe -Force -ErrorAction SilentlyContinue
    Write-Host "RAG Cloud Files удалён. Гидратированные локальные файлы сохранены в $RootPath."
    exit 0
}

if (-not $Server.Trim()) {
    throw "Для установки укажите -Server https://catalog.example.org"
}

if (-not (Test-Path -LiteralPath $SourceExe)) {
    throw "Provider не найден: $SourceExe"
}

Stop-InstalledProvider
New-Item -ItemType Directory -Path $InstallDir -Force | Out-Null
Copy-Item -LiteralPath $SourceExe -Destination $InstalledExe -Force
New-Item -Path $RegistryPath -Force | Out-Null
$existingRegistry = Get-ItemProperty -Path $RegistryPath -Name DeviceId -ErrorAction SilentlyContinue
$existingDeviceId = if ($existingRegistry) { [string]$existingRegistry.DeviceId } else { "" }
if (-not $existingDeviceId) {
    $existingDeviceId = "win-$env:COMPUTERNAME-$([guid]::NewGuid().ToString('N'))"
}
Set-ItemProperty -Path $RegistryPath -Name Server -Value $Server.TrimEnd('/')
Set-ItemProperty -Path $RegistryPath -Name RootPath -Value $RootPath
Set-ItemProperty -Path $RegistryPath -Name DeviceId -Value $existingDeviceId

$escapedExe = $InstalledExe.Replace("'", "''")
$runCommand = "powershell.exe -NoProfile -WindowStyle Hidden -Command `"& '$escapedExe'`""
Set-ItemProperty -Path $RunPath -Name "RAGCloudFiles" -Value $runCommand

if (-not $NoStart) {
    Start-Process -FilePath $InstalledExe -WindowStyle Hidden
}
Write-Host "RAG Cloud Files установлен для $env:USERNAME. Корень: $RootPath"
