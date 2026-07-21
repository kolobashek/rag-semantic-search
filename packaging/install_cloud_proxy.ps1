[CmdletBinding()]
param(
    [string]$Domain = "cloud.tsk-nsk.ru",
    [string]$Upstream = "127.0.0.1:8080"
)

$ErrorActionPreference = "Stop"

if (-not ([Security.Principal.WindowsPrincipal] [Security.Principal.WindowsIdentity]::GetCurrent()).IsInRole(
    [Security.Principal.WindowsBuiltInRole]::Administrator
)) {
    throw "Запустите PowerShell от имени администратора."
}

$sourceConfig = Join-Path $PSScriptRoot "reverse_proxy\Caddyfile"
if (-not (Test-Path -LiteralPath $sourceConfig)) {
    throw "Не найден шаблон Caddyfile: $sourceConfig"
}

$caddyCommand = Get-Command caddy.exe -ErrorAction SilentlyContinue
if (-not $caddyCommand) {
    throw "Caddy не найден. Установите: winget install --id CaddyServer.Caddy --exact"
}

$installRoot = Join-Path $env:ProgramData "RAGCatalog\caddy"
$caddyExe = Join-Path $installRoot "caddy.exe"
$caddyConfig = Join-Path $installRoot "Caddyfile"
$taskName = "RAG Catalog HTTPS Proxy"

New-Item -ItemType Directory -Path $installRoot -Force | Out-Null
Copy-Item -LiteralPath $caddyCommand.Source -Destination $caddyExe -Force

$config = Get-Content -LiteralPath $sourceConfig -Raw
$config = $config.Replace("cloud.tsk-nsk.ru", $Domain).Replace("127.0.0.1:8080", $Upstream)
[IO.File]::WriteAllText($caddyConfig, $config, [Text.UTF8Encoding]::new($false))

& $caddyExe validate --config $caddyConfig --adapter caddyfile
if ($LASTEXITCODE -ne 0) {
    throw "Caddy отклонил конфигурацию."
}

$existingTask = Get-ScheduledTask -TaskName $taskName -ErrorAction SilentlyContinue
if ($existingTask) {
    Stop-ScheduledTask -TaskName $taskName -ErrorAction SilentlyContinue
    Unregister-ScheduledTask -TaskName $taskName -Confirm:$false
}

try {
    & $caddyCommand.Source stop | Out-Null
} catch {
    Write-Verbose "Активный пользовательский экземпляр Caddy не найден."
}

Get-Process caddy -ErrorAction SilentlyContinue | Where-Object {
    $_.Path -eq $caddyExe
} | Stop-Process -Force

$action = New-ScheduledTaskAction -Execute $caddyExe -Argument "run --config `"$caddyConfig`" --adapter caddyfile"
$trigger = New-ScheduledTaskTrigger -AtStartup
$principal = New-ScheduledTaskPrincipal -UserId "SYSTEM" -LogonType ServiceAccount -RunLevel Highest
$settings = New-ScheduledTaskSettingsSet `
    -AllowStartIfOnBatteries `
    -DontStopIfGoingOnBatteries `
    -ExecutionTimeLimit ([TimeSpan]::Zero) `
    -RestartCount 10 `
    -RestartInterval (New-TimeSpan -Minutes 1) `
    -StartWhenAvailable

Register-ScheduledTask `
    -TaskName $taskName `
    -Action $action `
    -Trigger $trigger `
    -Principal $principal `
    -Settings $settings `
    -Description "HTTPS reverse proxy for RAG Cloud Drive and semantic search" | Out-Null

foreach ($port in 80, 443) {
    $ruleName = "RAG Catalog HTTPS Proxy TCP $port"
    Remove-NetFirewallRule -DisplayName $ruleName -ErrorAction SilentlyContinue
    New-NetFirewallRule `
        -DisplayName $ruleName `
        -Direction Inbound `
        -Action Allow `
        -Protocol TCP `
        -LocalPort $port `
        -Program $caddyExe `
        -Profile Any | Out-Null
}

Start-ScheduledTask -TaskName $taskName
Start-Sleep -Seconds 2

$task = Get-ScheduledTask -TaskName $taskName
$process = Get-Process caddy -ErrorAction SilentlyContinue | Where-Object { $_.Path -eq $caddyExe }
if (-not $process) {
    throw "Caddy не запустился. Проверьте журнал событий Task Scheduler."
}

Write-Host "Caddy запущен: https://$Domain -> http://$Upstream"
Write-Host "Scheduled task: $($task.State)"
Write-Host "Config: $caddyConfig"
