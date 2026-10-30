[CmdletBinding()]
param(
    [string]$Python = 'C:\Python314\python.exe',
    [switch]$Unattended,
    [switch]$PromptForCredential,
    [PSCredential]$Credential
)

$ErrorActionPreference = 'Stop'
$root = Split-Path -Parent $PSScriptRoot
$resultPath = Join-Path $root 'runtime\autostart-install-result.json'
$stage = 'preflight'
trap {
    $failure = @{success=$false; at=(Get-Date).ToString('o'); stage=$stage;
        errorType=$_.Exception.GetType().Name; hresult=$_.Exception.HResult; errorId=$_.FullyQualifiedErrorId}
    $failure | ConvertTo-Json | Set-Content -LiteralPath $resultPath -Encoding UTF8
    if ($PromptForCredential) {
        Add-Type -AssemblyName System.Windows.Forms
        [System.Windows.Forms.MessageBox]::Show(('RAG autostart setup failed: ' + $_.Exception.Message), 'RAG autostart') | Out-Null
    }
    exit 1
}
$identity = [Security.Principal.WindowsIdentity]::GetCurrent()
$user = $identity.Name
$elevated = ([Security.Principal.WindowsPrincipal]$identity).IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)
if (-not $elevated) {
    throw 'Run this script in an elevated Windows PowerShell to update the server tasks.'
}
if (-not (Test-Path -LiteralPath $Python -PathType Leaf)) { throw 'Python executable not found.' }
$docker = Join-Path $env:ProgramFiles 'Docker\Docker\Docker Desktop.exe'
if (-not (Test-Path -LiteralPath $docker -PathType Leaf)) { throw 'Docker Desktop executable not found.' }
$proxy = Get-ScheduledTask -TaskName 'RAG Catalog HTTPS Proxy' -ErrorAction Stop
if ($proxy.Actions.Count -ne 1) { throw 'Expected one existing HTTPS proxy action.' }
$env:PYTHONPATH = Join-Path $root 'src'
& $Python -c 'from rag_catalog.cli.launcher import load_config, _search_runtime_ready; raise SystemExit(0 if _search_runtime_ready(load_config()) else 1)'
if ($LASTEXITCODE -ne 0) { throw 'The selected Python cannot load the configured search runtime.' }

if ($Unattended) {
    $stage = 'credential'
    if ($PromptForCredential -and -not $Credential) {
        # Windows PowerShell 5.1 opens a local credential dialog; never log its value.
        $Credential = Get-Credential -UserName $user -Message 'RAG autostart: Windows account password (not PIN). Stored only by Windows Task Scheduler.'
    }
    if (-not $Credential) { throw 'Unattended startup needs a Windows credential; do not send it in chat.' }
    $credentialSid = ([Security.Principal.NTAccount]$Credential.UserName).Translate([Security.Principal.SecurityIdentifier]).Value
    if ($credentialSid -ne $identity.User.Value) { throw 'Use the current server account so Docker, model cache and SMB credentials remain available.' }
}

$backupRoot = Join-Path $root ('runtime\autostart-backups\' + (Get-Date -Format 'yyyyMMdd-HHmmss'))
$stage = 'task-definitions'
New-Item -ItemType Directory -Path $backupRoot -Force | Out-Null
foreach ($name in 'RAG Catalog Server', 'RAG Docker Desktop', 'RAG Catalog HTTPS Proxy') {
    if (Get-ScheduledTask -TaskName $name -ErrorAction SilentlyContinue) {
        Export-ScheduledTask -TaskName $name | Set-Content -LiteralPath (Join-Path $backupRoot ($name + '.xml')) -Encoding Unicode
    }
}

$settings = New-ScheduledTaskSettingsSet -MultipleInstances IgnoreNew -StartWhenAvailable `
    -AllowStartIfOnBatteries -DontStopIfGoingOnBatteries -ExecutionTimeLimit ([TimeSpan]::Zero) `
    -RestartCount 3 -RestartInterval (New-TimeSpan -Minutes 1)
$logon = New-ScheduledTaskTrigger -AtLogOn -User $user
$repeat = New-ScheduledTaskTrigger -Once -At (Get-Date).AddMinutes(1) -RepetitionInterval (New-TimeSpan -Minutes 1)
$serverTriggers = @($logon, $repeat)
$dockerTriggers = @($logon)
# Keep a separate safety net if the launcher itself cannot start. IgnoreNew
# suppresses repeated triggers while the long-lived proxy is already running.
$proxyTriggers = @($logon, $repeat)
if ($Unattended) {
    $boot = New-ScheduledTaskTrigger -AtStartup
    $boot.Delay = 'PT30S'
    $serverTriggers += $boot
    $dockerBoot = New-ScheduledTaskTrigger -AtStartup
    $dockerBoot.Delay = 'PT10S'
    $dockerTriggers += $dockerBoot
    $proxyTriggers += (New-ScheduledTaskTrigger -AtStartup)
}
$shell = Join-Path $env:SystemRoot 'System32\WindowsPowerShell\v1.0\powershell.exe'
$runner = Join-Path $PSScriptRoot 'run_server_recovery.ps1'
$serverAction = New-ScheduledTaskAction -Execute $shell -WorkingDirectory $root `
    -Argument "-NoProfile -NonInteractive -WindowStyle Hidden -ExecutionPolicy Bypass -File `"$runner`" -Python `"$Python`""
$dockerAction = New-ScheduledTaskAction -Execute $docker -Argument '--background'
$definitions = @(
    @{Name='RAG Docker Desktop'; Action=$dockerAction; Triggers=$dockerTriggers},
    @{Name='RAG Catalog HTTPS Proxy'; Action=$proxy.Actions; Triggers=$proxyTriggers},
    @{Name='RAG Catalog Server'; Action=$serverAction; Triggers=$serverTriggers}
)
foreach ($definition in $definitions) {
    $stage = 'register:' + $definition.Name
    $parameters = @{
        TaskName=$definition.Name; Action=$definition.Action; Trigger=$definition.Triggers;
        Settings=$settings; Force=$true; Description='RAG recovery: boot/logon and bounded idempotent health checks. See launcher-recovery.log.'
    }
    if ($Unattended) {
        # RegisterTask stores the secret in Windows, not in the XML backup or repo.
        $parameters.User = $Credential.UserName
        $parameters.Password = $Credential.GetNetworkCredential().Password
        $parameters.RunLevel = 'Limited'
    } else {
        $parameters.Principal = New-ScheduledTaskPrincipal -UserId $user -LogonType Interactive -RunLevel Limited
        if ($definition.Name -eq 'RAG Catalog HTTPS Proxy') { $parameters.Principal = $proxy.Principal }
    }
    try { Register-ScheduledTask @parameters | Out-Null }
    finally { $parameters.Remove('Password'); $parameters.Clear() }
}
$Credential = $null
# Existing healthy processes are adopted, not restarted.
$stage = 'start-recovery'
Start-ScheduledTask -TaskName 'RAG Catalog Server'
@{success=$true; at=(Get-Date).ToString('o'); unattended=[bool]$Unattended; backup=$backupRoot} |
    ConvertTo-Json | Set-Content -LiteralPath $resultPath -Encoding UTF8
if ($PromptForCredential) {
    Add-Type -AssemblyName System.Windows.Forms
    [System.Windows.Forms.MessageBox]::Show('RAG autostart tasks installed. A reboot test is still required; this setup does not reboot the PC.', 'RAG autostart') | Out-Null
}
Write-Host "RAG recovery configured. Unattended=$Unattended; task XML backups: $backupRoot"
