[CmdletBinding()]
param(
    [Parameter(Mandatory=$true)][string]$ConfigPath,
    [string]$TaskName = 'RAG Mail.ru Disk Bridge',
    [string]$Python = '',
    [int]$IntervalMinutes = 3,
    [int]$MaxFiles = 50,
    [switch]$EnableWrites
)
$ErrorActionPreference = 'Stop'
if ($IntervalMinutes -lt 1 -or $MaxFiles -lt 1) { throw 'Positive interval and batch size required.' }
$project = Split-Path -Parent $PSScriptRoot
$config = (Resolve-Path -LiteralPath $ConfigPath).Path
$spec = Get-Content -LiteralPath $config -Raw -Encoding UTF8 | ConvertFrom-Json
if (-not $spec.source_root -or -not $spec.mappings -or -not $spec.state_path) {
    throw 'Explicit source, mappings and state_path are required.'
}
if (-not $Python) { $Python = (Get-Command python.exe -ErrorAction Stop).Source }
$pythonw = Join-Path (Split-Path -Parent $Python) 'pythonw.exe'
if (-not (Test-Path -LiteralPath $pythonw -PathType Leaf)) { throw "Missing $pythonw" }
$existing = Get-ScheduledTask -TaskName $TaskName -ErrorAction SilentlyContinue
if ($existing -and $existing.State -eq 'Running') { throw 'Wait for the running pass before changing its task.' }
$arguments = '-c "import sys; sys.path.insert(0, ''src''); from rag_catalog.cli.disk_bridge import main; raise SystemExit(main())" --config "' + $config + '" --max-files ' + $MaxFiles
if ($EnableWrites) { $arguments += ' --apply' }
$user = [System.Security.Principal.WindowsIdentity]::GetCurrent().Name
$action = New-ScheduledTaskAction -Execute $pythonw -Argument $arguments -WorkingDirectory $project
$principal = New-ScheduledTaskPrincipal -UserId $user -LogonType Interactive -RunLevel Limited
$periodic = New-ScheduledTaskTrigger -Once -At (Get-Date).AddMinutes($IntervalMinutes) -RepetitionInterval (New-TimeSpan -Minutes $IntervalMinutes)
$logon = New-ScheduledTaskTrigger -AtLogOn -User $user
$settings = New-ScheduledTaskSettingsSet -MultipleInstances IgnoreNew -ExecutionTimeLimit (New-TimeSpan -Minutes 30) -StartWhenAvailable -AllowStartIfOnBatteries -DontStopIfGoingOnBatteries
Register-ScheduledTask -TaskName $TaskName -Action $action -Principal $principal -Settings $settings -Trigger @($periodic, $logon) -Force | Out-Null
Start-ScheduledTask -TaskName $TaskName
Get-ScheduledTask -TaskName $TaskName | Select-Object TaskName, State
