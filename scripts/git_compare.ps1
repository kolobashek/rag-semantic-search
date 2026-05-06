[CmdletBinding()]
param(
    [Parameter(Mandatory = $true)]
    [string]$Left,
    [Parameter(Mandatory = $true)]
    [string]$Right,
    [string]$Repo
)

if (-not $Repo) {
    $scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
    $Repo = (Resolve-Path (Join-Path $scriptDir "..")).Path
}

Write-Host "Repo: $Repo"
Write-Host "Compare: $Left .. $Right"
Write-Host ""
Write-Host "== Stat =="
git -C $Repo diff --stat "$Left..$Right"
Write-Host ""
Write-Host "== Commits =="
git -C $Repo log --oneline --left-right "$Left...$Right"
