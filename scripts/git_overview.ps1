[CmdletBinding()]
param(
    [string]$Repo
)

if (-not $Repo) {
    $scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
    $Repo = (Resolve-Path (Join-Path $scriptDir "..")).Path
}

Write-Host "Repo: $Repo"
Write-Host ""
Write-Host "== Status =="
git -C $Repo status --short
Write-Host ""
Write-Host "== Branches =="
git -C $Repo branch --all --verbose --no-abbrev
Write-Host ""
Write-Host "== Graph =="
git -C $Repo log --oneline --graph --decorate --all -20
Write-Host ""
Write-Host "== Stashes =="
git -C $Repo stash list
