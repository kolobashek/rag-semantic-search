param(
    [switch]$RemoveBuilds = $true
)

$ErrorActionPreference = "Stop"
$Root = (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot "..")).Path

function Remove-InProjectPath {
    param([Parameter(Mandatory = $true)][string]$Path)

    if (-not (Test-Path -LiteralPath $Path)) {
        return
    }
    $Resolved = (Resolve-Path -LiteralPath $Path).Path
    if (-not $Resolved.StartsWith($Root, [System.StringComparison]::OrdinalIgnoreCase)) {
        throw "Refusing to remove path outside project: $Resolved"
    }
    Remove-Item -LiteralPath $Resolved -Recurse -Force
    Write-Host "removed $Resolved"
}

Get-ChildItem -LiteralPath $Root -Directory -Recurse -Force -Filter "__pycache__" |
    ForEach-Object { Remove-InProjectPath -Path $_.FullName }

Remove-InProjectPath -Path (Join-Path $Root ".pytest_cache")

if ($RemoveBuilds) {
    Get-ChildItem -LiteralPath $Root -Directory -Force |
        Where-Object { $_.Name -match "^(build|dist)(_|$)" } |
        ForEach-Object { Remove-InProjectPath -Path $_.FullName }
}

Write-Host "cleanup complete"
