param(
    [string]$DotNet = "$env:USERPROFILE\.dotnet\dotnet.exe"
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$ProjectDir = Split-Path -Parent $PSScriptRoot
$ClientProject = Join-Path $ProjectDir "clients\windows-cloud-files\RagCloudFiles.csproj"
$DistDir = Join-Path $PSScriptRoot "dist"

if (-not (Test-Path -LiteralPath $DotNet)) {
    $resolved = Get-Command dotnet -ErrorAction SilentlyContinue
    if (-not $resolved) {
        throw ".NET 8 SDK не найден."
    }
    $DotNet = $resolved.Source
}

New-Item -ItemType Directory -Path $DistDir -Force | Out-Null
& $DotNet publish $ClientProject `
    -c Release `
    -r win-x64 `
    --self-contained true `
    -p:PublishSingleFile=true `
    -p:DebugType=None `
    --nologo `
    -o $DistDir
if ($LASTEXITCODE -ne 0) {
    throw "Cloud Files provider publish failed."
}

$exe = Join-Path $DistDir "RagCloudFiles.exe"
if (-not (Test-Path -LiteralPath $exe)) {
    throw "Publish не создал $exe."
}

$hash = (Get-FileHash -Algorithm SHA256 -LiteralPath $exe).Hash.ToLowerInvariant()
Set-Content -LiteralPath "$exe.sha256" -Value "$hash  RagCloudFiles.exe" -Encoding ascii
Write-Host "Built $exe ($([math]::Round((Get-Item $exe).Length / 1MB, 1)) MB)"
Write-Host "SHA-256 $hash"
