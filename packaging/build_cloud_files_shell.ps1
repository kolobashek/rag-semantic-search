param(
    [string]$CertificatePath = "",
    [string]$CertificatePassword = "",
    [string]$Publisher = "CN=TSK-NSK",
    [string]$PackageVersion = "0.4.0.0"
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$ProjectDir = Split-Path -Parent $PSScriptRoot
$ShellProject = Join-Path $ProjectDir "clients\windows-shell-extension\RagCloudShell.vcxproj"
$ShellHostProject = Join-Path $ProjectDir "clients\windows-shell-extension\RagCloudShellHost.vcxproj"
$PackageSource = Join-Path $PSScriptRoot "cloud-files-shell"
$PackageStage = Join-Path $PSScriptRoot "dist\cloud-files-shell"
$PackagePath = Join-Path $PSScriptRoot "dist\RagCloudFilesShell-0.4.0.msix"
$MsBuild = "C:\Program Files (x86)\Microsoft Visual Studio\2022\BuildTools\MSBuild\Current\Bin\amd64\MSBuild.exe"
$SdkBin = "C:\Program Files (x86)\Windows Kits\10\bin\10.0.26100.0\x64"
$MakeAppx = Join-Path $SdkBin "makeappx.exe"
$SignTool = Join-Path $SdkBin "signtool.exe"

foreach ($required in @($MsBuild, $MakeAppx)) {
    if (-not (Test-Path -LiteralPath $required)) {
        throw "Build tool was not found: $required"
    }
}

& $MsBuild $ShellProject /t:Build /p:Configuration=Release /p:Platform=x64 /nologo
if ($LASTEXITCODE -ne 0) {
    throw "RAG Cloud shell extension build failed."
}
& $MsBuild $ShellHostProject /t:Build /p:Configuration=Release /p:Platform=x64 /nologo
if ($LASTEXITCODE -ne 0) {
    throw "RAG Cloud shell host build failed."
}

if (Test-Path -LiteralPath $PackageStage) {
    Remove-Item -LiteralPath $PackageStage -Recurse -Force
}
New-Item -ItemType Directory -Path (Join-Path $PackageStage "Assets") -Force | Out-Null
Copy-Item -LiteralPath (Join-Path $PackageSource "AppxManifest.xml") -Destination $PackageStage
$manifestPath = Join-Path $PackageStage "AppxManifest.xml"
[xml]$manifest = Get-Content -LiteralPath $manifestPath
$manifest.Package.Identity.Publisher = $Publisher
$manifest.Package.Identity.Version = $PackageVersion
$manifest.Save($manifestPath)
Copy-Item -LiteralPath (
    Join-Path $ProjectDir "clients\windows-shell-extension\bin\Release\RagCloudShell.dll"
) -Destination $PackageStage
Copy-Item -LiteralPath (
    Join-Path $ProjectDir "clients\windows-shell-extension\bin\Release\RagCloudShellHost.exe"
) -Destination $PackageStage
Copy-Item -LiteralPath (Join-Path $ProjectDir "assets\brand\png\app-icon-64.png") `
    -Destination (Join-Path $PackageStage "Assets\StoreLogo.png")
Copy-Item -LiteralPath (Join-Path $ProjectDir "assets\brand\png\app-icon-64.png") `
    -Destination (Join-Path $PackageStage "Assets\Square44x44Logo.png")
Copy-Item -LiteralPath (Join-Path $ProjectDir "assets\brand\png\app-icon-256.png") `
    -Destination (Join-Path $PackageStage "Assets\Square150x150Logo.png")

if (Test-Path -LiteralPath $PackagePath) {
    Remove-Item -LiteralPath $PackagePath -Force
}
& $MakeAppx pack /d $PackageStage /p $PackagePath /o
if ($LASTEXITCODE -ne 0) {
    throw "MSIX package build failed."
}

if ($CertificatePath) {
    if (-not (Test-Path -LiteralPath $CertificatePath)) {
        throw "Signing certificate was not found: $CertificatePath"
    }
    $certificate = [System.Security.Cryptography.X509Certificates.X509Certificate2]::new(
        $CertificatePath,
        $CertificatePassword
    )
    if ($certificate.Subject -ne $Publisher) {
        throw "Certificate subject '$($certificate.Subject)' does not match package publisher '$Publisher'."
    }
    $arguments = @("sign", "/fd", "SHA256", "/f", $CertificatePath)
    if ($CertificatePassword) {
        $arguments += @("/p", $CertificatePassword)
    }
    $arguments += $PackagePath
    & $SignTool @arguments
    if ($LASTEXITCODE -ne 0) {
        throw "MSIX package signing failed."
    }
    & $SignTool verify /pa /v $PackagePath
    if ($LASTEXITCODE -ne 0) {
        throw "MSIX package signature verification failed."
    }
    $hash = (Get-FileHash -Algorithm SHA256 -LiteralPath $PackagePath).Hash.ToLowerInvariant()
    Set-Content -LiteralPath "$PackagePath.sha256" `
        -Value "$hash  $(Split-Path -Leaf $PackagePath)" -Encoding ascii
    Write-Host "Built and signed $PackagePath"
}
else {
    Remove-Item -LiteralPath "$PackagePath.sha256" -Force -ErrorAction SilentlyContinue
    Write-Warning "Built unsigned $PackagePath. It cannot be deployed until signed by a trusted organization certificate."
}
