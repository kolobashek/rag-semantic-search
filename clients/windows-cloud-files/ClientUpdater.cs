using System.Security.Cryptography;

namespace RagCloudFiles;

internal sealed class ClientUpdater
{
    private static readonly TimeSpan CheckInterval = TimeSpan.FromHours(6);
    private const long MaximumUpdateBytes = 1024L * 1024 * 1024;

    private readonly CloudDriveApi _api;
    private readonly ClientStatusModel _status;
    private readonly SemaphoreSlim _checkLock = new(1, 1);
    public string LastCheckMessage { get; private set; } = "";

    public ClientUpdater(CloudDriveApi api, ClientStatusModel status)
    {
        _api = api;
        _status = status;
    }

    public async Task RunAutomaticAsync(string clientId, Action requestShutdown, CancellationToken cancellationToken)
    {
        DateTimeOffset nextCheck = DateTimeOffset.MinValue;
        DateTimeOffset retryAfter = DateTimeOffset.MinValue;
        using PeriodicTimer timer = new(TimeSpan.FromSeconds(30));
        do
        {
            bool requested = false;
            try { requested = await _api.IsUpdateRequestedAsync(clientId, cancellationToken); }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested) { return; }
            catch { /* An unavailable command endpoint must not disable ordinary updates. */ }
            if ((requested || DateTimeOffset.UtcNow >= nextCheck) && DateTimeOffset.UtcNow >= retryAfter)
            {
                if (await CheckAndApplyAsync(requestShutdown, cancellationToken)) return;
                nextCheck = DateTimeOffset.UtcNow + CheckInterval;
                retryAfter = DateTimeOffset.UtcNow + TimeSpan.FromMinutes(5);
            }
        } while (await timer.WaitForNextTickAsync(cancellationToken));
    }

    public async Task<bool> CheckAndApplyAsync(
        Action requestShutdown,
        CancellationToken cancellationToken)
    {
        if (!WindowsBootstrap.IsRunningInstalled ||
            !await _checkLock.WaitAsync(0, cancellationToken))
        {
            LastCheckMessage = "Проверка обновлений уже выполняется.";
            return false;
        }

        try
        {
            UpdateManifest manifest = await _api.GetUpdateManifestAsync(cancellationToken);
            try
            {
                await EnsureShellExtensionAsync(manifest, cancellationToken);
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                throw;
            }
            catch (Exception exception)
            {
                AppLog.Error("Интеграция с меню Проводника не установлена.", exception);
            }
            if (!manifest.HasCloudFilesExecutable ||
                !IsNewerVersion(AppDefaults.Version, manifest.Version))
            {
                LastCheckMessage = $"Установлена актуальная версия {AppDefaults.Version}.";
                return false;
            }
            if (!IsValidSha256(manifest.Sha256) ||
                manifest.SizeBytes <= 0 ||
                manifest.SizeBytes > MaximumUpdateBytes)
            {
                throw new InvalidDataException("Манифест обновления содержит недопустимый размер или SHA-256.");
            }

            Version version = Version.Parse(manifest.Version);
            string finalPath = Path.Combine(
                WindowsBootstrap.UpdateDirectory,
                $"RagCloudFiles-{version}.exe");
            string temporaryPath = finalPath + ".download";
            Directory.CreateDirectory(WindowsBootstrap.UpdateDirectory);
            File.Delete(temporaryPath);
            File.Delete(finalPath);

            _status.SetState(ClientRunState.Syncing, $"Загрузка обновления {version}…");
            await _api.DownloadUpdateAsync(
                manifest.DownloadUrl,
                temporaryPath,
                manifest.SizeBytes,
                cancellationToken);
            string actualHash = await ComputeSha256Async(temporaryPath, cancellationToken);
            if (!actualHash.Equals(manifest.Sha256, StringComparison.OrdinalIgnoreCase))
            {
                throw new InvalidDataException(
                    $"SHA-256 обновления не совпал: ожидался {manifest.Sha256}, получен {actualHash}.");
            }

            File.Move(temporaryPath, finalPath);
            _status.SetState(ClientRunState.Syncing, $"Установка обновления {version}…");
            WindowsBootstrap.LaunchStagedUpdate(finalPath, actualHash);
            LastCheckMessage = $"Устанавливается версия {version}.";
            requestShutdown();
            return true;
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception exception)
        {
            AppLog.Error("Автоматическое обновление не выполнено.", exception);
            LastCheckMessage = "Не удалось обновить клиент: " + exception.Message;
            return false;
        }
        finally
        {
            _checkLock.Release();
        }
    }

    private async Task EnsureShellExtensionAsync(
        UpdateManifest manifest,
        CancellationToken cancellationToken)
    {
        if (!OperatingSystem.IsWindowsVersionAtLeast(10, 0, 22000) ||
            !manifest.HasShellPackage ||
            !IsValidSha256(manifest.ShellSha256) ||
            manifest.ShellSizeBytes <= 0 ||
            manifest.ShellSizeBytes > MaximumUpdateBytes ||
            !Version.TryParse(manifest.ShellVersion, out Version? shellVersion))
        {
            return;
        }

        string installedVersion = await WindowsBootstrap.GetShellExtensionVersionAsync(cancellationToken);
        if (installedVersion.Length > 0 &&
            !IsNewerVersion(installedVersion, shellVersion.ToString()))
        {
            return;
        }

        string finalPath = Path.Combine(
            WindowsBootstrap.UpdateDirectory,
            $"RagCloudFilesShell-{shellVersion}.msix");
        string temporaryPath = finalPath + ".download";
        Directory.CreateDirectory(WindowsBootstrap.UpdateDirectory);
        File.Delete(temporaryPath);
        File.Delete(finalPath);
        _status.SetState(ClientRunState.Syncing, "Установка интеграции с Проводником…");
        await _api.DownloadUpdateAsync(
            manifest.ShellDownloadUrl,
            temporaryPath,
            manifest.ShellSizeBytes,
            cancellationToken);
        string actualHash = await ComputeSha256Async(temporaryPath, cancellationToken);
        if (!actualHash.Equals(manifest.ShellSha256, StringComparison.OrdinalIgnoreCase))
        {
            throw new InvalidDataException(
                $"SHA-256 shell-пакета не совпал: ожидался {manifest.ShellSha256}, получен {actualHash}.");
        }
        File.Move(temporaryPath, finalPath);
        await WindowsBootstrap.InstallShellExtensionAsync(finalPath, cancellationToken);
    }

    internal static bool IsNewerVersion(string current, string candidate) =>
        Version.TryParse(current, out Version? currentVersion) &&
        Version.TryParse(candidate, out Version? candidateVersion) &&
        candidateVersion > currentVersion;

    internal static bool IsValidSha256(string value) =>
        value.Length == 64 && value.All(Uri.IsHexDigit);

    internal static async Task<string> ComputeSha256Async(
        string path,
        CancellationToken cancellationToken)
    {
        await using FileStream stream = new(
            path,
            FileMode.Open,
            FileAccess.Read,
            FileShare.Read,
            bufferSize: 1024 * 1024,
            useAsync: true);
        byte[] hash = await SHA256.HashDataAsync(stream, cancellationToken);
        return Convert.ToHexString(hash).ToLowerInvariant();
    }
}
