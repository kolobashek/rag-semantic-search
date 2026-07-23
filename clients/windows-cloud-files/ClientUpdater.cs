using System.Security.Cryptography;

namespace RagCloudFiles;

internal sealed class ClientUpdater
{
    private static readonly TimeSpan CheckInterval = TimeSpan.FromHours(6);
    private const long MaximumUpdateBytes = 1024L * 1024 * 1024;

    private readonly CloudDriveApi _api;
    private readonly ClientStatusModel _status;
    private readonly SemaphoreSlim _checkLock = new(1, 1);

    public ClientUpdater(CloudDriveApi api, ClientStatusModel status)
    {
        _api = api;
        _status = status;
    }

    public async Task RunAutomaticAsync(Action requestShutdown, CancellationToken cancellationToken)
    {
        if (await CheckAndApplyAsync(requestShutdown, cancellationToken))
        {
            return;
        }

        using PeriodicTimer timer = new(CheckInterval);
        while (await timer.WaitForNextTickAsync(cancellationToken))
        {
            if (await CheckAndApplyAsync(requestShutdown, cancellationToken))
            {
                return;
            }
        }
    }

    public async Task<bool> CheckAndApplyAsync(
        Action requestShutdown,
        CancellationToken cancellationToken)
    {
        if (!WindowsBootstrap.IsRunningInstalled ||
            !await _checkLock.WaitAsync(0, cancellationToken))
        {
            return false;
        }

        try
        {
            UpdateManifest manifest = await _api.GetUpdateManifestAsync(cancellationToken);
            if (!manifest.HasCloudFilesExecutable ||
                !IsNewerVersion(AppDefaults.Version, manifest.Version))
            {
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
            ClientStatusSnapshot snapshot = _status.Current;
            _status.SetState(
                snapshot.ActiveTransfers > 0 ? ClientRunState.Syncing : ClientRunState.UpToDate,
                snapshot.ActiveTransfers > 0
                    ? $"Загружается файлов: {snapshot.ActiveTransfers}"
                    : "Синхронизировано");
            return false;
        }
        finally
        {
            _checkLock.Release();
        }
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
