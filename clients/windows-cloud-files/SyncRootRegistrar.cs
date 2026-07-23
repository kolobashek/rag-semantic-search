using System.Security.Cryptography;
using System.Security.Principal;
using System.Runtime.InteropServices;
using System.Text;
using Windows.Security.Cryptography;
using Windows.Storage;
using Windows.Storage.Provider;
using Windows.Win32;

namespace RagCloudFiles;

internal static class SyncRootRegistrar
{
    private const int NotUnderSyncRootHResult = unchecked((int)0x80070186);
    internal static readonly Guid ProviderId = new("8f734f08-90fd-4c31-a3e2-1edcad1693fb");

    public static async Task EnsureRegisteredAsync(
        ProviderConfig config,
        string rootPath,
        CancellationToken cancellationToken)
    {
        string root = Path.GetFullPath(rootPath);
        StorageFolder folder = await StorageFolder.GetFolderFromPathAsync(root);
        string expectedId = BuildSyncRootId(config.Server);
        StorageProviderSyncRootInfo? existing = GetRegistration(folder);
        if (existing is not null
            && existing.Id.Equals(expectedId, StringComparison.OrdinalIgnoreCase)
            && existing.Version.Equals(AppDefaults.Version, StringComparison.Ordinal))
        {
            return;
        }

        if (existing is not null && existing.Id.Length > 0)
        {
            StorageProviderSyncRootManager.Unregister(existing.Id);
        }
        else
        {
            int result = PInvoke.CfUnregisterSyncRoot(root).Value;
            if (result < 0 && result != NotUnderSyncRootHResult)
            {
                AppLog.Info($"Legacy CfAPI unregister returned 0x{result:X8} for {root}.");
            }
        }

        cancellationToken.ThrowIfCancellationRequested();
        StorageProviderSyncRootInfo info = new()
        {
            Id = expectedId,
            Path = folder,
            DisplayNameResource = AppDefaults.ProductName,
            IconResource = $"{Path.GetFullPath(Environment.ProcessPath ?? WindowsBootstrap.InstalledExecutable)},0",
            HydrationPolicy = StorageProviderHydrationPolicy.Partial,
            HydrationPolicyModifier = StorageProviderHydrationPolicyModifier.AutoDehydrationAllowed,
            PopulationPolicy = StorageProviderPopulationPolicy.AlwaysFull,
            InSyncPolicy = StorageProviderInSyncPolicy.FileCreationTime
                | StorageProviderInSyncPolicy.FileLastWriteTime
                | StorageProviderInSyncPolicy.DirectoryCreationTime
                | StorageProviderInSyncPolicy.DirectoryLastWriteTime,
            Version = AppDefaults.Version,
            ShowSiblingsAsGroup = false,
            HardlinkPolicy = StorageProviderHardlinkPolicy.None,
            ProtectionMode = StorageProviderProtectionMode.Personal,
            AllowPinning = true,
            ProviderId = ProviderId,
            Context = CryptographicBuffer.ConvertStringToBinary(
                $"rag-cloud-files\n{config.Server}\n{root}",
                BinaryStringEncoding.Utf8),
        };
        StorageProviderSyncRootManager.Register(info);
        await Task.Delay(250, cancellationToken);
        AppLog.Info($"Registered Explorer sync root {expectedId} at {root}.");
    }

    public static void Unregister(string rootPath)
    {
        string root = Path.GetFullPath(rootPath);
        if (!Directory.Exists(root))
        {
            VirtualDriveManager.RemoveForRoot(root);
            return;
        }

        StorageFolder folder = StorageFolder.GetFolderFromPathAsync(root).GetAwaiter().GetResult();
        StorageProviderSyncRootInfo? existing = GetRegistration(folder);
        if (existing is not null && existing.Id.Length > 0)
        {
            StorageProviderSyncRootManager.Unregister(existing.Id);
        }
        else
        {
            PInvoke.CfUnregisterSyncRoot(root).ThrowOnFailure();
        }
        VirtualDriveManager.RemoveForRoot(root);
    }

    internal static string BuildSyncRootId(string server)
    {
        string sid = WindowsIdentity.GetCurrent().User?.Value
            ?? throw new InvalidOperationException("Не удалось определить SID пользователя Windows.");
        byte[] hash = SHA256.HashData(Encoding.UTF8.GetBytes(server.TrimEnd('/').ToLowerInvariant()));
        return $"TSK.RagCloudFiles!{sid}!{Convert.ToHexString(hash)[..16]}";
    }

    private static StorageProviderSyncRootInfo? GetRegistration(StorageFolder folder)
    {
        try
        {
            return StorageProviderSyncRootManager.GetSyncRootInformationForFolder(folder);
        }
        catch (FileNotFoundException)
        {
            return null;
        }
        catch (UnauthorizedAccessException)
        {
            return null;
        }
        catch (COMException exception)
        {
            AppLog.Info(
                $"Explorer sync-root lookup returned 0x{exception.HResult:X8}; migrating legacy registration.");
            return null;
        }
    }
}
