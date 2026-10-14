using System.Security.Cryptography;
using System.Security.Principal;
using System.Runtime.InteropServices;
using System.Text;
using Microsoft.Win32;
using Windows.Security.Cryptography;
using Windows.Storage;
using Windows.Storage.Provider;
using Windows.Win32;

namespace RagCloudFiles;

internal static class SyncRootRegistrar
{
    private const int NotUnderSyncRootHResult = unchecked((int)0x80070186);
    private const string SyncRootRegistryPath = @"SOFTWARE\Microsoft\Windows\CurrentVersion\Explorer\SyncRootManager";
    internal static readonly Guid ProviderId = new("8f734f08-90fd-4c31-a3e2-1edcad1693fb");

    public static async Task EnsureRegisteredAsync(
        ProviderConfig config,
        string rootPath,
        CancellationToken cancellationToken)
    {
        string root = Path.GetFullPath(rootPath);
        StorageFolder folder = await StorageFolder.GetFolderFromPathAsync(root);
        string expectedId = BuildSyncRootId(config.Server, config.RootKey);
        StorageProviderSyncRootInfo? existing = GetRegistration(folder);
        if (existing is not null && CanReuseRegistration(existing.Id, expectedId))
        {
            ReconcileNavigation(config, root, expectedId);
            return;
        }

        if (existing is not null && existing.Id.Length > 0)
        {
            throw new InvalidOperationException("Облачная папка зарегистрирована другим провайдером. Регистрация и файлы сохранены.");
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
        ReconcileNavigation(config, root, expectedId);
    }

    private static void ReconcileNavigation(ProviderConfig config, string root, string expectedId)
    {
        try
        {
            int hidden = 0;
            IReadOnlyList<(string Id, string Path)> roots = ReadShellRoots();
            foreach ((string id, string path) in roots.Take(20))
                AppLog.Info($"Explorer root entry: {id}; path={path}.");
            foreach ((string id, string path) in roots)
            {
                bool? pinned = NavigationVisibility(id, path, expectedId, root, config.PreservedRoot);
                if (pinned is null) continue;
                try
                {
                    SetNavigationPinned(id, path, pinned.Value);
                    if (!pinned.Value) hidden++;
                }
                catch (Exception exception)
                {
                    AppLog.Warn($"Could not update Explorer entry {id} (0x{exception.HResult:X8}): {exception.Message}");
                }
            }
            AppLog.Info($"Explorer navigation reconciled: active={root}; preserved={config.PreservedRoot}; inactive entries hidden={hidden}; registered roots={roots.Count}; active id={expectedId}.");
        }
        catch (Exception exception)
        {
            // Shell presentation must never unregister roots or interrupt file synchronization.
            AppLog.Warn($"Could not reconcile Explorer navigation (0x{exception.HResult:X8}): {exception.Message}");
        }
    }

    private static IReadOnlyList<(string Id, string Path)> ReadShellRoots()
    {
        using RegistryKey machine = RegistryKey.OpenBaseKey(RegistryHive.LocalMachine, RegistryView.Registry64);
        using RegistryKey? manager = machine.OpenSubKey(SyncRootRegistryPath);
        List<(string, string)> roots = [];
        foreach (string id in manager?.GetSubKeyNames() ?? [])
        {
            if (!id.StartsWith(CurrentUserIdPrefix(), StringComparison.OrdinalIgnoreCase)) continue;
            using RegistryKey? users = manager!.OpenSubKey(id + @"\UserSyncRoots");
            string path = Convert.ToString(users?.GetValue(WindowsIdentity.GetCurrent().User!.Value)) ?? "";
            if (path.Length > 0) roots.Add((id, path));
        }
        return roots;
    }

    internal static bool? NavigationVisibility(string id, string path,
        string expectedId, string root, string preservedRoot)
    {
        if (!id.StartsWith(CurrentUserIdPrefix(), StringComparison.OrdinalIgnoreCase))
            return null;
        if (SamePath(path, root)) return CanReuseRegistration(id, expectedId);
        if (!CanReuseRegistration(id, expectedId) && SamePath(path, preservedRoot))
            return false;
        return null;
    }

    private static bool SamePath(string left, string right) =>
        left.Length > 0 && right.Length > 0 && Path.IsPathFullyQualified(left) && Path.IsPathFullyQualified(right)
        && Path.GetFullPath(left).TrimEnd('\\').Equals(Path.GetFullPath(right).TrimEnd('\\'), StringComparison.OrdinalIgnoreCase);

    private static string CurrentUserIdPrefix() => $"TSK.RagCloudFiles!{WindowsIdentity.GetCurrent().User?.Value}!";

    private static RegistryKey OpenNavigationKey(string id, string root, bool writable)
    {
        if (!id.StartsWith(CurrentUserIdPrefix(), StringComparison.OrdinalIgnoreCase) || id.Contains('\\') || id.Contains('/'))
            throw new InvalidDataException("Not a RAG sync root for this Windows user.");
        using RegistryKey machine = RegistryKey.OpenBaseKey(RegistryHive.LocalMachine, RegistryView.Registry64);
        using RegistryKey? registration = machine.OpenSubKey(SyncRootRegistryPath + "\\" + id);
        using RegistryKey? userRoots = registration?.OpenSubKey("UserSyncRoots");
        string sid = WindowsIdentity.GetCurrent().User!.Value;
        if (!SamePath(Convert.ToString(userRoots?.GetValue(sid)) ?? "", root)
            || !Guid.TryParse(Convert.ToString(registration?.GetValue("NamespaceCLSID")), out Guid clsid))
            throw new InvalidDataException("Explorer sync root ownership could not be verified.");
        RegistryKey? key = Registry.CurrentUser.OpenSubKey(@"Software\Classes\CLSID\" + clsid.ToString("B"), writable);
        using RegistryKey? properties = key?.OpenSubKey(@"Instance\InitPropertyBag");
        if (key is null || !SamePath(Convert.ToString(properties?.GetValue("TargetFolderPath")) ?? "", root))
        {
            key?.Dispose();
            throw new InvalidDataException("Explorer namespace target does not match the cloud root.");
        }
        return key;
    }

    private static void SetNavigationPinned(string id, string root, bool pinned)
    {
        using RegistryKey key = OpenNavigationKey(id, root, writable: true);
        int value = pinned ? 1 : 0;
        if (key.GetValue("System.IsPinnedToNameSpaceTree") is int existing && existing == value) return;
        // Keep CfAPI registration and placeholders intact, including unsynchronized local files.
        // https://learn.microsoft.com/windows/win32/shell/integrate-cloud-storage (step 3)
        key.SetValue("System.IsPinnedToNameSpaceTree", value, RegistryValueKind.DWord);
        SHChangeNotify(0x08000000 /* SHCNE_ASSOCCHANGED */, 0, 0, 0);
        AppLog.Info($"Explorer navigation {(pinned ? "shown" : "hidden")}: {root}; files and sync registration preserved.");
    }

    internal static bool IsNavigationPinned(string id, string root)
    {
        using RegistryKey key = OpenNavigationKey(id, root, writable: false);
        return key.GetValue("System.IsPinnedToNameSpaceTree") is int value && value == 1;
    }

    [DllImport("shell32.dll")]
    private static extern void SHChangeNotify(uint eventId, uint flags, nint item1, nint item2);

    internal static bool CanReuseRegistration(string existingId, string expectedId) =>
        existingId.Equals(expectedId, StringComparison.OrdinalIgnoreCase);

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
        string? id = existing?.Id;
        if (string.IsNullOrEmpty(id))
        {
            // WinRT can miss an unpackaged provider's Shell registration; CfAPI alone leaves its tree entry behind.
            id = ReadShellRoots().SingleOrDefault(item => SamePath(item.Path, root)).Id;
        }
        if (!string.IsNullOrEmpty(id))
        {
            StorageProviderSyncRootManager.Unregister(id);
        }
        else
        {
            PInvoke.CfUnregisterSyncRoot(root).ThrowOnFailure();
        }
        VirtualDriveManager.RemoveForRoot(root);
    }

    internal static string BuildSyncRootId(string server, string rootKey = "")
    {
        if (rootKey.Length > 0 && !Guid.TryParseExact(rootKey, "N", out _))
            throw new InvalidDataException("Invalid cloud root key.");
        string sid = WindowsIdentity.GetCurrent().User?.Value
            ?? throw new InvalidOperationException("Не удалось определить SID пользователя Windows.");
        string identity = server.TrimEnd('/').ToLowerInvariant() + (rootKey.Length > 0 ? "\n" + rootKey : "");
        byte[] hash = SHA256.HashData(Encoding.UTF8.GetBytes(identity));
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
            throw;
        }
        catch (COMException exception)
        {
            if (exception.HResult is NotUnderSyncRootHResult or unchecked((int)0x80070490)) return null;
            throw;
        }
    }
}
