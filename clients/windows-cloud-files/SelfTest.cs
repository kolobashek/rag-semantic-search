using System.Text.Json;

namespace RagCloudFiles;

internal static class SelfTest
{
    public static void Run()
    {
        Equal("https://cloud.tsk-nsk.ru", new ProviderConfig().Server);
        Equal(false, WindowsBootstrap.IsInteractiveInstall(["--self-test"]));
        Equal("Folder/file.txt", CloudPath.Normalize("/Folder\\file.txt/"));
        Equal("Folder", CloudPath.Parent("Folder/file.txt"));
        Equal(2, CloudPath.Depth("Folder/file.txt"));
        Equal(true, ClientUpdater.IsNewerVersion("0.3.0", "0.3.1"));
        Equal(false, ClientUpdater.IsNewerVersion("0.3.0", "0.3.0"));
        Equal(false, ClientUpdater.IsNewerVersion("0.3.0", "invalid"));
        Equal(true, ClientUpdater.IsValidSha256(new string('a', 64)));
        Equal(false, ClientUpdater.IsValidSha256("not-a-hash"));

        string unicodePath = "Документы/Смета 2026.xlsx";
        Equal(unicodePath, FileIdentityCodec.Decode(FileIdentityCodec.Encode(unicodePath)));
        Throws<InvalidDataException>(() => CloudPath.Normalize("Folder/../secret.txt"));
        Throws<InvalidDataException>(() => FileIdentityCodec.Decode("bad"u8));
        Equal(true, ShellCommandHandler.IsSupported("share"));
        Equal(false, ShellCommandHandler.IsSupported("delete"));
        Equal(
            "https://catalog.example/explorer?path=%D0%94%D0%BE%D0%BA%D1%83%D0%BC%D0%B5%D0%BD%D1%82%D1%8B%2F%D0%A1%D0%BC%D0%B5%D1%82%D0%B0%202026.xlsx&kind=file&share=1",
            ShellCommandHandler.BuildExplorerUri(
                "https://catalog.example/",
                unicodePath,
                isFolder: false,
                openShare: true).AbsoluteUri);

        ChangePage page = JsonSerializer.Deserialize<ChangePage>("""
            {
              "next_cursor": "cursor-1",
              "acl_revision": "acl-1",
              "changes": [
                {
                  "node_type": "file",
                  "path": "Folder/file.txt",
                  "size_bytes": 123,
                  "checksum": "abc"
                }
              ]
            }
            """) ?? throw new InvalidOperationException("JSON self-test failed.");
        Equal("cursor-1", page.NextCursor);
        Equal("acl-1", page.AclRevision);
        Equal(123L, page.Changes.Single().SizeBytes);

        string temporary = Path.Combine(Path.GetTempPath(), "rag-cloud-files-self-test-" + Guid.NewGuid().ToString("N"));
        try
        {
            ConfigStore store = new(Path.Combine(temporary, "config.json"));
            ProviderConfig config = new()
            {
                Server = "https://catalog.example",
                DeviceId = "device-1",
                Token = "secret-device-token",
                KeepAllOffline = true,
                OfflinePaths = new HashSet<string>(["Документы"], StringComparer.OrdinalIgnoreCase),
                StartWithWindows = false,
            };
            store.SaveConfig(config);
            Equal("device-1", store.LoadConfig().DeviceId);
            Equal("secret-device-token", store.LoadConfig().Token);
            Equal(true, store.LoadConfig().KeepAllOffline);
            Equal(true, store.LoadConfig().OfflinePaths.Contains("документы"));
            Equal(false, store.LoadConfig().StartWithWindows);
            string root = Path.Combine(temporary, "root");
            string localFile = Path.Combine(root, "Документы", "Смета 2026.xlsx");
            Directory.CreateDirectory(Path.GetDirectoryName(localFile)!);
            File.WriteAllText(localFile, "test");
            Equal(unicodePath, ShellCommandHandler.GetCloudPath(root, localFile));
            Equal("", ShellCommandHandler.GetCloudPath(root, root));
            Throws<InvalidDataException>(() =>
                ShellCommandHandler.GetCloudPath(root, Path.Combine(temporary, "outside.txt")));
            CloudNode matchingRemote = new()
            {
                NodeType = "file",
                Path = unicodePath,
                SizeBytes = 4,
                Checksum = "9f86d081884c7d659a2feaa0c55ad015"
                    + "a3bf4f1b2b0b822cd15d6c15b0f00a08",
            };
            Equal(
                true,
                CloudFilesProvider.RemoteContentMatchesAsync(
                        matchingRemote,
                        localFile,
                        CancellationToken.None)
                    .GetAwaiter()
                    .GetResult());
            matchingRemote.Checksum = new string('0', 64);
            Equal(
                false,
                CloudFilesProvider.RemoteContentMatchesAsync(
                        matchingRemote,
                        localFile,
                        CancellationToken.None)
                    .GetAwaiter()
                    .GetResult());
            string testLog = Path.Combine(temporary, "logs", "RagCloudFiles.log");
            Directory.CreateDirectory(Path.GetDirectoryName(testLog)!);
            File.WriteAllText(testLog, "current log");
            File.WriteAllText(
                Path.Combine(Path.GetDirectoryName(testLog)!, "RagCloudFiles.1.log"),
                "recent archive");
            string expiredArchive = Path.Combine(
                Path.GetDirectoryName(testLog)!,
                "RagCloudFiles.2.log");
            File.WriteAllText(expiredArchive, "expired archive");
            File.SetLastWriteTimeUtc(expiredArchive, DateTime.UtcNow.AddDays(-31));
            AppLog.MaintainFiles(
                testLog,
                maxFileBytes: 4,
                maxArchiveFiles: 2,
                archiveRetention: TimeSpan.FromDays(30),
                now: DateTimeOffset.UtcNow);
            Equal(false, File.Exists(testLog));
            Equal(
                "current log",
                File.ReadAllText(Path.Combine(
                    Path.GetDirectoryName(testLog)!,
                    "RagCloudFiles.1.log")));
            Equal(
                "recent archive",
                File.ReadAllText(Path.Combine(
                    Path.GetDirectoryName(testLog)!,
                    "RagCloudFiles.2.log")));
            Equal(
                false,
                File.Exists(Path.Combine(
                    Path.GetDirectoryName(testLog)!,
                    "RagCloudFiles.3.log")));
            if (File.ReadAllText(store.ConfigPath).Contains("secret-device-token", StringComparison.Ordinal))
            {
                throw new InvalidOperationException("Device token was stored in clear text.");
            }
            ProviderState state = new()
            {
                ManagedPaths = new HashSet<string>([unicodePath]),
                AppliedOfflinePaths = new HashSet<string>(["Документы"]),
                AppliedOfflineVersions = new Dictionary<string, string>
                {
                    [unicodePath] = "version-1",
                },
                LocalFingerprints = new Dictionary<string, string>
                {
                    [unicodePath] = "123:456",
                },
            };
            store.SaveState(state);
            if (!store.LoadState().ManagedPaths.Contains(unicodePath.ToUpperInvariant()))
            {
                throw new InvalidOperationException("State path comparer is not case-insensitive.");
            }
            Equal(true, store.LoadState().AppliedOfflinePaths.Contains("документы"));
            Equal("version-1", store.LoadState().AppliedOfflineVersions[unicodePath.ToUpperInvariant()]);
            Equal("123:456", store.LoadState().LocalFingerprints[unicodePath.ToUpperInvariant()]);

            ClientStatusModel status = new();
            status.BeginTransfer(unicodePath);
            Equal(1, status.Current.ActiveTransfers);
            status.EndTransfer(unicodePath);
            Equal(ClientRunState.UpToDate, status.Current.State);

            using Icon baseIcon = (Icon)SystemIcons.Application.Clone();
            foreach (ClientRunState runState in Enum.GetValues<ClientRunState>())
            {
                using Icon statusIcon = TrayIconFactory.Create(baseIcon, runState);
                Equal(new Size(32, 32), statusIcon.Size);
            }
        }
        finally
        {
            if (Directory.Exists(temporary))
            {
                Directory.Delete(temporary, true);
            }
        }
    }

    private static void Equal<T>(T expected, T actual)
    {
        if (!EqualityComparer<T>.Default.Equals(expected, actual))
        {
            throw new InvalidOperationException($"Expected {expected}, got {actual}.");
        }
    }

    private static void Throws<TException>(Action action)
        where TException : Exception
    {
        try
        {
            action();
        }
        catch (TException)
        {
            return;
        }

        throw new InvalidOperationException($"Expected {typeof(TException).Name}.");
    }
}
