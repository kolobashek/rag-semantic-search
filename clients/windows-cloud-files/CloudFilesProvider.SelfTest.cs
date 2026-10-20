namespace RagCloudFiles;

internal sealed partial class CloudFilesProvider
{
    private static void CheckRemoval(bool condition, string message)
    {
        if (!condition) throw new InvalidOperationException("Remote removal regression: " + message);
    }

    internal static async Task TestRemoteRemovalAsync()
    {
        string fixture = Path.Combine(Path.GetTempPath(), "rag-removal-test-" + Guid.NewGuid().ToString("N"));
        string root = Path.Combine(fixture, "root");
        Directory.CreateDirectory(root);
        ConfigStore store = new(Path.Combine(fixture, "config", "config.json"));
        ProviderConfig config = new() { RootPath = root, Server = "https://removal.invalid" };
        SelfTest.HydrationTestHandler handler = new();
        try
        {
            using CloudDriveApi api = new(config.Server, "test", handler);
            await using CloudFilesProvider provider = new(config, store, api);
            string path = Path.Combine(root, "edited.txt");
            File.WriteAllText(path, "unsynchronized local edit");
            provider._state.ManagedPaths.Add("edited.txt");
            provider._state.ManagedVersions["edited.txt"] = "old-version";
            provider._state.LocalFingerprints["edited.txt"] = "old-fingerprint";
            using (FileStream locked = new(path, FileMode.Open, FileAccess.ReadWrite, FileShare.Read))
            {
                provider.ReconcileNamespace(CancellationToken.None);
                ProviderState persisted = store.LoadState();
                CheckRemoval(persisted.ManagedPaths.Contains("edited.txt"), "locked entry lost ownership");
                CheckRemoval(persisted.ManagedVersions["edited.txt"] == "old-version", "lost version");
                CheckRemoval(persisted.LocalFingerprints["edited.txt"] == "old-fingerprint", "lost fingerprint");
                CheckRemoval(provider._namespaceIncomplete, "locked removal reported success");
                bool deferred = false;
                try { await provider.SyncLocalFileAsync("edited.txt", CancellationToken.None); }
                catch (IOException) { deferred = true; }
                CheckRemoval(deferred && handler.Requests == 0, "locked file was uploaded or hydrated");
            }
            provider.ReconcileNamespace(CancellationToken.None);
            CheckRemoval(!File.Exists(path) && !store.LoadState().ManagedPaths.Contains("edited.txt"),
                "removal did not recover after the lock was released");
            string recovery = Path.Combine(root, PlaceholderRecovery.FolderName);
            string preserved = Directory.GetFiles(recovery, "edited.txt", SearchOption.AllDirectories).Single();
            CheckRemoval(File.ReadAllText(preserved) == "unsynchronized local edit", "local edit lost");
            CheckRemoval(!provider._namespaceIncomplete, "deferred state did not clear");

            string folder = Path.Combine(root, "deleted-folder");
            Directory.CreateDirectory(folder);
            File.WriteAllText(Path.Combine(folder, "new-local.txt"), "new unsent file");
            provider._state.ManagedPaths.Add("deleted-folder");
            provider._state.ManagedVersions["deleted-folder"] = "folder";
            provider.ReconcileNamespace(CancellationToken.None);
            preserved = Directory.GetFiles(recovery, "new-local.txt", SearchOption.AllDirectories).Single();
            CheckRemoval(!Directory.Exists(folder) && File.ReadAllText(preserved) == "new unsent file",
                "untracked changes inside a removed folder lost");

            File.WriteAllText(Path.Combine(root, "genuinely-new.txt"), "new file");
            bool attemptedUpload = false;
            try { await provider.SyncLocalFileAsync("genuinely-new.txt", CancellationToken.None); }
            catch (HttpRequestException) { attemptedUpload = true; }
            CheckRemoval(attemptedUpload && handler.Requests == 1, "ordinary new files no longer upload");
            CheckRemoval(File.Exists(Path.Combine(root, "genuinely-new.txt")), "failed upload lost local data");
        }
        finally { Directory.Delete(fixture, recursive: true); }
    }

    private async Task TestNativeRemoteRemovalAsync(SelfTest.HydrationTestHandler handler)
    {
        CloudNode removed = new() { Path = "removed-native.pdf", NodeType = "file", SizeBytes = 1024 };
        CloudNode orphan = new() { Path = "orphan-native.pdf", NodeType = "file", SizeBytes = 2048 };
        CloudNode nested = new() { Path = "synthetic-parent/child.pdf", NodeType = "file", SizeBytes = 512 };
        foreach (CloudNode node in new[] { removed, orphan, nested }) _nodes[node.Path] = node;
        ReconcileNamespace(CancellationToken.None);
        int requestsBefore = handler.Requests;
        await SyncLocalDirectoryAsync("synthetic-parent", CancellationToken.None);
        CheckRemoval(Directory.Exists(CloudPath.LocalPath(_root, "synthetic-parent")), "visible ancestor retired");
        _nodes.Remove(removed.Path);
        _nodes.Remove(orphan.Path);
        // Reproduce 0.6.15 forgetting a file after a failed local deletion.
        _state.ManagedPaths.Remove(orphan.Path);
        _state.ManagedVersions.Remove(orphan.Path);
        ReconcileNamespace(CancellationToken.None);
        await SyncLocalFileAsync(orphan.Path, CancellationToken.None);
        string recovery = Path.Combine(_root, PlaceholderRecovery.FolderName);
        foreach (CloudNode node in new[] { removed, orphan })
        {
            CheckRemoval(!File.Exists(CloudPath.LocalPath(_root, node.Path)), "stale placeholder remains");
            string preserved = Directory.GetFiles(recovery, Path.GetFileName(node.Path), SearchOption.AllDirectories).Single();
            CheckRemoval(CloudFilePinning.ReadPlaceholderState(preserved, out _), "placeholder metadata lost");
            CheckRemoval(new FileInfo(preserved).Length == node.SizeBytes, "placeholder length changed");
        }
        CheckRemoval(handler.Requests == requestsBefore, "preservation fetched missing remote bytes");

        string dirty = Path.Combine(_root, "dirty-native.txt");
        File.WriteAllText(dirty, "local native edit");
        CloudFilePinning.ConvertToPlaceholder(dirty, "dirty-native.txt");
        File.AppendAllText(dirty, " after sync");
        await SyncLocalFileAsync("dirty-native.txt", CancellationToken.None);
        string dirtyCopy = Directory.GetFiles(recovery, "dirty-native.txt", SearchOption.AllDirectories).Single();
        CheckRemoval(File.ReadAllText(dirtyCopy) == "local native edit after sync", "native local edit lost");
        CheckRemoval(handler.Requests == requestsBefore, "orphan dirty placeholder uploaded or hydrated");

        string renamed = Path.Combine(_root, "renamed-local.txt");
        File.WriteAllText(renamed, "renamed edit");
        CloudFilePinning.ConvertToPlaceholder(renamed, "original-name.txt");
        CheckRemoval(!TryRetireAbsentEntry("renamed-local.txt", renamed) && File.Exists(renamed),
            "a local rename was mistaken for a removed remote file");

        _nodes["newly-uploaded.txt"] = new CloudNode { Path = "newly-uploaded.txt", NodeType = "file" };
        string uploaded = Path.Combine(_root, "newly-uploaded.txt");
        File.WriteAllText(uploaded, "uploaded edit");
        _state.ManagedPaths.Add("newly-uploaded.txt");
        CheckRemoval(!TryRetireAbsentEntry("newly-uploaded.txt", uploaded),
            "an upload newer than the last snapshot was retired");

        _nodes.Remove(nested.Path);
        ReconcileNamespace(CancellationToken.None);
        CheckRemoval(!Directory.Exists(CloudPath.LocalPath(_root, "synthetic-parent")), "removed folder remains");
        CheckRemoval(handler.Requests == requestsBefore, "removed folder caused network IO");
        Console.WriteLine("Native remote removal: clean/orphan/dirty placeholders and folders preserved without network IO");
    }
}
