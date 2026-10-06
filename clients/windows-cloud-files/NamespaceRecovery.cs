using System.Text.Json;

namespace RagCloudFiles;

internal static class NamespaceRecovery
{
    internal static void Prepare(ProviderConfig config, ConfigStore store, string requestId)
    {
        if (!Guid.TryParseExact(requestId, "N", out _)) throw new InvalidDataException("Invalid recovery request.");
        if (config.RootKey == requestId) return;
        string oldRoot = Path.GetFullPath(config.RootPath).TrimEnd(Path.DirectorySeparatorChar);
        string parent = Path.GetDirectoryName(oldRoot) ?? throw new IOException("Cloud root has no parent.");
        for (DirectoryInfo? ancestor = new(parent); ancestor is not null; ancestor = ancestor.Parent)
            if (ancestor.LinkTarget is not null) throw new IOException("Recovery cannot traverse directory links.");
        string newRoot = Path.Combine(parent, "RAG Cloud Drive - " + requestId[..8]);
        string intent = Path.Combine(Path.GetDirectoryName(store.ConfigPath)!, $"recovery-{requestId}.json");
        string record = JsonSerializer.Serialize(new { oldRoot, newRoot, requestId });
        Directory.CreateDirectory(Path.GetDirectoryName(intent)!);
        if (File.Exists(intent))
        {
            if (File.ReadAllText(intent) != record) throw new IOException("Recovery intent does not match this root.");
        }
        else
        {
            if (Directory.Exists(newRoot) || File.Exists(newRoot)) throw new IOException("Recovery destination already exists.");
            using FileStream file = new(intent, FileMode.CreateNew, FileAccess.Write);
            using StreamWriter writer = new(file);
            writer.Write(record);
        }
        if (Directory.Exists(newRoot) && (new DirectoryInfo(newRoot).LinkTarget is not null
            || Directory.EnumerateFileSystemEntries(newRoot).Any()))
            throw new IOException("Recovery destination is not empty or is a link.");
        Directory.CreateDirectory(newRoot);
        // The old root, its registration, and its state file are deliberately untouched.
        config.PreservedRoot = oldRoot;
        config.RootPath = newRoot;
        config.RootKey = requestId;
        store.SaveConfig(config);
        AppLog.Info($"Fresh cloud root prepared at {newRoot}; original preserved at {oldRoot}. Local-only edits remain in the original.");
    }
}
