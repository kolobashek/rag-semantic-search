using System.Text.Json;

namespace RagCloudFiles;

internal static class NamespaceRecovery
{
    internal static string FindPreservedRoot(string configPath, string activeRoot)
    {
        string directory = Path.GetDirectoryName(Path.GetFullPath(configPath))!;
        if (!Directory.Exists(directory)) return "";
        try
        {
            string active = Path.GetFullPath(activeRoot).TrimEnd('\\');
            HashSet<string> matches = new(StringComparer.OrdinalIgnoreCase);
            string[] records = Directory.EnumerateFiles(directory, "recovery-*.json").Take(129).ToArray();
            if (records.Length > 128) return "";
            foreach (string record in records)
            {
                try
                {
                    if (new FileInfo(record).Length > 16384) continue;
                    using JsonDocument json = JsonDocument.Parse(File.ReadAllText(record));
                    string request = json.RootElement.GetProperty("requestId").GetString() ?? "";
                    string oldRoot = json.RootElement.GetProperty("oldRoot").GetString() ?? "";
                    string newRoot = json.RootElement.GetProperty("newRoot").GetString() ?? "";
                    if (!Guid.TryParseExact(request, "N", out _)
                        || !Path.GetFileName(record).Equals($"recovery-{request}.json", StringComparison.OrdinalIgnoreCase)
                        || !Path.IsPathFullyQualified(oldRoot) || !Path.IsPathFullyQualified(newRoot)) continue;
                    oldRoot = Path.GetFullPath(oldRoot).TrimEnd('\\');
                    newRoot = Path.GetFullPath(newRoot).TrimEnd('\\');
                    string? parent = Path.GetDirectoryName(oldRoot);
                    if (parent is null || oldRoot.Equals(active, StringComparison.OrdinalIgnoreCase)
                        || !newRoot.Equals(active, StringComparison.OrdinalIgnoreCase)
                        || !Path.Combine(parent, "RAG Cloud Drive - " + request[..8])
                            .Equals(active, StringComparison.OrdinalIgnoreCase)) continue;
                    matches.Add(oldRoot);
                }
                catch (Exception exception) when (exception is IOException or UnauthorizedAccessException
                    or JsonException or KeyNotFoundException or InvalidOperationException or ArgumentException)
                {
                    AppLog.Warn($"Ignored invalid recovery navigation record: {Path.GetFileName(record)}.");
                }
            }
            // Restore presentation only: never switch RootKey or the synchronization state database.
            return matches.Count == 1 ? matches.Single() : "";
        }
        catch (Exception exception) when (exception is IOException or UnauthorizedAccessException or ArgumentException)
        {
            AppLog.Warn($"Could not read recovery navigation history (0x{exception.HResult:X8}).");
            return "";
        }
    }

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
