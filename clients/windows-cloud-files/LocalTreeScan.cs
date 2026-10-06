namespace RagCloudFiles;

internal sealed class LocalTreeScan
{
    public List<string> Directories { get; } = [];
    public List<string> Files { get; } = [];
    public List<string> Unreadable { get; } = [];

    public static LocalTreeScan Read(string root, Action<string, Exception> onError,
        Func<string, string[]>? list = null, Func<string, FileAttributes>? attributes = null,
        Func<string, bool>? repair = null, CancellationToken cancellationToken = default)
    {
        list ??= Directory.GetFileSystemEntries;
        attributes ??= File.GetAttributes;
        LocalTreeScan result = new();
        Queue<string> pending = new();
        HashSet<string> retried = new(StringComparer.OrdinalIgnoreCase);
        pending.Enqueue(root);
        while (pending.TryDequeue(out string? directory))
        {
            cancellationToken.ThrowIfCancellationRequested();
            try
            {
                foreach (string path in list(directory))
                {
                    cancellationToken.ThrowIfCancellationRequested();
                    if (directory.Equals(root, StringComparison.OrdinalIgnoreCase)
                        && Path.GetFileName(path).Equals(PlaceholderRecovery.FolderName, StringComparison.OrdinalIgnoreCase)) continue;
                    try
                    {
                        FileAttributes flags = attributes(path);
                        if ((flags & FileAttributes.Directory) == 0) result.Files.Add(path);
                        else
                        {
                            result.Directories.Add(path);
                            // Do not traverse junctions/symlinks outside the sync root. Cloud
                            // placeholders also have ReparsePoint, but no LinkTarget.
                            if ((flags & FileAttributes.ReparsePoint) == 0 || new DirectoryInfo(path).LinkTarget is null)
                                pending.Enqueue(path);
                            else result.Unreadable.Add(path);
                        }
                    }
                    catch (Exception ex) when (ex is IOException or UnauthorizedAccessException)
                    {
                        result.Unreadable.Add(path);
                        onError(path, ex);
                    }
                }
            }
            catch (Exception ex) when (ex is IOException or UnauthorizedAccessException)
            {
                if (repair is not null && retried.Add(directory) && repair(directory))
                {
                    pending.Enqueue(directory);
                    continue;
                }
                result.Unreadable.Add(directory);
                onError(directory, ex);
            }
        }
        return result;
    }
}
