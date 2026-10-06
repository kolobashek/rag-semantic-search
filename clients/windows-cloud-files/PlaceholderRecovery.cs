namespace RagCloudFiles;

internal static class PlaceholderRecovery
{
    internal const string FolderName = ".rag-cloud-recovery";
    internal static bool IsRecoveryPath(string path) => path.Equals(FolderName, StringComparison.OrdinalIgnoreCase)
        || path.StartsWith(FolderName + "/", StringComparison.OrdinalIgnoreCase);
    internal static bool IsCorruptMetadata(int hresult) => hresult == unchecked((int)0x8007016B);

    internal static string Preserve(string root, string cloudPath)
    {
        string fullRoot = Path.GetFullPath(root).TrimEnd(Path.DirectorySeparatorChar);
        string source = CloudPath.LocalPath(fullRoot, cloudPath);
        for (DirectoryInfo? parent = new FileInfo(source).Directory; parent is not null; parent = parent.Parent)
        {
            if (parent.LinkTarget is not null) throw new IOException("Recovery cannot traverse a directory link.");
            if (parent.FullName.Equals(fullRoot, StringComparison.OrdinalIgnoreCase)) break;
        }
        string recovery = Path.Combine(fullRoot, FolderName);
        if (Directory.Exists(recovery) && new DirectoryInfo(recovery).LinkTarget is not null)
            throw new IOException("Recovery directory must not be a link.");
        string marker = Path.Combine(recovery, ".rag-recovery-marker");
        if (Directory.Exists(recovery) && !File.Exists(marker))
            throw new IOException("Recovery directory already exists and is not owned by the client.");
        Directory.CreateDirectory(recovery);
        if (!File.Exists(marker)) File.WriteAllText(marker, "RAG Cloud Files: preserved local entries, never uploaded or automatically deleted.");
        string destination = CloudPath.LocalPath(Path.Combine(recovery, Guid.NewGuid().ToString("N")), cloudPath);
        Directory.CreateDirectory(Path.GetDirectoryName(destination)!);
        File.Move(source, destination, overwrite: false);
        return destination;
    }
}
