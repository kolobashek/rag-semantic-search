using System.Text;

namespace RagCloudFiles;

internal static class AppLog
{
    private const long MaxFileBytes = 5L * 1024 * 1024;
    private const int MaxArchiveFiles = 4;
    private static readonly TimeSpan ArchiveRetention = TimeSpan.FromDays(30);
    private static readonly object Sync = new();

    public static string DirectoryPath { get; } = Path.Combine(
        Environment.GetFolderPath(Environment.SpecialFolder.LocalApplicationData),
        "RAGCloudFiles",
        "logs");

    public static string FilePath { get; } = Path.Combine(DirectoryPath, "RagCloudFiles.log");

    public static void Info(string message) => Write("INFO", message);

    public static void Error(string message, Exception? exception = null)
    {
        Write("ERROR", exception is null ? message : $"{message}{Environment.NewLine}{exception}");
    }

    private static void Write(string level, string message)
    {
        try
        {
            lock (Sync)
            {
                Directory.CreateDirectory(DirectoryPath);
                MaintainFiles(
                    FilePath,
                    MaxFileBytes,
                    MaxArchiveFiles,
                    ArchiveRetention,
                    DateTimeOffset.UtcNow);
                File.AppendAllText(
                    FilePath,
                    $"{DateTimeOffset.Now:O} [{level}] {message}{Environment.NewLine}",
                    Encoding.UTF8);
            }
        }
        catch
        {
            // Logging must never terminate the provider.
        }
    }

    internal static void MaintainFiles(
        string filePath,
        long maxFileBytes,
        int maxArchiveFiles,
        TimeSpan archiveRetention,
        DateTimeOffset now)
    {
        if (maxFileBytes <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(maxFileBytes));
        }
        if (maxArchiveFiles < 0)
        {
            throw new ArgumentOutOfRangeException(nameof(maxArchiveFiles));
        }

        string fullPath = Path.GetFullPath(filePath);
        string directory = Path.GetDirectoryName(fullPath)
            ?? throw new InvalidOperationException("Не удалось определить каталог журнала.");
        if (!Directory.Exists(directory))
        {
            return;
        }

        string baseName = Path.GetFileNameWithoutExtension(fullPath);
        string extension = Path.GetExtension(fullPath);
        DateTime cutoffUtc = now.Subtract(archiveRetention).UtcDateTime;
        foreach (string archive in Directory.EnumerateFiles(
                     directory,
                     $"{baseName}.*{extension}",
                     SearchOption.TopDirectoryOnly))
        {
            if (!TryGetArchiveIndex(archive, baseName, extension, out int archiveIndex))
            {
                continue;
            }
            if (archiveIndex > maxArchiveFiles ||
                File.GetLastWriteTimeUtc(archive) < cutoffUtc)
            {
                File.Delete(archive);
            }
        }

        if (!File.Exists(fullPath) || new FileInfo(fullPath).Length < maxFileBytes)
        {
            return;
        }
        if (maxArchiveFiles == 0)
        {
            File.Delete(fullPath);
            return;
        }

        for (int index = maxArchiveFiles; index >= 1; index--)
        {
            string destination = ArchivePath(fullPath, index);
            string source = index == 1 ? fullPath : ArchivePath(fullPath, index - 1);
            if (!File.Exists(source))
            {
                continue;
            }
            File.Move(source, destination, overwrite: true);
        }
    }

    private static string ArchivePath(string filePath, int index)
    {
        string directory = Path.GetDirectoryName(filePath)!;
        string baseName = Path.GetFileNameWithoutExtension(filePath);
        string extension = Path.GetExtension(filePath);
        return Path.Combine(directory, $"{baseName}.{index}{extension}");
    }

    private static bool TryGetArchiveIndex(
        string archivePath,
        string baseName,
        string extension,
        out int index)
    {
        string filename = Path.GetFileName(archivePath);
        string prefix = baseName + ".";
        if (!filename.StartsWith(prefix, StringComparison.OrdinalIgnoreCase) ||
            !filename.EndsWith(extension, StringComparison.OrdinalIgnoreCase))
        {
            index = 0;
            return false;
        }

        string value = filename[prefix.Length..^extension.Length];
        return int.TryParse(value, out index) && index > 0;
    }
}
