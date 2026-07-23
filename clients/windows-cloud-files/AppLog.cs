using System.Text;

namespace RagCloudFiles;

internal static class AppLog
{
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
}
