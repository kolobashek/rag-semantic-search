namespace RagCloudFiles;

internal static class AppDefaults
{
    public const string ProductName = "RAG Cloud Files";
    public const string Server = "https://cloud.tsk-nsk.ru";
    public static string Version { get; } = typeof(AppDefaults).Assembly.GetName().Version?.ToString(3)
        ?? throw new InvalidOperationException("Client assembly version is missing.");
}
