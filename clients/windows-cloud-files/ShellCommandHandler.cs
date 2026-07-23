using System.Diagnostics;
using System.Windows.Forms;

namespace RagCloudFiles;

internal static class ShellCommandHandler
{
    private static readonly HashSet<string> SupportedCommands = new(
        ["share", "copy-link", "manage-access", "keep-offline"],
        StringComparer.OrdinalIgnoreCase);

    public static bool IsRequested(IReadOnlyDictionary<string, string> options) =>
        options.ContainsKey("shell-command");

    public static bool IsSupported(string command) => SupportedCommands.Contains(command);

    public static async Task<int> RunAsync(
        IReadOnlyDictionary<string, string> options,
        ConfigStore store,
        CancellationToken cancellationToken)
    {
        string command = options.GetValueOrDefault("shell-command", "").Trim();
        if (!IsSupported(command))
        {
            throw new ArgumentException($"Неизвестная команда Проводника: {command}");
        }

        ProviderConfig config = store.LoadConfig();
        string localPath = options.GetValueOrDefault("shell-path", "").Trim().Trim('"');
        string cloudPath = GetCloudPath(config.RootPath, localPath);

        switch (command.ToLowerInvariant())
        {
            case "share":
            case "manage-access":
                OpenBrowser(BuildExplorerUri(
                    config.Server,
                    cloudPath,
                    isFolder: Directory.Exists(localPath),
                    openShare: true));
                return 0;
            case "copy-link":
                Clipboard.SetText(BuildExplorerUri(
                    config.Server,
                    cloudPath,
                    isFolder: Directory.Exists(localPath),
                    openShare: false).AbsoluteUri);
                return 0;
            case "keep-offline":
                await KeepOfflineAsync(config, store, localPath, cloudPath, cancellationToken);
                return 0;
            default:
                throw new ArgumentException($"Неизвестная команда Проводника: {command}");
        }
    }

    public static Uri BuildExplorerUri(
        string server,
        string cloudPath,
        bool isFolder,
        bool openShare)
    {
        string normalizedServer = server.TrimEnd('/');
        if (!Uri.TryCreate(normalizedServer, UriKind.Absolute, out Uri? baseUri) ||
            baseUri.Scheme is not ("http" or "https"))
        {
            throw new InvalidDataException("В настройках клиента указан недопустимый адрес сервера.");
        }

        string query = $"path={Uri.EscapeDataString(CloudPath.Normalize(cloudPath))}"
            + $"&kind={(isFolder ? "folder" : "file")}";
        if (openShare)
        {
            query += "&share=1";
        }
        return new Uri($"{normalizedServer}/explorer?{query}");
    }

    public static string GetCloudPath(string rootPath, string selectedPath)
    {
        if (string.IsNullOrWhiteSpace(selectedPath))
        {
            throw new InvalidDataException("Проводник не передал выбранный файл или папку.");
        }

        string root = Path.GetFullPath(rootPath).TrimEnd(Path.DirectorySeparatorChar);
        string candidate = Path.GetFullPath(selectedPath).TrimEnd(Path.DirectorySeparatorChar);
        if (!candidate.Equals(root, StringComparison.OrdinalIgnoreCase) &&
            !candidate.StartsWith(root + Path.DirectorySeparatorChar, StringComparison.OrdinalIgnoreCase))
        {
            throw new InvalidDataException("Команда доступна только внутри папки RAG Cloud Drive.");
        }
        if (!File.Exists(candidate) && !Directory.Exists(candidate))
        {
            throw new FileNotFoundException("Выбранный облачный объект больше не существует.", candidate);
        }

        string relative = Path.GetRelativePath(root, candidate);
        return relative == "."
            ? ""
            : CloudPath.Normalize(relative);
    }

    private static async Task KeepOfflineAsync(
        ProviderConfig config,
        ConfigStore store,
        string localPath,
        string cloudPath,
        CancellationToken cancellationToken)
    {
        if (cloudPath.Length == 0)
        {
            config.KeepAllOffline = true;
            config.OfflinePaths.Clear();
        }
        else if (!config.KeepAllOffline)
        {
            config.OfflinePaths.Add(cloudPath);
        }
        store.SaveConfig(config);
        WindowsBootstrap.SavePreferences(config);

        await Task.Run(() =>
        {
            cancellationToken.ThrowIfCancellationRequested();
            bool recursive = Directory.Exists(localPath);
            CloudFilePinning.SetPinState(localPath, pinned: true, recursive);
            IEnumerable<string> files = recursive
                ? Directory.EnumerateFiles(localPath, "*", SearchOption.AllDirectories)
                : [localPath];
            foreach (string file in files)
            {
                cancellationToken.ThrowIfCancellationRequested();
                if (CloudFilePinning.IsPlaceholder(file))
                {
                    CloudFilePinning.SetPinState(file, pinned: true, recursive: false);
                    CloudFilePinning.HydrateFile(file);
                }
            }
            CloudFilePinning.RefreshShell(localPath);
        }, cancellationToken);
    }

    private static void OpenBrowser(Uri uri)
    {
        Process.Start(new ProcessStartInfo(uri.AbsoluteUri)
        {
            UseShellExecute = true,
        });
    }
}
