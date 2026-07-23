using System.Text.Json;
using System.Security.Cryptography;
using System.Text;
using Microsoft.Win32;

namespace RagCloudFiles;

internal sealed class ConfigStore
{
    private static readonly JsonSerializerOptions JsonOptions = new()
    {
        PropertyNameCaseInsensitive = true,
        WriteIndented = true,
    };

    public ConfigStore(string? configPath = null)
    {
        string baseDir = Path.Combine(
            Environment.GetFolderPath(Environment.SpecialFolder.LocalApplicationData),
            "RAGCloudFiles");
        ConfigPath = Path.GetFullPath(configPath ?? Path.Combine(baseDir, "config.json"));
        StatePath = Path.Combine(Path.GetDirectoryName(ConfigPath)!, "state.json");
    }

    public string ConfigPath { get; }

    public string StatePath { get; }

    public ProviderConfig LoadConfig()
    {
        ProviderConfig config = LoadRegistryDefaults();
        if (!File.Exists(ConfigPath))
        {
            return config;
        }

        ProviderConfig saved = JsonSerializer.Deserialize<ProviderConfig>(File.ReadAllText(ConfigPath), JsonOptions)
            ?? new ProviderConfig();
        if (saved.Server.Length > 0)
        {
            config.Server = saved.Server;
        }
        if (saved.RootPath.Length > 0)
        {
            config.RootPath = saved.RootPath;
        }
        if (saved.DeviceId.Length > 0)
        {
            config.DeviceId = saved.DeviceId;
        }
        config.ClientId = saved.ClientId;
        config.PollSeconds = saved.PollSeconds;
        config.KeepAllOffline = saved.KeepAllOffline;
        config.OfflinePaths = new HashSet<string>(
            saved.OfflinePaths ?? [],
            StringComparer.OrdinalIgnoreCase);
        config.StartWithWindows = saved.StartWithWindows;
        config.ProtectedToken = saved.ProtectedToken;
        config.Token = UnprotectToken(saved.ProtectedToken);
        return config;
    }

    public void SaveConfig(ProviderConfig config)
    {
        config.ProtectedToken = ProtectToken(config.Token);
        SaveJson(ConfigPath, config);
    }

    public ProviderState LoadState()
    {
        if (!File.Exists(StatePath))
        {
            return new ProviderState();
        }

        ProviderState state = JsonSerializer.Deserialize<ProviderState>(File.ReadAllText(StatePath), JsonOptions)
            ?? new ProviderState();
        state.ManagedPaths = new HashSet<string>(state.ManagedPaths, StringComparer.OrdinalIgnoreCase);
        state.ManagedVersions = new Dictionary<string, string>(state.ManagedVersions, StringComparer.OrdinalIgnoreCase);
        state.AppliedOfflinePaths = new HashSet<string>(
            state.AppliedOfflinePaths ?? [],
            StringComparer.OrdinalIgnoreCase);
        state.AppliedOfflineVersions = new Dictionary<string, string>(
            state.AppliedOfflineVersions ?? new Dictionary<string, string>(),
            StringComparer.OrdinalIgnoreCase);
        return state;
    }

    public void SaveState(ProviderState state) => SaveJson(StatePath, state);

    private static void SaveJson<T>(string path, T value)
    {
        Directory.CreateDirectory(Path.GetDirectoryName(path)!);
        string temporary = path + ".tmp";
        File.WriteAllText(temporary, JsonSerializer.Serialize(value, JsonOptions));
        File.Move(temporary, path, true);
    }

    private static ProviderConfig LoadRegistryDefaults()
    {
        ProviderConfig config = new();
        using RegistryKey? key = Registry.CurrentUser.OpenSubKey(@"Software\RAGCloudFiles");
        if (key is null)
        {
            return config;
        }

        config.Server = Convert.ToString(key.GetValue("Server"))?.Trim() ?? "";
        string root = Convert.ToString(key.GetValue("RootPath"))?.Trim() ?? "";
        if (root.Length > 0)
        {
            config.RootPath = Environment.ExpandEnvironmentVariables(root);
        }
        string deviceId = Convert.ToString(key.GetValue("DeviceId"))?.Trim() ?? "";
        if (deviceId.Length > 0)
        {
            config.DeviceId = deviceId;
        }
        config.KeepAllOffline = Convert.ToInt32(key.GetValue("KeepAllOffline", 0)) != 0;
        config.StartWithWindows = Convert.ToInt32(key.GetValue("StartWithWindows", 1)) != 0;
        return config;
    }

    private static string ProtectToken(string token)
    {
        if (token.Length == 0)
        {
            return "";
        }
        byte[] protectedBytes = ProtectedData.Protect(
            Encoding.UTF8.GetBytes(token),
            optionalEntropy: null,
            DataProtectionScope.CurrentUser);
        return Convert.ToBase64String(protectedBytes);
    }

    private static string UnprotectToken(string protectedToken)
    {
        if (protectedToken.Length == 0)
        {
            return "";
        }
        try
        {
            byte[] clearBytes = ProtectedData.Unprotect(
                Convert.FromBase64String(protectedToken),
                optionalEntropy: null,
                DataProtectionScope.CurrentUser);
            return Encoding.UTF8.GetString(clearBytes);
        }
        catch (CryptographicException)
        {
            return "";
        }
    }
}
