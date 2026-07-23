using System.Text.Json;

namespace RagCloudFiles;

internal static class SelfTest
{
    public static void Run()
    {
        Equal("https://cloud.tsk-nsk.ru", new ProviderConfig().Server);
        Equal(false, WindowsBootstrap.IsInteractiveInstall(["--self-test"]));
        Equal("Folder/file.txt", CloudPath.Normalize("/Folder\\file.txt/"));
        Equal("Folder", CloudPath.Parent("Folder/file.txt"));
        Equal(2, CloudPath.Depth("Folder/file.txt"));

        string unicodePath = "Документы/Смета 2026.xlsx";
        Equal(unicodePath, FileIdentityCodec.Decode(FileIdentityCodec.Encode(unicodePath)));
        Throws<InvalidDataException>(() => CloudPath.Normalize("Folder/../secret.txt"));
        Throws<InvalidDataException>(() => FileIdentityCodec.Decode("bad"u8));

        ChangePage page = JsonSerializer.Deserialize<ChangePage>("""
            {
              "next_cursor": "cursor-1",
              "acl_revision": "acl-1",
              "changes": [
                {
                  "node_type": "file",
                  "path": "Folder/file.txt",
                  "size_bytes": 123,
                  "checksum": "abc"
                }
              ]
            }
            """) ?? throw new InvalidOperationException("JSON self-test failed.");
        Equal("cursor-1", page.NextCursor);
        Equal("acl-1", page.AclRevision);
        Equal(123L, page.Changes.Single().SizeBytes);

        string temporary = Path.Combine(Path.GetTempPath(), "rag-cloud-files-self-test-" + Guid.NewGuid().ToString("N"));
        try
        {
            ConfigStore store = new(Path.Combine(temporary, "config.json"));
            ProviderConfig config = new()
            {
                Server = "https://catalog.example",
                DeviceId = "device-1",
                Token = "secret-device-token",
            };
            store.SaveConfig(config);
            Equal("device-1", store.LoadConfig().DeviceId);
            Equal("secret-device-token", store.LoadConfig().Token);
            if (File.ReadAllText(store.ConfigPath).Contains("secret-device-token", StringComparison.Ordinal))
            {
                throw new InvalidOperationException("Device token was stored in clear text.");
            }
            ProviderState state = new() { ManagedPaths = new HashSet<string>([unicodePath]) };
            store.SaveState(state);
            if (!store.LoadState().ManagedPaths.Contains(unicodePath.ToUpperInvariant()))
            {
                throw new InvalidOperationException("State path comparer is not case-insensitive.");
            }
        }
        finally
        {
            if (Directory.Exists(temporary))
            {
                Directory.Delete(temporary, true);
            }
        }
    }

    private static void Equal<T>(T expected, T actual)
    {
        if (!EqualityComparer<T>.Default.Equals(expected, actual))
        {
            throw new InvalidOperationException($"Expected {expected}, got {actual}.");
        }
    }

    private static void Throws<TException>(Action action)
        where TException : Exception
    {
        try
        {
            action();
        }
        catch (TException)
        {
            return;
        }

        throw new InvalidOperationException($"Expected {typeof(TException).Name}.");
    }
}
