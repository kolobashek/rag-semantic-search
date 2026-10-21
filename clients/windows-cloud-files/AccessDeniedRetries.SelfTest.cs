using System.Net;
using System.Net.Http.Json;

namespace RagCloudFiles;

internal static class AccessDeniedTest
{
    internal sealed class Handler : HttpMessageHandler
    {
        internal int Requests;
        internal bool AllowFolders;
        protected override Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken token)
        {
            Requests++;
            HttpResponseMessage response = new(HttpStatusCode.Forbidden);
            if (AllowFolders && request.RequestUri!.AbsolutePath.EndsWith("/folders"))
                response = new(HttpStatusCode.OK)
                {
                    Content = JsonContent.Create(new CloudNode { Path = "blocked", NodeType = "folder" }),
                };
            return Task.FromResult(response);
        }
    }
}

internal sealed partial class CloudFilesProvider
{
    internal static async Task TestAccessDeniedSyncAsync()
    {
        string fixture = Path.Combine(Path.GetTempPath(), "rag-denied-test-" + Guid.NewGuid().ToString("N"));
        string root = Path.Combine(fixture, "root");
        Directory.CreateDirectory(Path.Combine(root, "blocked"));
        for (int i = 0; i < 20; i++) File.WriteAllText(Path.Combine(root, "blocked", $"{i}.txt"), "local data");
        ConfigStore store = new(Path.Combine(fixture, "config", "config.json"));
        ProviderConfig config = new() { RootPath = root, Server = "https://denied.invalid" };
        AccessDeniedTest.Handler handler = new();
        try
        {
            using CloudDriveApi api = new(config.Server, "test", handler);
            await using CloudFilesProvider provider = new(config, store, api);
            provider.UpdateAclRevision("first");
            await provider.RecoverLocalChangesAsync(CancellationToken.None);
            CheckRemoval(handler.Requests == 1, "each child retried denied parent creation");
            CheckRemoval(provider._status.Current.State == ClientRunState.Error, "denial reported synchronized");
            CheckRemoval(provider._status.Current.LastError.Contains("blocked"), "denied path hidden");
            await provider.RecoverLocalChangesAsync(CancellationToken.None);
            CheckRemoval(handler.Requests == 1, "full scan bypassed cooldown");
            for (int i = 0; i < 20; i++)
            {
                try { await provider.SyncLocalPathAsync($"blocked/{i}.txt", false, CancellationToken.None); }
                catch (AccessDeniedSyncException ex)
                {
                    CheckRemoval(!ex.RequestAttempted, "watcher retried denied parent");
                }
                CheckRemoval(File.ReadAllText(Path.Combine(root, "blocked", $"{i}.txt")) == "local data",
                    "denial changed local file");
            }
            CheckRemoval(handler.Requests == 1, "watcher bypassed cooldown");
            CheckRemoval(store.LoadState().ManagedPaths.Count == 0, "denied paths marked synchronized");
            provider.UpdateAclRevision("first");
            try { await provider.EnsureRemoteFoldersAsync("blocked", CancellationToken.None); }
            catch (AccessDeniedSyncException) { }
            CheckRemoval(handler.Requests == 1, "unchanged ACL cleared cooldown");
            provider._pendingLocalChanges["blocked/0.txt"] = DateTimeOffset.UtcNow.AddMinutes(5);
            provider._explicitLocalDeletes["blocked/0.txt"] = 0;
            provider.UpdateAclRevision("second");
            CheckRemoval(provider._pendingLocalChanges["blocked/0.txt"] <= DateTimeOffset.UtcNow,
                "ACL change did not wake pending mutation");
            CheckRemoval(provider._explicitLocalDeletes.ContainsKey("blocked/0.txt"), "delete intent lost");
            handler.AllowFolders = true;
            await provider.EnsureRemoteFoldersAsync("blocked", CancellationToken.None);
            CheckRemoval(handler.Requests == 2 && provider.HasRemoteNode("blocked"),
                "changed ACL did not resume parent creation");
            CheckRemoval(provider._status.Current.State == ClientRunState.Error,
                "parent success hid unsynchronized child errors");
        }
        finally
        {
            if (!Path.GetFullPath(fixture).StartsWith(Path.GetFullPath(Path.GetTempPath()), StringComparison.OrdinalIgnoreCase))
                throw new InvalidOperationException("Invalid test cleanup path.");
            Directory.Delete(fixture, recursive: true);
        }
    }
}
