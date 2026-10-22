using System.Net;
using System.Net.Http.Json;

namespace RagCloudFiles;

internal static class AccessDeniedTest
{
    internal sealed class Handler : HttpMessageHandler
    {
        internal int Requests;
        internal int DeltaRequests;
        internal int SnapshotRequests;
        internal bool AllowFolders;
        protected override Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken token)
        {
            if (request.RequestUri!.AbsolutePath.EndsWith("/changes"))
            {
                if (request.RequestUri.Query.EndsWith("since=")) SnapshotRequests++;
                else DeltaRequests++;
                return Task.FromResult(new HttpResponseMessage(HttpStatusCode.OK)
                {
                    Content = JsonContent.Create(new ChangePage { NextCursor = "cursor", AclRevision = "first" }),
                });
            }
            if (request.RequestUri!.AbsolutePath.EndsWith("/heartbeat"))
                return Task.FromResult(new HttpResponseMessage(HttpStatusCode.OK));
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
            CheckRemoval(!provider._localScanIncomplete, "one denied path invalidated the local tree scan");
            CheckRemoval(provider._pendingLocalChanges.Count == 21, "failed paths not queued individually");
            CheckRemoval(provider._pendingLocalChanges.Values.All(at => at > DateTimeOffset.UtcNow),
                "deferred paths lost their backoff");
            provider._lastFullSnapshot = DateTimeOffset.UtcNow.AddMinutes(-6);
            provider._nextCacheCheck = DateTimeOffset.UtcNow.AddHours(1);
            provider._cursor = "cursor";
            DateTimeOffset lastRecovery = provider._lastLocalRecovery;
            await provider.SyncNowAsync(CancellationToken.None);
            CheckRemoval(handler.DeltaRequests == 1 && handler.SnapshotRequests == 0,
                "single-path denial triggered a full catalog download");
            CheckRemoval(provider._lastLocalRecovery == lastRecovery, "single-path denial rescanned the local tree");
            CheckRemoval(provider._status.Current.State == ClientRunState.Error
                && provider._status.Current.LastSync is null, "delta success concealed denied files");

            // A genuinely incomplete tree scan still repairs locally, without refetching the catalog.
            provider._localScanIncomplete = true;
            provider._lastLocalRecovery = DateTimeOffset.UtcNow.AddMinutes(-6);
            await provider.SyncNowAsync(CancellationToken.None);
            CheckRemoval(handler.DeltaRequests == 2 && handler.SnapshotRequests == 0
                && provider._lastLocalRecovery > lastRecovery && !provider._localScanIncomplete,
                "local scan repair downloaded the remote namespace or was skipped");
            Interlocked.Increment(ref provider._localChangeOverflowVersion);
            provider._lastLocalRecovery = DateTimeOffset.UtcNow.AddMinutes(-6);
            await provider.SyncNowAsync(CancellationToken.None);
            CheckRemoval(!provider.NeedsLocalRecovery && handler.SnapshotRequests == 0,
                "watcher overflow did not recover locally");
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

            // A vanished unsent file is cleared by the same bounded watcher queue, not a full scan.
            File.Delete(Path.Combine(root, "blocked", "19.txt"));
            provider._pendingLocalChanges["blocked/19.txt"] = DateTimeOffset.UtcNow.AddSeconds(-2);
            using (CancellationTokenSource timeout = new(TimeSpan.FromSeconds(2)))
            {
                try { await provider.LocalChangeLoopAsync(timeout.Token); }
                catch (OperationCanceledException) when (timeout.IsCancellationRequested) { }
            }
            CheckRemoval(!provider._pendingLocalChanges.ContainsKey("blocked/19.txt"),
                "isolated retry did not clear a removed local file");
            CheckRemoval(handler.Requests == 1 && handler.SnapshotRequests == 0,
                "isolated retry touched unrelated remote entries");
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
