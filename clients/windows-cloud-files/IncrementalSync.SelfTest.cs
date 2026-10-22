using System.Net;
using System.Net.Http.Json;

namespace RagCloudFiles;

internal sealed partial class CloudFilesProvider
{
    private sealed class DeltaHandler : HttpMessageHandler
    {
        internal int Requests;
        internal bool FailSecondPage = true;
        protected override Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken token)
        {
            Requests++;
            bool first = request.RequestUri!.Query.Contains("since=before");
            if (!first && FailSecondPage)
                return Task.FromResult(new HttpResponseMessage(HttpStatusCode.ServiceUnavailable));
            return Task.FromResult(new HttpResponseMessage(HttpStatusCode.OK)
            {
                Content = JsonContent.Create(new ChangePage
                {
                    AclRevision = "acl", NextCursor = "after",
                    Changes = first ? [new CloudNode { Path = "new-folder", NodeType = "folder" }] : [],
                }),
            });
        }
    }

    internal static async Task TestIncrementalSyncAsync()
    {
        string fixture = Path.Combine(Path.GetTempPath(), "rag-delta-test-" + Guid.NewGuid().ToString("N"));
        string root = Path.Combine(fixture, "root");
        Directory.CreateDirectory(Path.Combine(root, "new-folder"));
        try
        {
            ConfigStore store = new(Path.Combine(fixture, "config", "config.json"));
            ProviderConfig config = new() { RootPath = root, Server = "https://delta.invalid" };
            DeltaHandler handler = new();
            using CloudDriveApi api = new(config.Server, "test", handler);
            await using CloudFilesProvider provider = new(config, store, api);
            provider._aclRevision = "acl";
            provider._cursor = "before";
            provider._state.ManagedPaths.Add("new-folder");
            provider._state.ManagedVersions["new-folder"] = "folder";
            // Missing unrelated placeholder: a full reconciliation would try native creation.
            provider._nodes["unrelated.pdf"] = new CloudNode { Path = "unrelated.pdf", NodeType = "file" };
            provider._state.ManagedPaths.Add("unrelated.pdf");
            provider._state.ManagedVersions["unrelated.pdf"] = "known-version";
            provider._state.LocalFingerprints["unrelated.pdf"] = "known-fingerprint";
            try { await provider.ApplyIncrementalChangesAsync(CancellationToken.None); }
            catch (HttpRequestException ex) when (ex.StatusCode == HttpStatusCode.ServiceUnavailable) { }
            CheckRemoval(provider._pendingRemoteChanges.Contains("new-folder"), "partial delta discarded pending changes");
            handler.FailSecondPage = false;
            await provider.ApplyIncrementalChangesAsync(CancellationToken.None);
            CheckRemoval(provider._pendingRemoteChanges.Count == 0 && provider._visiblePaths.Contains("new-folder"),
                "partial delta did not resume reconciliation");
            CheckRemoval(provider._deferredPlaceholders == 0 && !provider._namespaceIncomplete,
                "delta visited unrelated placeholders");
            CheckRemoval(provider._lastLocalRecovery == DateTimeOffset.MinValue,
                "single remote change triggered a full local scan");
            CheckRemoval(store.LoadState().LocalFingerprints["unrelated.pdf"] == "known-fingerprint",
                "delta lost unrelated state");
        }
        finally { Directory.Delete(fixture, recursive: true); }
    }

    private void TestNativeIncrementalReconcile()
    {
        CloudNode untouched = new() { Path = "delta-untouched.pdf", NodeType = "file", SizeBytes = 123 };
        _nodes[untouched.Path] = untouched;
        ReconcileNamespace(CancellationToken.None);
        RemoveManagedPath(untouched.Path);
        CloudNode added = new() { Path = "delta-parent/sub/new.pdf", NodeType = "file", SizeBytes = 321 };
        _nodes[added.Path] = added;
        ReconcileNamespace(CancellationToken.None, new(StringComparer.OrdinalIgnoreCase) { added.Path });
        CheckRemoval(_createdPlaceholders == 3 && !_namespaceIncomplete,
            "delta failed to create synthetic ancestors and a file");
        CheckRemoval(!File.Exists(CloudPath.LocalPath(_root, untouched.Path)), "delta traversed unrelated files");
        CheckRemoval(CloudFilePinning.ReadPlaceholderState(CloudPath.LocalPath(_root, added.Path), out _),
            "delta file has invalid metadata");
        CheckRemoval(_state.ManagedPaths.Contains(untouched.Path), "delta dropped unrelated ownership");
        _nodes.Remove(added.Path);
        ReconcileNamespace(CancellationToken.None, new(StringComparer.OrdinalIgnoreCase) { added.Path });
        CheckRemoval(!_state.ManagedPaths.Contains(added.Path)
            && !Directory.Exists(CloudPath.LocalPath(_root, "delta-parent")), "delta failed to retire removed subtree");
        CheckRemoval(Directory.GetFiles(Path.Combine(_root, PlaceholderRecovery.FolderName), "new.pdf",
            SearchOption.AllDirectories).Length == 1, "delta removed data without preservation");
        _namespaceIncomplete = true;
        ReconcileNamespace(CancellationToken.None, new(StringComparer.OrdinalIgnoreCase) { "absent.pdf" });
        CheckRemoval(_namespaceIncomplete, "successful delta concealed an unrelated deferred repair");
        ReconcileNamespace(CancellationToken.None);
        CheckRemoval(!_namespaceIncomplete && File.Exists(CloudPath.LocalPath(_root, untouched.Path)),
            "full local repair failed after a delta");
        Console.WriteLine("Native incremental namespace: ancestors, placeholders, unrelated state and preserved removals verified.");
    }
}
