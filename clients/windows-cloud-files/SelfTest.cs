using System.Text.Json;

namespace RagCloudFiles;

internal static class SelfTest
{
    public static void Run()
    {
        Equal(true, SyncRootRegistrar.CanReuseRegistration("Provider!A", "provider!a"));
        Equal(false, SyncRootRegistrar.CanReuseRegistration("other", "Provider!A"));
        Equal(true, PlaceholderRecovery.IsCorruptMetadata(unchecked((int)0x8007016B)));
        Equal(false, PlaceholderRecovery.IsCorruptMetadata(unchecked((int)0x80070005)));
        LocalTreeScan scan = LocalTreeScan.Read("root", (_, _) => { },
            path => path switch
            {
                "root" => ["bad", "good"],
                "bad" => throw new IOException("Cloud provider unavailable"),
                "good" => ["good/file.pdf"],
                _ => [],
            }, path => path.EndsWith(".pdf") ? FileAttributes.Archive : FileAttributes.Directory);
        Equal(1, scan.Unreadable.Count);
        Equal("good/file.pdf", scan.Files.Single());
        int attempts = 0;
        int repairs = 0;
        LocalTreeScan recovered = LocalTreeScan.Read("root", (_, _) => { },
            path => attempts++ == 0 ? throw new IOException("Old placeholder") : ["file.pdf"],
            _ => FileAttributes.Archive, _ => { repairs++; return true; });
        Equal(0, recovered.Unreadable.Count);
        Equal(1, repairs);
        Equal(1, recovered.Files.Count);
        repairs = 0;
        LocalTreeScan persistent = LocalTreeScan.Read("root", (_, _) => { },
            _ => throw new IOException("Still unavailable"), _ => FileAttributes.Directory,
            _ => { repairs++; return true; });
        Equal(1, repairs);
        Equal(1, persistent.Unreadable.Count);
        TestSessionAuthorizationAsync().GetAwaiter().GetResult();
        TestNetworkRecoveryAsync().GetAwaiter().GetResult();
        TestSnapshotRetryAsync().GetAwaiter().GetResult();
        TestNamespaceRecovery();
        Equal("https://cloud.tsk-nsk.ru", new ProviderConfig().Server);
        Equal(false, WindowsBootstrap.IsInteractiveInstall(["--self-test"]));
        Equal("Folder/file.txt", CloudPath.Normalize("/Folder\\file.txt/"));
        Equal("Folder", CloudPath.Parent("Folder/file.txt"));
        Equal(2, CloudPath.Depth("Folder/file.txt"));
        Equal(true, ClientUpdater.IsNewerVersion("0.3.0", "0.3.1"));
        Equal(false, ClientUpdater.IsNewerVersion("0.3.0", "0.3.0"));
        Equal(false, ClientUpdater.IsNewerVersion("0.3.0", "invalid"));
        Equal(true, ClientUpdater.IsValidSha256(new string('a', 64)));
        Equal(false, ClientUpdater.IsValidSha256("not-a-hash"));
        Equal("R", VirtualDriveManager.NormalizeDriveLetter("r:"));
        Equal("R", VirtualDriveManager.NormalizeDriveLetter("invalid"));
        Equal("S", VirtualDriveManager.CandidateLetters("S").First());
        Equal(20, CachePolicy.NormalizeMaxCacheSizeGb(0));
        Equal(10, CachePolicy.NormalizeMinimumFreeSpaceGb(0));
        Equal(
            15L,
            CachePolicy.CalculateBytesToReclaim(
                allocatedBytes: 35,
                availableFreeBytes: 100,
                maximumCacheBytes: 20,
                minimumFreeBytes: 10));
        Equal(
            7L,
            CachePolicy.CalculateBytesToReclaim(
                allocatedBytes: 10,
                availableFreeBytes: 3,
                maximumCacheBytes: 20,
                minimumFreeBytes: 10));
        DateTimeOffset cacheNow = DateTimeOffset.UtcNow;
        IReadOnlyList<CacheEntry> evictionCandidates = CachePolicy.SelectEvictionCandidates(
            [
                new("recent.txt", 100, cacheNow.AddMinutes(-2), false, false),
                new("pinned.txt", 100, cacheNow.AddDays(-3), true, false),
                new("open.txt", 100, cacheNow.AddDays(-4), false, true),
                new("older.txt", 100, cacheNow.AddDays(-2), false, false),
                new("oldest.txt", 100, cacheNow.AddDays(-3), false, false),
            ],
            cacheNow);
        Equal("oldest.txt", evictionCandidates[0].CloudPath);
        Equal("older.txt", evictionCandidates[1].CloudPath);
        Equal(2, evictionCandidates.Count);
        Equal(
            true,
            SyncRootRegistrar.BuildSyncRootId("https://catalog.example")
                .StartsWith("TSK.RagCloudFiles!S-", StringComparison.Ordinal));
        Equal(true, CloudFilesProvider.ShouldPropagateDelete(false, false, true));
        Equal(false, CloudFilesProvider.ShouldPropagateDelete(true, false, true));
        Equal(false, CloudFilesProvider.ShouldPropagateDelete(false, true, true));
        Equal(false, CloudFilesProvider.ShouldPropagateDelete(false, false, false));

        string unicodePath = "Документы/Смета 2026.xlsx";
        Equal(unicodePath, FileIdentityCodec.Decode(FileIdentityCodec.Encode(unicodePath)));
        Throws<InvalidDataException>(() => CloudPath.Normalize("Folder/../secret.txt"));
        Throws<InvalidDataException>(() => FileIdentityCodec.Decode("bad"u8));
        Equal(true, ShellCommandHandler.IsSupported("share"));
        Equal(false, ShellCommandHandler.IsSupported("delete"));
        Equal(
            "https://catalog.example/explorer?path=%D0%94%D0%BE%D0%BA%D1%83%D0%BC%D0%B5%D0%BD%D1%82%D1%8B%2F%D0%A1%D0%BC%D0%B5%D1%82%D0%B0%202026.xlsx&kind=file&share=1",
            ShellCommandHandler.BuildExplorerUri(
                "https://catalog.example/",
                unicodePath,
                isFolder: false,
                openShare: true).AbsoluteUri);

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
                KeepAllOffline = true,
                OfflinePaths = new HashSet<string>(["Документы"], StringComparer.OrdinalIgnoreCase),
                MaxCacheSizeGb = 24,
                MinimumFreeSpaceGb = 8,
                StartWithWindows = false,
                MountAsDrive = true,
                DriveLetter = "S",
            };
            store.SaveConfig(config);
            Equal("device-1", store.LoadConfig().DeviceId);
            Equal("secret-device-token", store.LoadConfig().Token);
            Equal(true, store.LoadConfig().KeepAllOffline);
            Equal(true, store.LoadConfig().OfflinePaths.Contains("документы"));
            Equal(24, store.LoadConfig().MaxCacheSizeGb);
            Equal(8, store.LoadConfig().MinimumFreeSpaceGb);
            Equal(false, store.LoadConfig().StartWithWindows);
            Equal(true, store.LoadConfig().MountAsDrive);
            Equal("S", store.LoadConfig().DriveLetter);
            string root = Path.Combine(temporary, "root");
            string localFile = Path.Combine(root, "Документы", "Смета 2026.xlsx");
            Directory.CreateDirectory(Path.GetDirectoryName(localFile)!);
            File.WriteAllText(localFile, "test");
            Equal(unicodePath, ShellCommandHandler.GetCloudPath(root, localFile));
            Equal("", ShellCommandHandler.GetCloudPath(root, root));
            Throws<InvalidDataException>(() =>
                ShellCommandHandler.GetCloudPath(root, Path.Combine(temporary, "outside.txt")));
            CloudNode matchingRemote = new()
            {
                NodeType = "file",
                Path = unicodePath,
                SizeBytes = 4,
                Checksum = "9f86d081884c7d659a2feaa0c55ad015"
                    + "a3bf4f1b2b0b822cd15d6c15b0f00a08",
            };
            Equal(
                true,
                CloudFilesProvider.RemoteContentMatchesAsync(
                        matchingRemote,
                        localFile,
                        CancellationToken.None)
                    .GetAwaiter()
                    .GetResult());
            matchingRemote.Checksum = new string('0', 64);
            Equal(
                false,
                CloudFilesProvider.RemoteContentMatchesAsync(
                        matchingRemote,
                        localFile,
                        CancellationToken.None)
                    .GetAwaiter()
                    .GetResult());
            string preserved = PlaceholderRecovery.Preserve(root, unicodePath);
            Equal("test", File.ReadAllText(preserved));
            Equal(false, File.Exists(localFile));
            Throws<InvalidDataException>(() => PlaceholderRecovery.Preserve(root, "../outside.txt"));
            string testLog = Path.Combine(temporary, "logs", "RagCloudFiles.log");
            Directory.CreateDirectory(Path.GetDirectoryName(testLog)!);
            string privateLog = "Authorization: Bearer private-token\npassword=secret-value\n"
                + "https://cloud.example/auth/device?code=ABCD-1234; code ABCD-1234\n"
                + "https://store.example/?X-Amz-Signature=signing-secret\n";
            string cleaned = ClientDiagnostics.Redact(privateLog);
            foreach (string secret in new[] { "private-token", "secret-value", "ABCD-1234", "signing-secret" })
            {
                Equal(false, cleaned.Contains(secret, StringComparison.Ordinal));
            }
            File.WriteAllText(testLog, new string('x', 300000) + "\nlast entry\n");
            Equal("last entry\n", ClientDiagnostics.ReadTail(testLog));
            File.WriteAllText(testLog, privateLog);
            Equal(cleaned, ClientDiagnostics.ReadTail(testLog));
            TestDiagnosticsAsync(testLog).GetAwaiter().GetResult();
            File.WriteAllText(testLog, "current log");
            File.WriteAllText(
                Path.Combine(Path.GetDirectoryName(testLog)!, "RagCloudFiles.1.log"),
                "recent archive");
            string expiredArchive = Path.Combine(
                Path.GetDirectoryName(testLog)!,
                "RagCloudFiles.2.log");
            File.WriteAllText(expiredArchive, "expired archive");
            File.SetLastWriteTimeUtc(expiredArchive, DateTime.UtcNow.AddDays(-31));
            AppLog.MaintainFiles(
                testLog,
                maxFileBytes: 4,
                maxArchiveFiles: 2,
                archiveRetention: TimeSpan.FromDays(30),
                now: DateTimeOffset.UtcNow);
            Equal(false, File.Exists(testLog));
            Equal(
                "current log",
                File.ReadAllText(Path.Combine(
                    Path.GetDirectoryName(testLog)!,
                    "RagCloudFiles.1.log")));
            Equal(
                "recent archive",
                File.ReadAllText(Path.Combine(
                    Path.GetDirectoryName(testLog)!,
                    "RagCloudFiles.2.log")));
            Equal(
                false,
                File.Exists(Path.Combine(
                    Path.GetDirectoryName(testLog)!,
                    "RagCloudFiles.3.log")));
            if (File.ReadAllText(store.ConfigPath).Contains("secret-device-token", StringComparison.Ordinal))
            {
                throw new InvalidOperationException("Device token was stored in clear text.");
            }
            ProviderState state = new()
            {
                ManagedPaths = new HashSet<string>([unicodePath]),
                AppliedOfflinePaths = new HashSet<string>(["Документы"]),
                AppliedOfflineVersions = new Dictionary<string, string>
                {
                    [unicodePath] = "version-1",
                },
                LocalFingerprints = new Dictionary<string, string>
                {
                    [unicodePath] = "123:456",
                },
                LastAccessedUtc = new Dictionary<string, string>
                {
                    [unicodePath] = cacheNow.ToString("O"),
                },
            };
            store.SaveState(state);
            if (!store.LoadState().ManagedPaths.Contains(unicodePath.ToUpperInvariant()))
            {
                throw new InvalidOperationException("State path comparer is not case-insensitive.");
            }
            Equal(true, store.LoadState().AppliedOfflinePaths.Contains("документы"));
            Equal("version-1", store.LoadState().AppliedOfflineVersions[unicodePath.ToUpperInvariant()]);
            Equal("123:456", store.LoadState().LocalFingerprints[unicodePath.ToUpperInvariant()]);
            Equal(
                cacheNow.ToString("O"),
                store.LoadState().LastAccessedUtc[unicodePath.ToUpperInvariant()]);

            ClientStatusModel status = new();
            status.BeginTransfer(unicodePath);
            Equal(1, status.Current.ActiveTransfers);
            status.EndTransfer(unicodePath);
            Equal(ClientRunState.UpToDate, status.Current.State);

            using Icon baseIcon = (Icon)SystemIcons.Application.Clone();
            foreach (ClientRunState runState in Enum.GetValues<ClientRunState>())
            {
                using Icon statusIcon = TrayIconFactory.Create(baseIcon, runState);
                Equal(new Size(32, 32), statusIcon.Size);
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

    private static async Task TestSessionAuthorizationAsync()
    {
        SessionAuthorization.ValidateClientIdentity("", "first-client");
        SessionAuthorization.ValidateClientIdentity("same", "same");
        Throws<InvalidOperationException>(() => SessionAuthorization.ValidateClientIdentity("old", "other"));
        int prompts = 0;
        int attempts = 0;
        Task<string> Authorize()
        {
            prompts++;
            return Task.FromResult("new-token");
        }
        Equal("client", await SessionAuthorization.RegisterAsync("valid", _ => Task.FromResult("client"), Authorize));
        Equal(0, prompts);
        Equal("new-token", await SessionAuthorization.RegisterAsync("", token => Task.FromResult(token), Authorize));
        Equal(1, prompts);
        Equal("client", await SessionAuthorization.RegisterAsync("expired", token =>
        {
            attempts++;
            if (token == "expired")
                throw new HttpRequestException("expired", null, System.Net.HttpStatusCode.Unauthorized);
            Equal("new-token", token);
            return Task.FromResult("client");
        }, Authorize));
        Equal(2, attempts);
        Equal(2, prompts);

        foreach (System.Net.HttpStatusCode? code in new System.Net.HttpStatusCode?[]
                 { System.Net.HttpStatusCode.Forbidden, System.Net.HttpStatusCode.ServiceUnavailable, null })
        {
            Throws<HttpRequestException>(() => SessionAuthorization.RegisterAsync("saved",
                _ => throw new HttpRequestException("failure", null, code), Authorize).GetAwaiter().GetResult());
        }
        Equal(2, prompts);
        Throws<HttpRequestException>(() => SessionAuthorization.RegisterAsync("expired",
            _ => throw new HttpRequestException("invalid", null, System.Net.HttpStatusCode.Unauthorized),
            Authorize).GetAwaiter().GetResult());
        Equal(3, prompts);
        Throws<OperationCanceledException>(() => SessionAuthorization.RegisterAsync("saved",
            _ => throw new OperationCanceledException(), Authorize).GetAwaiter().GetResult());
        Equal(3, prompts);

        int notifications = 0;
        using SessionExpiryHandler handler = new(() => notifications++, new UnauthorizedTestHandler());
        using HttpClient client = new(handler);
        using (await client.GetAsync("https://test.invalid/one")) { }
        using (await client.GetAsync("https://test.invalid/two")) { }
        Equal(1, notifications);
        handler.Reset();
        using (await client.GetAsync("https://test.invalid/three")) { }
        Equal(2, notifications);
    }

    private static async Task TestSnapshotRetryAsync()
    {
        SnapshotRetryHandler handler = new();
        using CloudDriveApi api = new("https://test.invalid", "test-token", handler);
        int retries = 0;
        api.RetryingRead += (_, _) => retries++;
        VisibleSnapshot snapshot = await api.GetVisibleSnapshotAsync(CancellationToken.None);
        Equal(1, retries);
        Equal(2, snapshot.Nodes.Count);
        Equal(handler.Urls[1], handler.Urls[2]); // Retry the failed page, not the whole snapshot.
        Equal(4, handler.Urls.Count);
        using CloudDriveApi denied = new("https://test.invalid", "test-token", new UnauthorizedTestHandler());
        try
        {
            await denied.GetVisibleSnapshotAsync(CancellationToken.None);
            throw new InvalidOperationException("Unauthorized snapshot accepted");
        }
        catch (HttpRequestException error) { Equal(System.Net.HttpStatusCode.Unauthorized, error.StatusCode!.Value); }
    }

    private sealed class SnapshotRetryHandler : HttpMessageHandler
    {
        public List<string> Urls = [];
        protected override Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken token)
        {
            Urls.Add(request.RequestUri!.ToString());
            if (Urls.Count == 2) return Task.FromResult(new HttpResponseMessage(System.Net.HttpStatusCode.BadGateway));
            string path = Urls.Count == 1 ? "a.pdf" : "b.pdf";
            string cursor = Urls.Count == 1 ? "page2" : "end";
            object[] nodes = Urls.Count == 4 ? [] : [new { path, node_type = "file" }];
            return Task.FromResult(new HttpResponseMessage(System.Net.HttpStatusCode.OK)
            {
                Content = new StringContent(JsonSerializer.Serialize(new { changes = nodes, next_cursor = cursor, acl_revision = "stable" })),
            });
        }
    }

    private static void TestNamespaceRecovery()
    {
        string temporary = Path.Combine(Path.GetTempPath(), "rag-recovery-test-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(temporary);
        try
        {
            string original = Path.Combine(temporary, "original");
            Directory.CreateDirectory(original);
            File.WriteAllText(Path.Combine(original, "local.txt"), "unsynced content");
            ConfigStore store = new(Path.Combine(temporary, "config", "config.json"));
            store.SaveState(new ProviderState());
            string originalState = File.ReadAllText(store.StatePath);
            ProviderConfig config = new() { RootPath = original, Token = "test-token", KeepAllOffline = false };
            config.OfflinePaths.Add("pinned");
            string request = Guid.NewGuid().ToString("N");
            NamespaceRecovery.Prepare(config, store, request);
            Equal("unsynced content", File.ReadAllText(Path.Combine(original, "local.txt")));
            Equal(originalState, File.ReadAllText(store.StatePath));
            Equal(original, config.PreservedRoot);
            Equal(0, Directory.GetFileSystemEntries(config.RootPath).Length);
            ProviderConfig restored = new ConfigStore(store.ConfigPath).LoadConfig();
            Equal(request, restored.RootKey);
            Equal("test-token", restored.Token);
            Equal(true, restored.OfflinePaths.Contains("pinned"));
            ConfigStore newStore = new(store.ConfigPath);
            newStore.LoadConfig();
            Equal(false, File.Exists(newStore.StatePath));
            NamespaceRecovery.Prepare(restored, newStore, request);
            Throws<InvalidDataException>(() => NamespaceRecovery.Prepare(restored, newStore, "../invalid"));
            string collision = Guid.NewGuid().ToString("N");
            Directory.CreateDirectory(Path.Combine(temporary, "RAG Cloud Drive - " + collision[..8]));
            Throws<IOException>(() => NamespaceRecovery.Prepare(restored, newStore, collision));
        }
        finally { Directory.Delete(temporary, recursive: true); }
    }

    private static async Task TestNetworkRecoveryAsync()
    {
        List<double> waits = [];
        int calls = 0;
        int result = await NetworkRecovery.ExecuteAsync(() =>
        {
            if (++calls < 8) throw new HttpRequestException("502", null, System.Net.HttpStatusCode.BadGateway);
            return Task.FromResult(42);
        }, CancellationToken.None, delay: (wait, _) => { waits.Add(wait.TotalSeconds); return Task.CompletedTask; });
        Equal(42, result);
        Equal("5,10,20,40,60,60,60", string.Join(",", waits));
        foreach (var code in new[] { System.Net.HttpStatusCode.Unauthorized, System.Net.HttpStatusCode.Forbidden,
                                     System.Net.HttpStatusCode.NotFound, System.Net.HttpStatusCode.BadRequest })
            Equal(false, NetworkRecovery.IsTransient(new HttpRequestException("fatal", null, code), CancellationToken.None));
        Equal(false, NetworkRecovery.IsTransient(new IOException("corrupt local metadata"), CancellationToken.None));
        Equal(true, NetworkRecovery.IsTransient(new TaskCanceledException("HTTP timeout"), CancellationToken.None));
        using CancellationTokenSource stop = new();
        calls = 0;
        try
        {
            await NetworkRecovery.ExecuteAsync<int>(() =>
            {
                calls++;
                throw new HttpRequestException("unavailable", null, System.Net.HttpStatusCode.ServiceUnavailable);
            }, stop.Token, delay: (_, token) => { stop.Cancel(); token.ThrowIfCancellationRequested(); return Task.CompletedTask; });
            throw new InvalidOperationException("Cancellation not observed");
        }
        catch (OperationCanceledException) { }
        Equal(1, calls);
        Equal(false, NetworkRecovery.IsTransient(new TaskCanceledException(), stop.Token));
        calls = 0;
        try
        {
            await NetworkRecovery.ExecuteAsync<int>(() => { calls++; throw new HttpRequestException("502", null, System.Net.HttpStatusCode.BadGateway); },
                CancellationToken.None, maxAttempts: 3, delay: (_, _) => Task.CompletedTask);
            throw new InvalidOperationException("Retry bound not enforced");
        }
        catch (HttpRequestException) { }
        Equal(3, calls);
    }

    private static async Task TestDiagnosticsAsync(string logPath)
    {
        DiagnosticsTestHandler handler = new();
        ClientStatusModel status = new();
        ProviderConfig config = new() { Server = "https://test.invalid", ClientId = "test-client", Token = "test-token" };
        await using ClientDiagnostics diagnostics = new(
            config, handler, logPath, status);
        config.Token = "changed-during-login";
        await diagnostics.SendOnceAsync(CancellationToken.None);
        Equal(1, handler.Uploads);
        Equal("registration", handler.Phase);
        await diagnostics.SendOnceAsync(CancellationToken.None);
        Equal(1, handler.Uploads);
        Equal(2, handler.Heartbeats);
        diagnostics.SetPhase("failed");
        status.SetState(ClientRunState.Error, "Registration failed", "Bearer private-token registration failed");
        handler.Pending = "admin-request";
        handler.Fail = true;
        Throws<HttpRequestException>(() => diagnostics.SendOnceAsync(CancellationToken.None).GetAwaiter().GetResult());
        handler.Fail = false;
        await diagnostics.SendOnceAsync(CancellationToken.None);
        Equal(2, handler.Uploads);
        Equal("admin-request", handler.LastRequestId);
        Equal("failed", handler.Phase);
        Equal("Error", handler.State);
        Equal(false, handler.Error.Contains("private-token"));
        Equal(true, handler.Error.Contains("registration failed"));
        DiagnosticsTestHandler unauthenticated = new();
        await using ClientDiagnostics missingIdentity = new(new ProviderConfig(), unauthenticated, logPath);
        await missingIdentity.SendOnceAsync(CancellationToken.None);
        Equal(0, unauthenticated.Heartbeats);
    }

    private sealed class DiagnosticsTestHandler : HttpMessageHandler
    {
        public int Uploads;
        public string Pending = "";
        public string LastRequestId = "";
        public bool Fail;
        public int Heartbeats;
        public string Phase = "";
        public string State = "";
        public string Error = "";

        protected override async Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken)
        {
            Equal("test-token", request.Headers.Authorization?.Parameter);
            Equal(true, request.RequestUri!.Query.Contains("client_id=test-client"));
            string payload;
            if (request.RequestUri.AbsolutePath.EndsWith("/heartbeat"))
            {
                using JsonDocument json = JsonDocument.Parse(await request.Content!.ReadAsStringAsync(cancellationToken));
                Phase = json.RootElement.GetProperty("phase").GetString()!;
                State = json.RootElement.GetProperty("state").GetString()!;
                Error = json.RootElement.GetProperty("last_error").GetString()!;
                Heartbeats++;
                payload = JsonSerializer.Serialize(new { request_id = Pending });
            }
            else
            {
                if (Fail) return new HttpResponseMessage(System.Net.HttpStatusCode.ServiceUnavailable);
                using JsonDocument json = JsonDocument.Parse(await request.Content!.ReadAsStringAsync(cancellationToken));
                LastRequestId = json.RootElement.GetProperty("request_id").GetString()!;
                string log = json.RootElement.GetProperty("log_text").GetString()!;
                Equal(false, log.Contains("private-token"));
                Equal(false, log.Contains("ABCD-1234"));
                Uploads++;
                payload = "{\"ok\":true}";
            }
            return new HttpResponseMessage(System.Net.HttpStatusCode.OK) { Content = new StringContent(payload) };
        }
    }

    private sealed class UnauthorizedTestHandler : HttpMessageHandler
    {
        protected override Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken)
            => Task.FromResult(new HttpResponseMessage(System.Net.HttpStatusCode.Unauthorized));
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
