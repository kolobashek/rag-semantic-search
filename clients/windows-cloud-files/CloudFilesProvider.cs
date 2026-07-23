using System.Collections.Concurrent;
using System.Runtime.InteropServices;
using System.Text;
using Windows.Win32;
using Windows.Win32.Foundation;
using Windows.Win32.Storage.CloudFilters;
using Windows.Win32.Storage.FileSystem;

namespace RagCloudFiles;

internal sealed class CloudFilesProvider : IAsyncDisposable
{
    private const int Alignment = 4096;
    private const int TransferChunkBytes = 4 * 1024 * 1024;
    private const int AlreadyExistsHResult = unchecked((int)0x800700B7);
    private const int UnsuccessfulNtStatus = unchecked((int)0xC0000001);
    private static readonly Guid ProviderId = new("8f734f08-90fd-4c31-a3e2-1edcad1693fb");
    private static CloudFilesProvider? _current;

    private readonly ProviderConfig _config;
    private readonly ConfigStore _store;
    private readonly CloudDriveApi _api;
    private readonly ClientStatusModel _status;
    private readonly ProviderState _state;
    private readonly object _nodesSync = new();
    private readonly Dictionary<string, CloudNode> _nodes = new(StringComparer.OrdinalIgnoreCase);
    private readonly ConcurrentDictionary<long, CancellationTokenSource> _hydrations = new();
    private readonly SemaphoreSlim _refreshLock = new(1, 1);
    private readonly string _root;
    private CF_CONNECTION_KEY _connectionKey;
    private bool _connected;
    private string _cursor = "";
    private string _aclRevision = "";
    private DateTimeOffset _lastFullSnapshot = DateTimeOffset.MinValue;

    public CloudFilesProvider(
        ProviderConfig config,
        ConfigStore store,
        CloudDriveApi api,
        ClientStatusModel? status = null)
    {
        _config = config;
        _store = store;
        _api = api;
        _status = status ?? new ClientStatusModel();
        _state = store.LoadState();
        _root = Path.GetFullPath(config.RootPath);
    }

    public async Task StartAsync(CancellationToken cancellationToken)
    {
        _status.SetState(ClientRunState.Syncing, "Подготовка облачной папки…");
        Directory.CreateDirectory(_root);
        RegisterSyncRoot();
        ConnectSyncRoot();
        await RefreshFullSnapshotAsync(cancellationToken);
        await ApplyOfflinePolicyAsync(cancellationToken);
    }

    public static void Unregister(string rootPath)
    {
        string root = Path.GetFullPath(rootPath);
        if (!Directory.Exists(root))
        {
            return;
        }

        PInvoke.CfUnregisterSyncRoot(root).ThrowOnFailure();
    }

    public async Task RunAsync(CancellationToken cancellationToken)
    {
        using PeriodicTimer timer = new(TimeSpan.FromSeconds(Math.Clamp(_config.PollSeconds, 15, 3600)));
        while (await timer.WaitForNextTickAsync(cancellationToken))
        {
            try
            {
                await SyncNowAsync(cancellationToken);
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                throw;
            }
            catch (Exception exception)
            {
                _status.SetState(ClientRunState.Offline, "Нет связи с облаком", exception.Message);
                AppLog.Error("Синхронизация namespace не выполнена.", exception);
                Console.Error.WriteLine($"Синхронизация namespace не выполнена: {exception.Message}");
            }
        }
    }

    public async Task SyncNowAsync(CancellationToken cancellationToken)
    {
        await _refreshLock.WaitAsync(cancellationToken);
        try
        {
            _status.SetState(ClientRunState.Syncing, "Проверка изменений…");
            if (DateTimeOffset.UtcNow - _lastFullSnapshot >= TimeSpan.FromMinutes(30))
            {
                await RefreshFullSnapshotAsync(cancellationToken);
            }
            else
            {
                await ApplyIncrementalChangesAsync(cancellationToken);
                _status.SetInventory(GetObjectCount(), DateTimeOffset.Now);
            }
            await _api.HeartbeatAsync(_config.ClientId, cancellationToken);
        }
        finally
        {
            _refreshLock.Release();
        }

        if (_config.KeepAllOffline || _config.OfflinePaths.Count > 0)
        {
            await ApplyOfflinePolicyAsync(cancellationToken);
        }
    }

    public IReadOnlyList<string> GetTopLevelFolders()
    {
        lock (_nodesSync)
        {
            return _nodes.Values
                .Where(node => node.IsFolder && CloudPath.Depth(node.Path) == 1)
                .Select(node => node.Path)
                .Distinct(StringComparer.OrdinalIgnoreCase)
                .OrderBy(path => path, StringComparer.CurrentCultureIgnoreCase)
                .ToList();
        }
    }

    public async Task ApplyOfflinePolicyAsync(CancellationToken cancellationToken)
    {
        await _refreshLock.WaitAsync(cancellationToken);
        try
        {
            _status.SetState(ClientRunState.Syncing, "Применение офлайн-настроек…");
            HashSet<string> desired = new(_config.OfflinePaths, StringComparer.OrdinalIgnoreCase);
            HashSet<string> previous = new(_state.AppliedOfflinePaths, StringComparer.OrdinalIgnoreCase);
            bool policyChanged = _state.AppliedAllOffline != _config.KeepAllOffline ||
                !previous.SetEquals(desired);
            await Task.Run(() =>
            {
                cancellationToken.ThrowIfCancellationRequested();
                if (_state.AppliedAllOffline && !_config.KeepAllOffline)
                {
                    CloudFilePinning.SetPinState(_root, pinned: false, recursive: true);
                }

                foreach (string removed in previous.Except(desired, StringComparer.OrdinalIgnoreCase))
                {
                    string localPath = CloudPath.LocalPath(_root, removed);
                    if (Directory.Exists(localPath) || File.Exists(localPath))
                    {
                        CloudFilePinning.SetPinState(localPath, pinned: false, recursive: true);
                    }
                }

                if (policyChanged && _config.KeepAllOffline)
                {
                    CloudFilePinning.SetPinState(_root, pinned: true, recursive: true);
                }
                else if (policyChanged)
                {
                    foreach (string cloudPath in desired)
                    {
                        string localPath = CloudPath.LocalPath(_root, cloudPath);
                        if (Directory.Exists(localPath) || File.Exists(localPath))
                        {
                            CloudFilePinning.SetPinState(localPath, pinned: true, recursive: true);
                        }
                    }
                }
            }, cancellationToken);

            IReadOnlyList<CloudNode> offlineFiles = GetOfflineFiles(desired);
            IReadOnlyList<CloudNode> filesToHydrate = offlineFiles
                .Where(node => !_state.AppliedOfflineVersions
                    .GetValueOrDefault(node.Path, "")
                    .Equals(NodeSignature(node), StringComparison.Ordinal))
                .ToList();
            if (filesToHydrate.Count > 0)
            {
                _status.SetState(
                    ClientRunState.Syncing,
                    $"Загрузка для офлайн-доступа: {filesToHydrate.Count:N0} файлов");
                await Parallel.ForEachAsync(
                    filesToHydrate,
                    new ParallelOptions
                    {
                        CancellationToken = cancellationToken,
                        MaxDegreeOfParallelism = 2,
                    },
                    async (node, token) =>
                    {
                        string localPath = CloudPath.LocalPath(_root, node.Path);
                        if (node.SizeBytes <= 0 || !File.Exists(localPath))
                        {
                            return;
                        }

                        await Task.Run(() =>
                        {
                            CloudFilePinning.SetPinState(localPath, pinned: true, recursive: false);
                            CloudFilePinning.HydrateFile(localPath);
                        }, token);
                    });
            }

            _state.AppliedAllOffline = _config.KeepAllOffline;
            _state.AppliedOfflinePaths = desired;
            _state.AppliedOfflineVersions = offlineFiles.ToDictionary(
                node => node.Path,
                NodeSignature,
                StringComparer.OrdinalIgnoreCase);
            _store.SaveState(_state);
            _status.SetInventory(GetObjectCount(), DateTimeOffset.Now);
        }
        finally
        {
            _refreshLock.Release();
        }
    }

    private IReadOnlyList<CloudNode> GetOfflineFiles(IReadOnlySet<string> desired)
    {
        lock (_nodesSync)
        {
            return _nodes.Values
                .Where(node => !node.IsFolder && (
                    _config.KeepAllOffline ||
                    desired.Any(path => IsAtOrBelow(node.Path, path))))
                .OrderBy(node => node.Path, StringComparer.OrdinalIgnoreCase)
                .ToList();
        }
    }

    private static bool IsAtOrBelow(string candidate, string folder) =>
        string.Equals(candidate, folder, StringComparison.OrdinalIgnoreCase) ||
        candidate.StartsWith($"{folder}/", StringComparison.OrdinalIgnoreCase);

    public async ValueTask DisposeAsync()
    {
        await _refreshLock.WaitAsync();
        try
        {
            foreach (CancellationTokenSource source in _hydrations.Values)
            {
                source.Cancel();
                source.Dispose();
            }

            _hydrations.Clear();
            if (_connected)
            {
                PInvoke.CfDisconnectSyncRoot(_connectionKey).ThrowOnFailure();
                _connected = false;
            }

            if (ReferenceEquals(_current, this))
            {
                _current = null;
            }
        }
        finally
        {
            _refreshLock.Release();
        }
    }

    private async Task RefreshFullSnapshotAsync(CancellationToken cancellationToken)
    {
        VisibleSnapshot snapshot = await _api.GetVisibleSnapshotAsync(cancellationToken);
        lock (_nodesSync)
        {
            _nodes.Clear();
            foreach (CloudNode node in snapshot.Nodes)
            {
                _nodes[node.Path] = node;
            }
        }

        _cursor = snapshot.Cursor;
        _aclRevision = snapshot.AclRevision;
        ReconcileNamespace();
        _lastFullSnapshot = DateTimeOffset.UtcNow;
        _status.SetInventory(GetObjectCount(), DateTimeOffset.Now);
        Console.WriteLine($"Доступно объектов: {_nodes.Count:N0}; содержимое файлов остаётся в облаке до открытия.");
    }

    private async Task ApplyIncrementalChangesAsync(CancellationToken cancellationToken)
    {
        bool changed = false;
        for (int pageNumber = 0; pageNumber < 1000; pageNumber++)
        {
            ChangePage page = await _api.GetChangesAsync(_cursor, cancellationToken);
            if (!string.Equals(page.AclRevision, _aclRevision, StringComparison.Ordinal))
            {
                await RefreshFullSnapshotAsync(cancellationToken);
                return;
            }
            lock (_nodesSync)
            {
                foreach (CloudNode node in page.Changes)
                {
                    if (!CloudPath.TryNormalize(node.Path, out string path) || path.Length == 0)
                    {
                        Console.Error.WriteLine($"Пропущен несовместимый с Windows путь: {node.Path}");
                        continue;
                    }

                    node.Path = path;
                    if (node.DeletedAt.Length == 0)
                    {
                        _nodes[path] = node;
                    }
                    else
                    {
                        _nodes.Remove(path);
                    }

                    changed = true;
                }
            }

            string next = page.NextCursor ?? "";
            if (string.Equals(next, _cursor, StringComparison.Ordinal))
            {
                break;
            }

            _cursor = next;
        }

        if (changed)
        {
            ReconcileNamespace();
        }
    }

    private void ReconcileNamespace()
    {
        Dictionary<string, CloudNode> desired;
        lock (_nodesSync)
        {
            desired = BuildDesiredNodes(_nodes.Values);
        }
        HashSet<string> nextManaged = new(StringComparer.OrdinalIgnoreCase);
        Dictionary<string, string> nextVersions = new(StringComparer.OrdinalIgnoreCase);

        foreach (IGrouping<int, CloudNode> depthGroup in desired.Values
                     .Where(node => node.IsFolder)
                     .OrderBy(node => CloudPath.Depth(node.Path))
                     .GroupBy(node => CloudPath.Depth(node.Path)))
        {
            CreateMissingPlaceholders(depthGroup, nextManaged, nextVersions);
        }

        foreach (IGrouping<string, CloudNode> parentGroup in desired.Values
                     .Where(node => !node.IsFolder)
                     .OrderBy(node => node.Path, StringComparer.OrdinalIgnoreCase)
                     .GroupBy(node => CloudPath.Parent(node.Path), StringComparer.OrdinalIgnoreCase))
        {
            CreateMissingPlaceholders(parentGroup, nextManaged, nextVersions);
        }

        foreach (string stalePath in _state.ManagedPaths
                     .Where(path => !desired.ContainsKey(path))
                     .OrderByDescending(CloudPath.Depth))
        {
            RemoveManagedPath(stalePath);
        }

        _state.ManagedPaths = nextManaged;
        _state.ManagedVersions = nextVersions;
        _store.SaveState(_state);
    }

    private void CreateMissingPlaceholders(
        IEnumerable<CloudNode> nodes,
        HashSet<string> nextManaged,
        Dictionary<string, string> nextVersions)
    {
        foreach (IGrouping<string, CloudNode> parentGroup in nodes.GroupBy(
                     node => CloudPath.Parent(node.Path),
                     StringComparer.OrdinalIgnoreCase))
        {
            string localParent = parentGroup.Key.Length == 0
                ? _root
                : CloudPath.LocalPath(_root, parentGroup.Key);
            foreach (CloudNode[] batch in parentGroup.Chunk(256))
            {
                List<CloudNode> create = [];
                foreach (CloudNode node in batch)
                {
                    string localPath = CloudPath.LocalPath(_root, node.Path);
                    string signature = NodeSignature(node);
                    bool exists = File.Exists(localPath) || Directory.Exists(localPath);
                    if (exists
                        && _state.ManagedPaths.Contains(node.Path)
                        && !node.IsFolder
                        && !_state.ManagedVersions.GetValueOrDefault(node.Path, "").Equals(
                            signature,
                            StringComparison.Ordinal))
                    {
                        RemoveManagedPath(node.Path);
                        exists = File.Exists(localPath) || Directory.Exists(localPath);
                    }
                    if (!exists)
                    {
                        create.Add(node);
                    }
                    else if (_state.ManagedPaths.Contains(node.Path))
                    {
                        nextManaged.Add(node.Path);
                        nextVersions[node.Path] = exists && !node.IsFolder
                            ? _state.ManagedVersions.GetValueOrDefault(node.Path, "")
                            : signature;
                    }
                    else
                    {
                        Console.Error.WriteLine($"Пропущена локальная коллизия: {localPath}");
                    }
                }

                if (create.Count == 0)
                {
                    continue;
                }

                using NativePlaceholderBatch native = new(create);
                HRESULT result = PInvoke.CfCreatePlaceholders(
                    localParent,
                    native.Infos,
                    CF_CREATE_FLAGS.CF_CREATE_FLAG_STOP_ON_ERROR,
                    out uint processed);
                result.ThrowOnFailure();
                if (processed != create.Count)
                {
                    throw new IOException($"CfAPI создал {processed} из {create.Count} плейсхолдеров в {localParent}.");
                }

                foreach (CloudNode node in create)
                {
                    nextManaged.Add(node.Path);
                    nextVersions[node.Path] = NodeSignature(node);
                }
            }
        }
    }

    private void RemoveManagedPath(string cloudPath)
    {
        string localPath = CloudPath.LocalPath(_root, cloudPath);
        try
        {
            if (File.Exists(localPath))
            {
                File.SetAttributes(localPath, File.GetAttributes(localPath) & ~FileAttributes.ReadOnly);
                File.Delete(localPath);
            }
            else if (Directory.Exists(localPath))
            {
                Directory.Delete(localPath, recursive: false);
            }
        }
        catch (IOException)
        {
            // Preserve a non-empty directory or an in-use file; retry on the next full snapshot.
        }
        catch (UnauthorizedAccessException)
        {
            // Preserve local data rather than forcing a destructive cleanup.
        }
    }

    private unsafe void RegisterSyncRoot()
    {
        const string providerName = "RAG Cloud Drive";
        const string providerVersion = AppDefaults.Version;
        byte[] rootIdentity = Encoding.UTF8.GetBytes("ragcf1\n" + _config.Server);

        fixed (char* providerNamePointer = providerName)
        fixed (char* providerVersionPointer = providerVersion)
        fixed (byte* rootIdentityPointer = rootIdentity)
        {
            CF_SYNC_REGISTRATION registration = new()
            {
                StructSize = (uint)sizeof(CF_SYNC_REGISTRATION),
                ProviderName = providerNamePointer,
                ProviderVersion = providerVersionPointer,
                SyncRootIdentity = rootIdentityPointer,
                SyncRootIdentityLength = (uint)rootIdentity.Length,
                ProviderId = ProviderId,
            };
            CF_SYNC_POLICIES policies = new()
            {
                StructSize = (uint)sizeof(CF_SYNC_POLICIES),
                Hydration = new CF_HYDRATION_POLICY
                {
                    Primary = CF_HYDRATION_POLICY_PRIMARY.CF_HYDRATION_POLICY_PARTIAL,
                    Modifier = CF_HYDRATION_POLICY_MODIFIER.CF_HYDRATION_POLICY_MODIFIER_AUTO_DEHYDRATION_ALLOWED,
                },
                Population = new CF_POPULATION_POLICY
                {
                    Primary = CF_POPULATION_POLICY_PRIMARY.CF_POPULATION_POLICY_ALWAYS_FULL,
                    Modifier = CF_POPULATION_POLICY_MODIFIER.CF_POPULATION_POLICY_MODIFIER_NONE,
                },
                InSync = CF_INSYNC_POLICY.CF_INSYNC_POLICY_TRACK_FILE_ALL,
                HardLink = CF_HARDLINK_POLICY.CF_HARDLINK_POLICY_NONE,
                PlaceholderManagement = CF_PLACEHOLDER_MANAGEMENT_POLICY.CF_PLACEHOLDER_MANAGEMENT_POLICY_DEFAULT,
            };
            CF_REGISTER_FLAGS flags = CF_REGISTER_FLAGS.CF_REGISTER_FLAG_MARK_IN_SYNC_ON_ROOT;
            HRESULT result = PInvoke.CfRegisterSyncRoot(_root, registration, policies, flags);
            if (result.Value == AlreadyExistsHResult)
            {
                result = PInvoke.CfRegisterSyncRoot(
                    _root,
                    registration,
                    policies,
                    flags | CF_REGISTER_FLAGS.CF_REGISTER_FLAG_UPDATE);
            }

            result.ThrowOnFailure();
        }
    }

    private unsafe void ConnectSyncRoot()
    {
        if (_current is not null)
        {
            throw new InvalidOperationException("В процессе уже подключён CfAPI provider.");
        }

        CF_CALLBACK_REGISTRATION[] callbacks =
        [
            new()
            {
                Type = CF_CALLBACK_TYPE.CF_CALLBACK_TYPE_FETCH_DATA,
                Callback = CallbackDelegates.FetchData,
            },
            new()
            {
                Type = CF_CALLBACK_TYPE.CF_CALLBACK_TYPE_CANCEL_FETCH_DATA,
                Callback = CallbackDelegates.CancelFetchData,
            },
            new()
            {
                Type = CF_CALLBACK_TYPE.CF_CALLBACK_TYPE_NONE,
                Callback = null!,
            },
        ];
        _current = this;
        HRESULT result = PInvoke.CfConnectSyncRoot(
            _root,
            callbacks,
            null,
            CF_CONNECT_FLAGS.CF_CONNECT_FLAG_REQUIRE_FULL_FILE_PATH,
            out _connectionKey);
        if (result.Failed)
        {
            _current = null;
            result.ThrowOnFailure();
        }

        _connected = true;
    }

    private void QueueHydration(HydrationRequest request)
    {
        CancellationTokenSource source = new();
        if (!_hydrations.TryAdd(request.RequestKey, source))
        {
            source.Dispose();
            return;
        }

        _ = Task.Run(async () =>
        {
            Exception? transferError = null;
            _status.BeginTransfer(request.CloudPath);
            try
            {
                await HydrateAsync(request, source.Token);
            }
            catch (OperationCanceledException) when (source.IsCancellationRequested)
            {
                // CfAPI has cancelled the pending fetch.
            }
            catch (Exception exception)
            {
                transferError = exception;
                AppLog.Error($"Не удалось загрузить {request.CloudPath}.", exception);
                Console.Error.WriteLine($"Не удалось загрузить {request.CloudPath}: {exception.Message}");
                CompleteTransferFailure(request);
            }
            finally
            {
                _status.EndTransfer(request.CloudPath, transferError);
                if (_hydrations.TryRemove(request.RequestKey, out CancellationTokenSource? removed))
                {
                    removed.Dispose();
                }
            }
        });
    }

    private async Task HydrateAsync(HydrationRequest request, CancellationToken cancellationToken)
    {
        long start = request.Offset / Alignment * Alignment;
        long requestedEnd = checked(request.Offset + request.Length);
        long end = Math.Min(request.FileSize, AlignUp(requestedEnd, Alignment));
        long position = start;
        while (position < end)
        {
            int length = checked((int)Math.Min(TransferChunkBytes, end - position));
            byte[] bytes = await _api.DownloadRangeAsync(request.CloudPath, position, length, cancellationToken);
            if (bytes.Length != length)
            {
                throw new EndOfStreamException($"Получено {bytes.Length} байт вместо {length}.");
            }

            TransferData(request, position, bytes);
            position += bytes.Length;
        }
    }

    private static unsafe void TransferData(HydrationRequest request, long offset, byte[] bytes)
    {
        fixed (byte* buffer = bytes)
        {
            CF_OPERATION_INFO operation = CreateOperationInfo(request);
            CF_OPERATION_PARAMETERS parameters = new()
            {
                ParamSize = TransferDataParameterSize(),
            };
            parameters.TransferData.Flags = CF_OPERATION_TRANSFER_DATA_FLAGS.CF_OPERATION_TRANSFER_DATA_FLAG_NONE;
            parameters.TransferData.CompletionStatus = (NTSTATUS)0;
            parameters.TransferData.Buffer = buffer;
            parameters.TransferData.Offset = offset;
            parameters.TransferData.Length = bytes.Length;
            PInvoke.CfExecute(operation, ref parameters).ThrowOnFailure();
        }
    }

    private static unsafe void CompleteTransferFailure(HydrationRequest request)
    {
        CF_OPERATION_INFO operation = CreateOperationInfo(request);
        CF_OPERATION_PARAMETERS parameters = new()
        {
            ParamSize = TransferDataParameterSize(),
        };
        parameters.TransferData.Flags = CF_OPERATION_TRANSFER_DATA_FLAGS.CF_OPERATION_TRANSFER_DATA_FLAG_NONE;
        parameters.TransferData.CompletionStatus = (NTSTATUS)UnsuccessfulNtStatus;
        parameters.TransferData.Buffer = null;
        parameters.TransferData.Offset = request.Offset / Alignment * Alignment;
        parameters.TransferData.Length = Math.Max(Alignment, AlignUp(request.Length, Alignment));
        PInvoke.CfExecute(operation, ref parameters);
    }

    private static unsafe CF_OPERATION_INFO CreateOperationInfo(HydrationRequest request) => new()
    {
        StructSize = (uint)sizeof(CF_OPERATION_INFO),
        Type = CF_OPERATION_TYPE.CF_OPERATION_TYPE_TRANSFER_DATA,
        ConnectionKey = request.ConnectionKey,
        TransferKey = request.TransferKey,
        RequestKey = request.RequestKey,
    };

    private static unsafe uint TransferDataParameterSize() => checked((uint)(
        Marshal.OffsetOf<CF_OPERATION_PARAMETERS>(nameof(CF_OPERATION_PARAMETERS.Anonymous)).ToInt32()
        + sizeof(CF_OPERATION_PARAMETERS._Anonymous_e__Union._TransferData_e__Struct)));

    private static long AlignUp(long value, int alignment) => checked((value + alignment - 1) / alignment * alignment);

    private int GetObjectCount()
    {
        lock (_nodesSync)
        {
            return _nodes.Count;
        }
    }

    private static unsafe void OnFetchData(CF_CALLBACK_INFO* info, CF_CALLBACK_PARAMETERS* parameters)
    {
        CloudFilesProvider? provider = _current;
        if (provider is null || info is null || parameters is null || info->FileIdentity is null)
        {
            return;
        }

        try
        {
            ReadOnlySpan<byte> identity = new(info->FileIdentity, checked((int)info->FileIdentityLength));
            string cloudPath = FileIdentityCodec.Decode(identity);
            provider.QueueHydration(new HydrationRequest(
                cloudPath,
                info->FileSize,
                parameters->FetchData.RequiredFileOffset,
                parameters->FetchData.RequiredLength,
                info->ConnectionKey,
                info->TransferKey,
                info->RequestKey));
        }
        catch (Exception exception)
        {
            Console.Error.WriteLine($"Некорректный запрос CfAPI: {exception.Message}");
        }
    }

    private static unsafe void OnCancelFetchData(CF_CALLBACK_INFO* info, CF_CALLBACK_PARAMETERS* parameters)
    {
        if (_current is not null && info is not null && _current._hydrations.TryGetValue(info->RequestKey, out CancellationTokenSource? source))
        {
            source.Cancel();
        }
    }

    private static Dictionary<string, CloudNode> BuildDesiredNodes(IEnumerable<CloudNode> source)
    {
        Dictionary<string, CloudNode> desired = new(StringComparer.OrdinalIgnoreCase);
        foreach (CloudNode node in source)
        {
            string path = CloudPath.Normalize(node.Path);
            if (path.Length == 0 || node.DeletedAt.Length > 0)
            {
                continue;
            }

            node.Path = path;
            desired[path] = node;
            string parent = CloudPath.Parent(path);
            while (parent.Length > 0)
            {
                desired.TryAdd(parent, new CloudNode
                {
                    NodeType = "folder",
                    Path = parent,
                    Name = Path.GetFileName(parent.Replace('/', Path.DirectorySeparatorChar)),
                });
                parent = CloudPath.Parent(parent);
            }
        }

        return desired;
    }

    private static string NodeSignature(CloudNode node) => node.IsFolder
        ? "folder"
        : string.Join(':', node.CurrentVersionId, node.Checksum, node.SizeBytes);

    private sealed unsafe class NativePlaceholderBatch : IDisposable
    {
        private readonly List<nint> _allocations = [];

        public NativePlaceholderBatch(IReadOnlyList<CloudNode> nodes)
        {
            Infos = new CF_PLACEHOLDER_CREATE_INFO[nodes.Count];
            for (int index = 0; index < nodes.Count; index++)
            {
                CloudNode node = nodes[index];
                nint name = Marshal.StringToHGlobalUni(Path.GetFileName(node.Path.Replace('/', Path.DirectorySeparatorChar)));
                byte[] identity = FileIdentityCodec.Encode(node.Path);
                nint identityPointer = Marshal.AllocHGlobal(identity.Length);
                Marshal.Copy(identity, 0, identityPointer, identity.Length);
                _allocations.Add(name);
                _allocations.Add(identityPointer);

                long created = ParseFileTime(node.CreatedAt);
                long updated = ParseFileTime(node.UpdatedAt);
                Infos[index] = new CF_PLACEHOLDER_CREATE_INFO
                {
                    RelativeFileName = (char*)name,
                    FsMetadata = new CF_FS_METADATA
                    {
                        BasicInfo = new FILE_BASIC_INFO
                        {
                            CreationTime = created,
                            LastAccessTime = updated,
                            LastWriteTime = updated,
                            ChangeTime = updated,
                            FileAttributes = (uint)(node.IsFolder ? FileAttributes.Directory : FileAttributes.ReadOnly),
                        },
                        FileSize = node.IsFolder ? 0 : Math.Max(0, node.SizeBytes),
                    },
                    FileIdentity = (void*)identityPointer,
                    FileIdentityLength = (uint)identity.Length,
                    Flags = CF_PLACEHOLDER_CREATE_FLAGS.CF_PLACEHOLDER_CREATE_FLAG_MARK_IN_SYNC
                        | (node.IsFolder
                            ? CF_PLACEHOLDER_CREATE_FLAGS.CF_PLACEHOLDER_CREATE_FLAG_DISABLE_ON_DEMAND_POPULATION
                            : CF_PLACEHOLDER_CREATE_FLAGS.CF_PLACEHOLDER_CREATE_FLAG_NONE),
                };
            }
        }

        public CF_PLACEHOLDER_CREATE_INFO[] Infos { get; }

        public void Dispose()
        {
            foreach (nint allocation in _allocations)
            {
                Marshal.FreeHGlobal(allocation);
            }
        }

        private static long ParseFileTime(string value)
        {
            return DateTimeOffset.TryParse(value, out DateTimeOffset timestamp)
                ? timestamp.UtcDateTime.ToFileTimeUtc()
                : DateTime.UtcNow.ToFileTimeUtc();
        }
    }

    private sealed record HydrationRequest(
        string CloudPath,
        long FileSize,
        long Offset,
        long Length,
        CF_CONNECTION_KEY ConnectionKey,
        long TransferKey,
        long RequestKey);

    private static unsafe class CallbackDelegates
    {
        internal static readonly CF_CALLBACK FetchData = OnFetchData;
        internal static readonly CF_CALLBACK CancelFetchData = OnCancelFetchData;
    }
}
