namespace RagCloudFiles;

internal enum ClientRunState
{
    Starting,
    Authorizing,
    Syncing,
    UpToDate,
    Offline,
    Error,
    Stopped,
}

internal sealed record ClientStatusSnapshot(
    ClientRunState State,
    string Message,
    string CurrentPath,
    int ActiveTransfers,
    int ObjectCount,
    DateTimeOffset? LastSync,
    string LastError);

internal sealed class ClientStatusModel
{
    private readonly object _sync = new();
    private readonly Dictionary<string, string> _fileErrors = new(StringComparer.OrdinalIgnoreCase);
    private int _syncIssues;
    private string _syncIssueMessage = "";
    private ClientStatusSnapshot _current = new(
        ClientRunState.Starting,
        "Запуск клиента…",
        "",
        0,
        0,
        null,
        "");

    public event Action<ClientStatusSnapshot>? Changed;

    public ClientStatusSnapshot Current
    {
        get
        {
            lock (_sync)
            {
                return _current;
            }
        }
    }

    public void SetState(ClientRunState state, string message, string error = "")
    {
        Update(current => current with
        {
            State = state,
            Message = message,
            LastError = error,
            CurrentPath = state is ClientRunState.UpToDate
                or ClientRunState.Offline
                or ClientRunState.Error
                or ClientRunState.Stopped
                ? ""
                : current.CurrentPath,
        });
    }

    public void SetInventory(int objectCount, DateTimeOffset lastSync)
    {
        Update(current => current with
        {
            State = current.ActiveTransfers > 0 ? ClientRunState.Syncing : ClientRunState.UpToDate,
            Message = current.ActiveTransfers > 0
                ? $"Загружается файлов: {current.ActiveTransfers}"
                : "Синхронизировано",
            ObjectCount = objectCount,
            LastSync = _syncIssues == 0 && _fileErrors.Count == 0 ? lastSync : current.LastSync,
            LastError = "",
        });
    }

    public void SetSyncIssues(int count, string message)
    {
        Update(current =>
        {
            _syncIssues = Math.Max(0, count);
            _syncIssueMessage = message;
            return current;
        });
    }

    public void ClearFileError(string cloudPath)
    {
        Update(current =>
        {
            _fileErrors.Remove(cloudPath);
            return current;
        });
    }

    public void RecordFileError(string cloudPath, Exception error)
    {
        Update(current =>
        {
            _fileErrors[cloudPath] = error.Message;
            return current;
        });
    }

    public void BeginTransfer(string cloudPath)
    {
        Update(current =>
        {
            int active = current.ActiveTransfers + 1;
            return current with
            {
                State = ClientRunState.Syncing,
                Message = $"Загружается файлов: {active}",
                CurrentPath = cloudPath,
                ActiveTransfers = active,
                LastError = "",
            };
        });
    }

    public void EndTransfer(string cloudPath, Exception? error = null)
    {
        Update(current =>
        {
            int active = Math.Max(0, current.ActiveTransfers - 1);
            if (error is not null)
            {
                _fileErrors[cloudPath] = error.Message;
                return current with
                {
                    State = ClientRunState.Error,
                    Message = "Ошибка загрузки файла",
                    CurrentPath = cloudPath,
                    ActiveTransfers = active,
                    LastError = error.Message,
                };
            }

            _fileErrors.Remove(cloudPath);

            return current with
            {
                State = active > 0 ? ClientRunState.Syncing : ClientRunState.UpToDate,
                Message = active > 0 ? $"Загружается файлов: {active}" : "Синхронизировано",
                CurrentPath = active > 0 ? current.CurrentPath : "",
                ActiveTransfers = active,
                LastError = "",
            };
        });
    }

    private void Update(Func<ClientStatusSnapshot, ClientStatusSnapshot> updater)
    {
        ClientStatusSnapshot snapshot;
        lock (_sync)
        {
            _current = updater(_current);
            if ((_syncIssues > 0 || _fileErrors.Count > 0)
                && _current.State is ClientRunState.UpToDate or ClientRunState.Syncing or ClientRunState.Error)
            {
                _current = _current with
                {
                    State = ClientRunState.Error,
                    Message = "Есть несинхронизированные файлы",
                    LastError = _fileErrors.Count > 0 ? _fileErrors.Values.First() : _syncIssueMessage,
                };
            }
            snapshot = _current;
        }

        Changed?.Invoke(snapshot);
    }
}
