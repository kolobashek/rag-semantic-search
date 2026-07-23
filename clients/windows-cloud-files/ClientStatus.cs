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
            LastSync = lastSync,
            LastError = "",
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
                return current with
                {
                    State = ClientRunState.Error,
                    Message = "Ошибка загрузки файла",
                    CurrentPath = cloudPath,
                    ActiveTransfers = active,
                    LastError = error.Message,
                };
            }

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
            snapshot = _current;
        }

        Changed?.Invoke(snapshot);
    }
}
