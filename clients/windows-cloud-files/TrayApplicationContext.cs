using System.Diagnostics;
using System.Drawing;
using System.Windows.Forms;

namespace RagCloudFiles;

internal sealed class TrayApplicationContext : ApplicationContext
{
    private readonly ClientStatusModel _status;
    private readonly ProviderConfig _config;
    private readonly Func<CloudFilesProvider?> _provider;
    private readonly Func<ClientSettingsSelection, Task> _saveSettings;
    private readonly Action _requestRestart;
    private readonly Action _requestExit;
    private readonly CancellationToken _applicationToken;
    private readonly Control _dispatcher;
    private readonly NotifyIcon _notifyIcon;
    private readonly Icon _baseIcon;
    private readonly ToolStripMenuItem _statusItem;
    private readonly ToolStripMenuItem _activityItem;
    private readonly ToolStripMenuItem _syncNowItem;
    private readonly ToolStripMenuItem _allOfflineItem;
    private Icon? _statusIcon;
    private ClientRunState _lastState = ClientRunState.Starting;
    private bool _disposed;

    public TrayApplicationContext(
        ClientStatusModel status,
        ProviderConfig config,
        Func<CloudFilesProvider?> provider,
        Func<ClientSettingsSelection, Task> saveSettings,
        Action requestRestart,
        Action requestExit,
        CancellationToken applicationToken)
    {
        _status = status;
        _config = config;
        _provider = provider;
        _saveSettings = saveSettings;
        _requestRestart = requestRestart;
        _requestExit = requestExit;
        _applicationToken = applicationToken;
        _dispatcher = new Control();
        _dispatcher.CreateControl();

        _statusItem = new ToolStripMenuItem("Запуск клиента…") { Enabled = false };
        _activityItem = new ToolStripMenuItem("") { Enabled = false, Visible = false };
        _syncNowItem = new ToolStripMenuItem("Синхронизировать сейчас");
        _syncNowItem.Click += async (_, _) => await RunOperationAsync(
            "Синхронизация",
            provider => provider.SyncNowAsync(_applicationToken));

        _allOfflineItem = new ToolStripMenuItem("Всегда хранить всё офлайн")
        {
            CheckOnClick = true,
            Checked = config.KeepAllOffline,
        };
        _allOfflineItem.Click += async (_, _) =>
        {
            ClientSettingsSelection selection = new(
                _config.RootPath,
                _allOfflineItem.Checked,
                new HashSet<string>(_config.OfflinePaths, StringComparer.OrdinalIgnoreCase),
                _config.StartWithWindows);
            await SaveSettingsAsync(selection);
        };

        ContextMenuStrip menu = new();
        menu.Items.Add(_statusItem);
        menu.Items.Add(_activityItem);
        menu.Items.Add(new ToolStripSeparator());
        menu.Items.Add("Открыть облачную папку", null, (_, _) => WindowsBootstrap.OpenRoot(_config.RootPath));
        menu.Items.Add(_syncNowItem);
        menu.Items.Add(_allOfflineItem);
        menu.Items.Add("Настройки…", null, (_, _) => OpenSettings());
        menu.Items.Add(new ToolStripSeparator());
        menu.Items.Add("Открыть облако в браузере", null, (_, _) => OpenUrl(_config.Server + "/explorer"));
        menu.Items.Add("Открыть журнал", null, (_, _) => OpenLog());
        menu.Items.Add("Перезапустить клиент", null, (_, _) => _requestRestart());
        menu.Items.Add(new ToolStripSeparator());
        menu.Items.Add("Выход", null, (_, _) => _requestExit());

        _baseIcon = Icon.ExtractAssociatedIcon(Environment.ProcessPath ?? "")
            ?? (Icon)SystemIcons.Application.Clone();
        _statusIcon = TrayIconFactory.Create(_baseIcon, ClientRunState.Starting);
        _notifyIcon = new NotifyIcon
        {
            Icon = _statusIcon,
            Text = AppDefaults.ProductName,
            ContextMenuStrip = menu,
            Visible = true,
        };
        _notifyIcon.DoubleClick += (_, _) => WindowsBootstrap.OpenRoot(_config.RootPath);

        _status.Changed += StatusChanged;
        ApplyStatus(_status.Current);
    }

    public void Stop()
    {
        if (_disposed || !_dispatcher.IsHandleCreated)
        {
            return;
        }

        try
        {
            _dispatcher.BeginInvoke(new Action(ExitThread));
        }
        catch (InvalidOperationException)
        {
            // The tray thread is already shutting down.
        }
    }

    protected override void ExitThreadCore()
    {
        if (!_disposed)
        {
            _disposed = true;
            _status.Changed -= StatusChanged;
            _notifyIcon.Visible = false;
            _notifyIcon.Dispose();
            _statusIcon?.Dispose();
            _baseIcon.Dispose();
            _dispatcher.Dispose();
        }
        base.ExitThreadCore();
    }

    private void StatusChanged(ClientStatusSnapshot snapshot)
    {
        if (_disposed || !_dispatcher.IsHandleCreated)
        {
            return;
        }
        try
        {
            _dispatcher.BeginInvoke(new Action(() => ApplyStatus(snapshot)));
        }
        catch (InvalidOperationException)
        {
            // The tray thread is already shutting down.
        }
    }

    private void ApplyStatus(ClientStatusSnapshot snapshot)
    {
        if (snapshot.State != _lastState)
        {
            Icon nextIcon = TrayIconFactory.Create(_baseIcon, snapshot.State);
            Icon? previousIcon = _statusIcon;
            _statusIcon = nextIcon;
            _notifyIcon.Icon = nextIcon;
            previousIcon?.Dispose();
        }
        _statusItem.Text = snapshot.Message;
        _activityItem.Text = snapshot.CurrentPath.Length > 0
            ? $"Сейчас: {snapshot.CurrentPath}"
            : snapshot.ObjectCount > 0
                ? $"Доступно объектов: {snapshot.ObjectCount:N0}"
                : "";
        _activityItem.Visible = _activityItem.Text.Length > 0;
        _syncNowItem.Enabled = snapshot.State is not ClientRunState.Starting
            and not ClientRunState.Authorizing;
        string tooltip = $"{AppDefaults.ProductName}: {snapshot.Message}";
        _notifyIcon.Text = tooltip.Length <= 63 ? tooltip : tooltip[..63];

        if (snapshot.State == ClientRunState.Error && _lastState != ClientRunState.Error)
        {
            _notifyIcon.ShowBalloonTip(
                6000,
                AppDefaults.ProductName,
                snapshot.LastError.Length > 0 ? snapshot.LastError : snapshot.Message,
                ToolTipIcon.Error);
        }
        else if (snapshot.State == ClientRunState.UpToDate &&
                 _lastState is ClientRunState.Starting or ClientRunState.Authorizing)
        {
            _notifyIcon.ShowBalloonTip(
                3500,
                AppDefaults.ProductName,
                "Облако подключено и синхронизировано.",
                ToolTipIcon.Info);
        }
        _lastState = snapshot.State;
    }

    private async void OpenSettings()
    {
        IReadOnlyList<string> folders = _provider()?.GetTopLevelFolders() ?? [];
        using SettingsForm dialog = new(_config, folders);
        if (dialog.ShowDialog() == DialogResult.OK && dialog.Selection is not null)
        {
            await SaveSettingsAsync(dialog.Selection);
        }
    }

    private async Task SaveSettingsAsync(ClientSettingsSelection selection)
    {
        try
        {
            await _saveSettings(selection);
            _allOfflineItem.Checked = selection.KeepAllOffline;
        }
        catch (OperationCanceledException) when (_applicationToken.IsCancellationRequested)
        {
            // The application is closing.
        }
        catch (Exception exception)
        {
            WindowsBootstrap.ShowError("Не удалось применить настройки клиента.", exception);
        }
    }

    private async Task RunOperationAsync(
        string operation,
        Func<CloudFilesProvider, Task> action)
    {
        CloudFilesProvider? provider = _provider();
        if (provider is null)
        {
            MessageBox.Show(
                "Клиент ещё не подключён.",
                AppDefaults.ProductName,
                MessageBoxButtons.OK,
                MessageBoxIcon.Information);
            return;
        }
        try
        {
            await action(provider);
        }
        catch (OperationCanceledException) when (_applicationToken.IsCancellationRequested)
        {
            // The application is closing.
        }
        catch (Exception exception)
        {
            WindowsBootstrap.ShowError($"{operation} не выполнена.", exception);
        }
    }

    private static void OpenUrl(string url)
    {
        try
        {
            Process.Start(new ProcessStartInfo(url) { UseShellExecute = true });
        }
        catch (Exception exception)
        {
            WindowsBootstrap.ShowError("Не удалось открыть браузер.", exception);
        }
    }

    private static void OpenLog()
    {
        try
        {
            Directory.CreateDirectory(AppLog.DirectoryPath);
            if (!File.Exists(AppLog.FilePath))
            {
                File.WriteAllText(AppLog.FilePath, "");
            }
            Process.Start(new ProcessStartInfo(AppLog.FilePath) { UseShellExecute = true });
        }
        catch (Exception exception)
        {
            WindowsBootstrap.ShowError("Не удалось открыть журнал.", exception);
        }
    }
}
