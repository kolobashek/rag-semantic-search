using System.Drawing;
using System.Windows.Forms;

namespace RagCloudFiles;

internal sealed record ClientSettingsSelection(
    string RootPath,
    bool KeepAllOffline,
    HashSet<string> OfflinePaths,
    int MaxCacheSizeGb,
    int MinimumFreeSpaceGb,
    bool StartWithWindows,
    bool MountAsDrive,
    string DriveLetter);

internal sealed class SettingsForm : Form
{
    private readonly TextBox _rootPath;
    private readonly CheckBox _keepAllOffline;
    private readonly CheckedListBox _offlineFolders;
    private readonly NumericUpDown _maxCacheSizeGb;
    private readonly NumericUpDown _minimumFreeSpaceGb;
    private readonly CheckBox _startWithWindows;
    private readonly CheckBox _mountAsDrive;
    private readonly ComboBox _driveLetter;
    private readonly HashSet<string> _nestedOfflinePaths;

    public SettingsForm(ProviderConfig config, IReadOnlyList<string> availableFolders)
    {
        Text = "Настройки RAG Cloud Files";
        StartPosition = FormStartPosition.CenterScreen;
        FormBorderStyle = FormBorderStyle.FixedDialog;
        MaximizeBox = false;
        MinimizeBox = false;
        ClientSize = new Size(620, 650);
        Font = new Font("Segoe UI", 9F);
        Icon = Icon.ExtractAssociatedIcon(Environment.ProcessPath ?? "") ?? SystemIcons.Application;
        _nestedOfflinePaths = config.OfflinePaths
            .Where(path => !availableFolders.Contains(path, StringComparer.OrdinalIgnoreCase))
            .ToHashSet(StringComparer.OrdinalIgnoreCase);

        Label locationLabel = new()
        {
            Text = "Расположение облака на компьютере",
            AutoSize = true,
            Location = new Point(24, 22),
        };
        _rootPath = new TextBox
        {
            Text = config.RootPath,
            Location = new Point(24, 47),
            Size = new Size(476, 27),
        };
        Button browse = new()
        {
            Text = "Обзор…",
            Location = new Point(510, 45),
            Size = new Size(82, 30),
        };
        browse.Click += (_, _) => BrowseForRoot();

        _keepAllOffline = new CheckBox
        {
            Text = "Всегда хранить всё облако на этом компьютере",
            Checked = config.KeepAllOffline,
            AutoSize = true,
            Location = new Point(24, 98),
        };
        Label folderLabel = new()
        {
            Text = "Папки, доступные офлайн",
            AutoSize = true,
            Location = new Point(24, 137),
        };
        _offlineFolders = new CheckedListBox
        {
            CheckOnClick = true,
            Location = new Point(24, 162),
            Size = new Size(568, 170),
            IntegralHeight = false,
        };
        foreach (string folder in availableFolders)
        {
            _offlineFolders.Items.Add(folder, config.OfflinePaths.Contains(folder));
        }
        if (availableFolders.Count == 0)
        {
            _offlineFolders.Items.Add("Папки появятся после первой синхронизации", false);
            _offlineFolders.Enabled = false;
        }
        _keepAllOffline.CheckedChanged += (_, _) =>
        {
            _offlineFolders.Enabled = !_keepAllOffline.Checked && availableFolders.Count > 0;
        };
        _offlineFolders.Enabled = !_keepAllOffline.Checked && availableFolders.Count > 0;

        Label hint = new()
        {
            Text = "Невыбранные папки остаются видимыми, но файлы загружаются только при открытии.",
            AutoSize = true,
            ForeColor = Color.DimGray,
            Location = new Point(24, 344),
        };
        Label cacheTitle = new()
        {
            Text = "Локальный кэш Files On-Demand",
            Font = new Font(Font, FontStyle.Bold),
            AutoSize = true,
            Location = new Point(24, 376),
        };
        Label maxCacheLabel = new()
        {
            Text = "Максимальный объём кэша",
            AutoSize = true,
            Location = new Point(24, 409),
        };
        _maxCacheSizeGb = new NumericUpDown
        {
            Minimum = 1,
            Maximum = 2048,
            Value = CachePolicy.NormalizeMaxCacheSizeGb(config.MaxCacheSizeGb),
            Location = new Point(220, 404),
            Size = new Size(86, 27),
        };
        Label maxCacheUnit = new()
        {
            Text = "ГБ",
            AutoSize = true,
            Location = new Point(314, 409),
        };
        Label freeSpaceLabel = new()
        {
            Text = "Оставлять свободными не менее",
            AutoSize = true,
            Location = new Point(24, 447),
        };
        _minimumFreeSpaceGb = new NumericUpDown
        {
            Minimum = 1,
            Maximum = 1024,
            Value = CachePolicy.NormalizeMinimumFreeSpaceGb(config.MinimumFreeSpaceGb),
            Location = new Point(250, 442),
            Size = new Size(86, 27),
        };
        _maxCacheSizeGb.Enabled = !_keepAllOffline.Checked;
        _minimumFreeSpaceGb.Enabled = !_keepAllOffline.Checked;
        _keepAllOffline.CheckedChanged += (_, _) =>
        {
            _maxCacheSizeGb.Enabled = !_keepAllOffline.Checked;
            _minimumFreeSpaceGb.Enabled = !_keepAllOffline.Checked;
        };
        Label freeSpaceUnit = new()
        {
            Text = "ГБ",
            AutoSize = true,
            Location = new Point(344, 447),
        };
        Label cacheHint = new()
        {
            Text = "Давно неиспользуемые файлы освобождаются автоматически. "
                + "Закреплённые офлайн-папки не очищаются и могут превысить лимит.",
            AutoSize = false,
            ForeColor = Color.DimGray,
            Location = new Point(24, 478),
            Size = new Size(568, 40),
        };
        _startWithWindows = new CheckBox
        {
            Text = "Запускать вместе с Windows",
            Checked = config.StartWithWindows,
            AutoSize = true,
            Location = new Point(24, 563),
        };
        _mountAsDrive = new CheckBox
        {
            Text = "Показывать облако отдельным диском",
            Checked = config.MountAsDrive,
            AutoSize = true,
            Location = new Point(24, 528),
        };
        _driveLetter = new ComboBox
        {
            DropDownStyle = ComboBoxStyle.DropDownList,
            Location = new Point(314, 523),
            Size = new Size(70, 28),
        };
        foreach (char letter in "RSTUVWXYZDEFGHIJKLMNOPQ")
        {
            _driveLetter.Items.Add($"{letter}:");
        }
        _driveLetter.SelectedItem = VirtualDriveManager.NormalizeDriveLetter(config.DriveLetter) + ":";
        _driveLetter.Enabled = _mountAsDrive.Checked;
        _mountAsDrive.CheckedChanged += (_, _) => _driveLetter.Enabled = _mountAsDrive.Checked;
        Button save = new()
        {
            Text = "Сохранить",
            Location = new Point(406, 602),
            Size = new Size(94, 32),
        };
        Button cancel = new()
        {
            Text = "Отмена",
            DialogResult = DialogResult.Cancel,
            Location = new Point(510, 602),
            Size = new Size(82, 32),
        };
        save.Click += (_, _) =>
        {
            if (TryCreateSelection(out ClientSettingsSelection? selection))
            {
                Selection = selection;
                DialogResult = DialogResult.OK;
                Close();
            }
        };

        Controls.AddRange([
            locationLabel,
            _rootPath,
            browse,
            _keepAllOffline,
            folderLabel,
            _offlineFolders,
            hint,
            cacheTitle,
            maxCacheLabel,
            _maxCacheSizeGb,
            maxCacheUnit,
            freeSpaceLabel,
            _minimumFreeSpaceGb,
            freeSpaceUnit,
            cacheHint,
            _mountAsDrive,
            _driveLetter,
            _startWithWindows,
            save,
            cancel,
        ]);
        AcceptButton = save;
        CancelButton = cancel;
    }

    public ClientSettingsSelection? Selection { get; private set; }

    private void BrowseForRoot()
    {
        using FolderBrowserDialog dialog = new()
        {
            Description = "Выберите расположение корпоративного облака",
            UseDescriptionForTitle = true,
            SelectedPath = Directory.Exists(_rootPath.Text) ? _rootPath.Text : "",
            ShowNewFolderButton = true,
        };
        if (dialog.ShowDialog(this) == DialogResult.OK)
        {
            _rootPath.Text = dialog.SelectedPath;
        }
    }

    private bool TryCreateSelection(out ClientSettingsSelection? selection)
    {
        selection = null;
        try
        {
            if (string.IsNullOrWhiteSpace(_rootPath.Text))
            {
                throw new InvalidOperationException("Путь не может быть пустым.");
            }
            string root = Path.GetFullPath(
                Environment.ExpandEnvironmentVariables(_rootPath.Text.Trim()));
            if (File.Exists(root))
            {
                throw new InvalidOperationException("Выбранный путь занят файлом.");
            }
            HashSet<string> offline = _offlineFolders.CheckedItems
                .Cast<object>()
                .Select(item => Convert.ToString(item) ?? "")
                .Where(item => item.Length > 0)
                .ToHashSet(StringComparer.OrdinalIgnoreCase);
            offline.UnionWith(_nestedOfflinePaths);
            selection = new ClientSettingsSelection(
                root,
                _keepAllOffline.Checked,
                offline,
                decimal.ToInt32(_maxCacheSizeGb.Value),
                decimal.ToInt32(_minimumFreeSpaceGb.Value),
                _startWithWindows.Checked,
                _mountAsDrive.Checked,
                VirtualDriveManager.NormalizeDriveLetter(
                    Convert.ToString(_driveLetter.SelectedItem) ?? "R"));
            return true;
        }
        catch (Exception exception)
        {
            MessageBox.Show(
                this,
                $"Не удалось сохранить настройки.\n\n{exception.Message}",
                AppDefaults.ProductName,
                MessageBoxButtons.OK,
                MessageBoxIcon.Warning);
            return false;
        }
    }
}
