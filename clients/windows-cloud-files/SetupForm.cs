using System.Drawing;
using System.Windows.Forms;

namespace RagCloudFiles;

internal sealed class SetupForm : Form
{
    private readonly TextBox _rootPath;
    private readonly CheckBox _keepAllOffline;
    private readonly NumericUpDown _maxCacheSizeGb;
    private readonly NumericUpDown _minimumFreeSpaceGb;
    private readonly CheckBox _startWithWindows;
    private readonly CheckBox _mountAsDrive;
    private readonly ComboBox _driveLetter;

    public SetupForm(
        string defaultRoot,
        bool keepAllOffline = false,
        int maxCacheSizeGb = CachePolicy.DefaultMaxCacheSizeGb,
        int minimumFreeSpaceGb = CachePolicy.DefaultMinimumFreeSpaceGb,
        bool startWithWindows = true,
        bool mountAsDrive = true,
        string driveLetter = "R")
    {
        Text = "Установка RAG Cloud Files";
        StartPosition = FormStartPosition.CenterScreen;
        FormBorderStyle = FormBorderStyle.FixedDialog;
        MaximizeBox = false;
        MinimizeBox = false;
        ClientSize = new Size(560, 520);
        Font = new Font("Segoe UI", 9F);
        Icon = Icon.ExtractAssociatedIcon(Environment.ProcessPath ?? "") ?? SystemIcons.Application;

        Label title = new()
        {
            Text = "Корпоративное облако",
            Font = new Font(Font.FontFamily, 17F, FontStyle.Bold),
            AutoSize = true,
            Location = new Point(28, 24),
        };
        Label description = new()
        {
            Text = "Все доступные файлы будут видны в Проводнике Windows.\n"
                + "По умолчанию содержимое загружается только при открытии.",
            AutoSize = true,
            ForeColor = Color.DimGray,
            Location = new Point(30, 68),
        };
        Label locationLabel = new()
        {
            Text = "Расположение облачной папки",
            AutoSize = true,
            Location = new Point(30, 128),
        };
        _rootPath = new TextBox
        {
            Text = defaultRoot,
            Location = new Point(30, 151),
            Size = new Size(420, 27),
        };
        Button browse = new()
        {
            Text = "Обзор…",
            Location = new Point(458, 149),
            Size = new Size(74, 30),
        };
        browse.Click += (_, _) => BrowseForRoot();

        _keepAllOffline = new CheckBox
        {
            Text = "Скачать всё облако и всегда хранить на этом компьютере",
            Checked = keepAllOffline,
            AutoSize = true,
            Location = new Point(30, 205),
        };
        Label offlineHint = new()
        {
            Text = "Требует свободного места в объёме всего доступного диска. "
                + "Отдельные папки можно выбрать позже в настройках.",
            AutoSize = false,
            ForeColor = Color.DimGray,
            Location = new Point(50, 231),
            Size = new Size(475, 42),
        };
        Label maxCacheLabel = new()
        {
            Text = "Максимальный локальный кэш",
            AutoSize = true,
            Location = new Point(30, 292),
        };
        _maxCacheSizeGb = new NumericUpDown
        {
            Minimum = 1,
            Maximum = 2048,
            Value = CachePolicy.NormalizeMaxCacheSizeGb(maxCacheSizeGb),
            Location = new Point(236, 287),
            Size = new Size(82, 27),
        };
        Label maxCacheUnit = new()
        {
            Text = "ГБ",
            AutoSize = true,
            Location = new Point(326, 292),
        };
        Label freeSpaceLabel = new()
        {
            Text = "Резерв свободного места",
            AutoSize = true,
            Location = new Point(30, 330),
        };
        _minimumFreeSpaceGb = new NumericUpDown
        {
            Minimum = 1,
            Maximum = 1024,
            Value = CachePolicy.NormalizeMinimumFreeSpaceGb(minimumFreeSpaceGb),
            Location = new Point(236, 325),
            Size = new Size(82, 27),
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
            Location = new Point(326, 330),
        };
        _startWithWindows = new CheckBox
        {
            Text = "Запускать вместе с Windows",
            Checked = startWithWindows,
            AutoSize = true,
            Location = new Point(30, 420),
        };
        _mountAsDrive = new CheckBox
        {
            Text = "Показывать облако отдельным диском",
            Checked = mountAsDrive,
            AutoSize = true,
            Location = new Point(30, 375),
        };
        _driveLetter = new ComboBox
        {
            DropDownStyle = ComboBoxStyle.DropDownList,
            Location = new Point(320, 370),
            Size = new Size(70, 28),
        };
        foreach (char letter in "RSTUVWXYZDEFGHIJKLMNOPQ")
        {
            _driveLetter.Items.Add($"{letter}:");
        }
        _driveLetter.SelectedItem = VirtualDriveManager.NormalizeDriveLetter(driveLetter) + ":";
        _driveLetter.Enabled = _mountAsDrive.Checked;
        _mountAsDrive.CheckedChanged += (_, _) => _driveLetter.Enabled = _mountAsDrive.Checked;

        Button install = new()
        {
            Text = "Установить",
            DialogResult = DialogResult.OK,
            Location = new Point(350, 466),
            Size = new Size(100, 32),
        };
        Button cancel = new()
        {
            Text = "Отмена",
            DialogResult = DialogResult.Cancel,
            Location = new Point(458, 466),
            Size = new Size(74, 32),
        };
        install.Click += (_, _) =>
        {
            if (ValidateRoot())
            {
                return;
            }
            DialogResult = DialogResult.None;
        };

        Controls.AddRange([
            title,
            description,
            locationLabel,
            _rootPath,
            browse,
            _keepAllOffline,
            offlineHint,
            maxCacheLabel,
            _maxCacheSizeGb,
            maxCacheUnit,
            freeSpaceLabel,
            _minimumFreeSpaceGb,
            freeSpaceUnit,
            _mountAsDrive,
            _driveLetter,
            _startWithWindows,
            install,
            cancel,
        ]);
        AcceptButton = install;
        CancelButton = cancel;
    }

    public string RootPath => Path.GetFullPath(
        Environment.ExpandEnvironmentVariables(_rootPath.Text.Trim()));

    public bool KeepAllOffline => _keepAllOffline.Checked;

    public int MaxCacheSizeGb => decimal.ToInt32(_maxCacheSizeGb.Value);

    public int MinimumFreeSpaceGb => decimal.ToInt32(_minimumFreeSpaceGb.Value);

    public bool StartWithWindows => _startWithWindows.Checked;

    public bool MountAsDrive => _mountAsDrive.Checked;

    public string DriveLetter => VirtualDriveManager.NormalizeDriveLetter(
        Convert.ToString(_driveLetter.SelectedItem) ?? "R");

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

    private bool ValidateRoot()
    {
        try
        {
            if (string.IsNullOrWhiteSpace(_rootPath.Text))
            {
                throw new InvalidOperationException("Путь не может быть пустым.");
            }
            string path = RootPath;
            if (File.Exists(path))
            {
                throw new InvalidOperationException("Выбранный путь занят файлом.");
            }
            return true;
        }
        catch (Exception exception)
        {
            MessageBox.Show(
                this,
                $"Укажите корректное расположение облака.\n\n{exception.Message}",
                AppDefaults.ProductName,
                MessageBoxButtons.OK,
                MessageBoxIcon.Warning);
            return false;
        }
    }
}
