using System.Drawing;
using System.Windows.Forms;

namespace RagCloudFiles;

internal sealed record ClientSettingsSelection(
    string RootPath,
    bool KeepAllOffline,
    HashSet<string> OfflinePaths,
    bool StartWithWindows);

internal sealed class SettingsForm : Form
{
    private readonly TextBox _rootPath;
    private readonly CheckBox _keepAllOffline;
    private readonly CheckedListBox _offlineFolders;
    private readonly CheckBox _startWithWindows;
    private readonly HashSet<string> _nestedOfflinePaths;

    public SettingsForm(ProviderConfig config, IReadOnlyList<string> availableFolders)
    {
        Text = "Настройки RAG Cloud Files";
        StartPosition = FormStartPosition.CenterScreen;
        MinimumSize = new Size(600, 520);
        ClientSize = new Size(600, 560);
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
            Anchor = AnchorStyles.Top | AnchorStyles.Left | AnchorStyles.Right,
            Size = new Size(456, 27),
        };
        Button browse = new()
        {
            Text = "Обзор…",
            Location = new Point(490, 45),
            Anchor = AnchorStyles.Top | AnchorStyles.Right,
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
            Anchor = AnchorStyles.Top | AnchorStyles.Bottom | AnchorStyles.Left | AnchorStyles.Right,
            Size = new Size(548, 285),
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
            Location = new Point(24, 458),
            Anchor = AnchorStyles.Bottom | AnchorStyles.Left,
        };
        _startWithWindows = new CheckBox
        {
            Text = "Запускать вместе с Windows",
            Checked = config.StartWithWindows,
            AutoSize = true,
            Location = new Point(24, 488),
            Anchor = AnchorStyles.Bottom | AnchorStyles.Left,
        };
        Button save = new()
        {
            Text = "Сохранить",
            Location = new Point(386, 516),
            Anchor = AnchorStyles.Bottom | AnchorStyles.Right,
            Size = new Size(94, 32),
        };
        Button cancel = new()
        {
            Text = "Отмена",
            DialogResult = DialogResult.Cancel,
            Location = new Point(490, 516),
            Anchor = AnchorStyles.Bottom | AnchorStyles.Right,
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
                _startWithWindows.Checked);
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
