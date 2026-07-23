using System.Diagnostics;
using Microsoft.Win32;
using System.Windows.Forms;

namespace RagCloudFiles;

internal static class WindowsBootstrap
{
    private const string ShellPackageName = "TSK.RAGCloudFiles.ShellExtension";
    private const string RegistryPath = @"Software\RAGCloudFiles";
    private const string RunPath = @"Software\Microsoft\Windows\CurrentVersion\Run";
    private const string RunValueName = "RAGCloudFiles";
    private const string CommandStorePath =
        @"Software\Microsoft\Windows\CurrentVersion\Explorer\CommandStore\shell";
    private static readonly string[] ClassicShellTargets =
    [
        @"Software\Classes\*\shell\RAGCloudFiles",
        @"Software\Classes\Directory\shell\RAGCloudFiles",
    ];

    public static string InstallDirectory { get; } = Path.Combine(
        Environment.GetFolderPath(Environment.SpecialFolder.LocalApplicationData),
        "RAG Cloud Files");

    public static string InstalledExecutable { get; } = Path.Combine(
        InstallDirectory,
        "RagCloudFiles.exe");

    public static string UpdateDirectory { get; } = Path.Combine(InstallDirectory, "updates");

    public static bool IsRunningInstalled => string.Equals(
        Path.GetFullPath(Environment.ProcessPath ?? ""),
        Path.GetFullPath(InstalledExecutable),
        StringComparison.OrdinalIgnoreCase);

    public static bool IsInteractiveInstall(string[] args)
    {
        if (args.Length != 0)
        {
            return false;
        }

        string current = Path.GetFullPath(Environment.ProcessPath ?? "");
        return !string.Equals(current, Path.GetFullPath(InstalledExecutable), StringComparison.OrdinalIgnoreCase);
    }

    public static bool InstallAndLaunch()
    {
        string source = Path.GetFullPath(
            Environment.ProcessPath
            ?? throw new InvalidOperationException("Не удалось определить путь запущенного приложения."));
        ConfigStore store = new();
        ProviderConfig config = store.LoadConfig();
        using SetupForm setup = new(
            config.RootPath,
            config.KeepAllOffline,
            config.StartWithWindows);
        if (setup.ShowDialog() != DialogResult.OK)
        {
            return false;
        }

        Directory.CreateDirectory(InstallDirectory);
        StopInstalledProvider();
        File.Copy(source, InstalledExecutable, overwrite: true);

        config.Server = AppDefaults.Server;
        config.RootPath = setup.RootPath;
        config.KeepAllOffline = setup.KeepAllOffline;
        config.StartWithWindows = setup.StartWithWindows;
        store.SaveConfig(config);
        SavePreferences(config);

        AppLog.Info($"Installed {source} to {InstalledExecutable}.");
        Process.Start(new ProcessStartInfo(InstalledExecutable, "--installed")
        {
            UseShellExecute = true,
            WorkingDirectory = InstallDirectory,
        });

        MessageBox.Show(
            "Приложение установлено. Сейчас откроется браузер для входа.\n\n"
            + "После подтверждения облачная папка появится в Проводнике Windows.",
            AppDefaults.ProductName,
            MessageBoxButtons.OK,
            MessageBoxIcon.Information);
        return true;
    }

    private static void StopInstalledProvider()
    {
        foreach (Process process in Process.GetProcessesByName("RagCloudFiles"))
        {
            using (process)
            {
                try
                {
                    if (process.Id == Environment.ProcessId)
                    {
                        continue;
                    }

                    string path = process.MainModule?.FileName ?? "";
                    if (!string.Equals(
                            Path.GetFullPath(path),
                            Path.GetFullPath(InstalledExecutable),
                            StringComparison.OrdinalIgnoreCase))
                    {
                        continue;
                    }

                    process.Kill(entireProcessTree: true);
                    process.WaitForExit(10_000);
                    AppLog.Info($"Stopped installed provider PID {process.Id} before update.");
                }
                catch (Exception exception)
                {
                    AppLog.Error($"Не удалось остановить установленный provider PID {process.Id}.", exception);
                }
            }
        }
    }

    public static void OpenRoot(string rootPath)
    {
        try
        {
            Process.Start(new ProcessStartInfo("explorer.exe", $"\"{Path.GetFullPath(rootPath)}\"")
            {
                UseShellExecute = true,
            });
        }
        catch (Exception exception)
        {
            AppLog.Error("Не удалось открыть облачную папку.", exception);
        }
    }

    public static void SavePreferences(ProviderConfig config)
    {
        using (RegistryKey appKey = Registry.CurrentUser.CreateSubKey(RegistryPath))
        {
            appKey.SetValue("Server", config.Server, RegistryValueKind.String);
            appKey.SetValue("RootPath", config.RootPath, RegistryValueKind.String);
            appKey.SetValue("KeepAllOffline", config.KeepAllOffline ? 1 : 0, RegistryValueKind.DWord);
            appKey.SetValue("StartWithWindows", config.StartWithWindows ? 1 : 0, RegistryValueKind.DWord);
            appKey.SetValue("DeviceId", config.DeviceId, RegistryValueKind.String);
            appKey.SetValue("Executable", InstalledExecutable, RegistryValueKind.String);
        }
        ApplyStartup(config.StartWithWindows);
        InstallClassicContextMenu(config.RootPath);
    }

    public static void RestartInstalled()
    {
        Process.Start(new ProcessStartInfo(InstalledExecutable, "--installed")
        {
            UseShellExecute = true,
            WorkingDirectory = InstallDirectory,
        });
    }

    public static void LaunchStagedUpdate(string stagedExecutable, string sha256)
    {
        ProcessStartInfo start = new(stagedExecutable)
        {
            UseShellExecute = false,
            WorkingDirectory = UpdateDirectory,
        };
        start.ArgumentList.Add("--apply-update");
        start.ArgumentList.Add("--wait-pid");
        start.ArgumentList.Add(Environment.ProcessId.ToString());
        start.ArgumentList.Add("--sha256");
        start.ArgumentList.Add(sha256);
        _ = Process.Start(start)
            ?? throw new InvalidOperationException("Не удалось запустить установщик обновления.");
    }

    public static async Task<int> ApplyStagedUpdateAsync(
        int waitProcessId,
        string expectedSha256)
    {
        try
        {
            string source = Path.GetFullPath(
                Environment.ProcessPath
                ?? throw new InvalidOperationException("Не удалось определить файл обновления."));
            string allowedDirectory = Path.GetFullPath(UpdateDirectory)
                .TrimEnd(Path.DirectorySeparatorChar) + Path.DirectorySeparatorChar;
            if (!source.StartsWith(allowedDirectory, StringComparison.OrdinalIgnoreCase) ||
                !ClientUpdater.IsValidSha256(expectedSha256))
            {
                throw new InvalidDataException("Недопустимый источник или SHA-256 обновления.");
            }

            string actualSha256 = await ClientUpdater.ComputeSha256Async(source, CancellationToken.None);
            if (!actualSha256.Equals(expectedSha256, StringComparison.OrdinalIgnoreCase))
            {
                throw new InvalidDataException("Контрольная сумма файла обновления не совпала.");
            }

            if (waitProcessId > 0 && waitProcessId != Environment.ProcessId)
            {
                try
                {
                    using Process previous = Process.GetProcessById(waitProcessId);
                    using CancellationTokenSource timeout = new(TimeSpan.FromMinutes(2));
                    await previous.WaitForExitAsync(timeout.Token);
                }
                catch (ArgumentException)
                {
                    // The previous client has already exited.
                }
            }

            Directory.CreateDirectory(InstallDirectory);
            Exception? lastError = null;
            for (int attempt = 0; attempt < 20; attempt++)
            {
                try
                {
                    File.Copy(source, InstalledExecutable, overwrite: true);
                    lastError = null;
                    break;
                }
                catch (IOException exception)
                {
                    lastError = exception;
                    await Task.Delay(500);
                }
                catch (UnauthorizedAccessException exception)
                {
                    lastError = exception;
                    await Task.Delay(500);
                }
            }
            if (lastError is not null)
            {
                throw new IOException("Не удалось заменить установленный клиент.", lastError);
            }

            AppLog.Info($"Applied staged update {actualSha256}.");
            RestartInstalled();
            return 0;
        }
        catch (Exception exception)
        {
            ShowError("Не удалось применить обновление RAG Cloud Files.", exception);
            return 1;
        }
    }

    public static void CleanupStagedUpdates()
    {
        if (!Directory.Exists(UpdateDirectory))
        {
            return;
        }

        foreach (string path in Directory.EnumerateFiles(UpdateDirectory))
        {
            try
            {
                File.Delete(path);
            }
            catch (IOException)
            {
                // A just-finished updater may still have its executable open.
            }
            catch (UnauthorizedAccessException)
            {
                // Cleanup is best effort and will be retried on the next start.
            }
        }
    }

    public static async Task<string> GetShellExtensionVersionAsync(
        CancellationToken cancellationToken)
    {
        ProcessStartInfo start = PowerShellStartInfo(
            $"(Get-AppxPackage -Name '{ShellPackageName}' | Select-Object -First 1 -ExpandProperty Version)");
        using Process process = Process.Start(start)
            ?? throw new InvalidOperationException("Не удалось проверить пакет интеграции Проводника.");
        string output = await process.StandardOutput.ReadToEndAsync(cancellationToken);
        await process.WaitForExitAsync(cancellationToken);
        return process.ExitCode == 0 ? output.Trim() : "";
    }

    public static async Task InstallShellExtensionAsync(
        string packagePath,
        CancellationToken cancellationToken)
    {
        string escapedPath = Path.GetFullPath(packagePath).Replace("'", "''", StringComparison.Ordinal);
        ProcessStartInfo start = PowerShellStartInfo(
            $"Add-AppxPackage -Path '{escapedPath}' -ForceUpdateFromAnyVersion -ForceApplicationShutdown");
        using Process process = Process.Start(start)
            ?? throw new InvalidOperationException("Не удалось запустить установку интеграции Проводника.");
        string standardError = await process.StandardError.ReadToEndAsync(cancellationToken);
        await process.WaitForExitAsync(cancellationToken);
        if (process.ExitCode != 0)
        {
            throw new InvalidOperationException(
                $"Windows отклонила пакет интеграции Проводника: {standardError.Trim()}");
        }
        AppLog.Info($"Installed File Explorer shell package {Path.GetFileName(packagePath)}.");
    }

    private static void ApplyStartup(bool enabled)
    {
        using RegistryKey runKey = Registry.CurrentUser.CreateSubKey(RunPath);
        if (enabled)
        {
            runKey.SetValue(
                RunValueName,
                $"\"{InstalledExecutable}\" --installed",
                RegistryValueKind.String);
        }
        else
        {
            runKey.DeleteValue(RunValueName, throwOnMissingValue: false);
        }
    }

    private static ProcessStartInfo PowerShellStartInfo(string command)
    {
        ProcessStartInfo start = new("powershell.exe")
        {
            CreateNoWindow = true,
            RedirectStandardError = true,
            RedirectStandardOutput = true,
            UseShellExecute = false,
            WindowStyle = ProcessWindowStyle.Hidden,
        };
        start.ArgumentList.Add("-NoProfile");
        start.ArgumentList.Add("-NonInteractive");
        start.ArgumentList.Add("-ExecutionPolicy");
        start.ArgumentList.Add("Bypass");
        start.ArgumentList.Add("-Command");
        start.ArgumentList.Add(command);
        return start;
    }

    private static void InstallClassicContextMenu(string rootPath)
    {
        string[] commandNames =
        [
            "RAGCloudFiles.Share",
            "RAGCloudFiles.CopyLink",
            "RAGCloudFiles.ManageAccess",
            "RAGCloudFiles.KeepOffline",
        ];
        string appliesTo = $"System.ItemPathDisplay:~=\"{Path.GetFullPath(rootPath).TrimEnd(Path.DirectorySeparatorChar)}\"";
        foreach (string target in ClassicShellTargets)
        {
            using RegistryKey key = Registry.CurrentUser.CreateSubKey(target);
            key.SetValue("MUIVerb", "RAG Cloud", RegistryValueKind.String);
            key.SetValue("Icon", InstalledExecutable, RegistryValueKind.String);
            key.SetValue("Position", "Top", RegistryValueKind.String);
            key.SetValue("SubCommands", string.Join(';', commandNames), RegistryValueKind.String);
            key.SetValue("AppliesTo", appliesTo, RegistryValueKind.String);
        }

        InstallClassicCommand(commandNames[0], "Поделиться…", "share");
        InstallClassicCommand(commandNames[1], "Скопировать ссылку", "copy-link");
        InstallClassicCommand(commandNames[2], "Управление доступом…", "manage-access");
        InstallClassicCommand(commandNames[3], "Всегда хранить на этом устройстве", "keep-offline");
    }

    private static void InstallClassicCommand(string name, string label, string action)
    {
        using RegistryKey key = Registry.CurrentUser.CreateSubKey($@"{CommandStorePath}\{name}");
        key.SetValue("", label, RegistryValueKind.String);
        key.SetValue("Icon", InstalledExecutable, RegistryValueKind.String);
        using RegistryKey command = key.CreateSubKey("command");
        command.SetValue(
            "",
            $"\"{InstalledExecutable}\" --shell-command {action} --shell-path \"%1\"",
            RegistryValueKind.String);
    }

    public static void ShowError(string message, Exception? exception = null)
    {
        AppLog.Error(message, exception);
        string details = exception is null ? message : $"{message}\n\n{exception.Message}";
        MessageBox.Show(
            $"{details}\n\nЖурнал: {AppLog.FilePath}",
            AppDefaults.ProductName,
            MessageBoxButtons.OK,
            MessageBoxIcon.Error);
    }
}
