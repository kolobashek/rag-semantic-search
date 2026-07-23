using System.Diagnostics;
using Microsoft.Win32;
using System.Windows.Forms;

namespace RagCloudFiles;

internal static class WindowsBootstrap
{
    private const string RegistryPath = @"Software\RAGCloudFiles";
    private const string RunPath = @"Software\Microsoft\Windows\CurrentVersion\Run";
    private const string RunValueName = "RAGCloudFiles";

    public static string InstallDirectory { get; } = Path.Combine(
        Environment.GetFolderPath(Environment.SpecialFolder.LocalApplicationData),
        "RAG Cloud Files");

    public static string InstalledExecutable { get; } = Path.Combine(
        InstallDirectory,
        "RagCloudFiles.exe");

    public static bool IsInteractiveInstall(string[] args)
    {
        if (args.Length != 0)
        {
            return false;
        }

        string current = Path.GetFullPath(Environment.ProcessPath ?? "");
        return !string.Equals(current, Path.GetFullPath(InstalledExecutable), StringComparison.OrdinalIgnoreCase);
    }

    public static void InstallAndLaunch()
    {
        string source = Path.GetFullPath(
            Environment.ProcessPath
            ?? throw new InvalidOperationException("Не удалось определить путь запущенного приложения."));
        Directory.CreateDirectory(InstallDirectory);
        StopInstalledProvider();
        File.Copy(source, InstalledExecutable, overwrite: true);

        string rootPath = Path.Combine(
            Environment.GetFolderPath(Environment.SpecialFolder.UserProfile),
            "RAG Cloud Drive");
        using (RegistryKey appKey = Registry.CurrentUser.CreateSubKey(RegistryPath))
        {
            appKey.SetValue("Server", AppDefaults.Server, RegistryValueKind.String);
            if (string.IsNullOrWhiteSpace(Convert.ToString(appKey.GetValue("RootPath"))))
            {
                appKey.SetValue("RootPath", rootPath, RegistryValueKind.String);
            }
            if (string.IsNullOrWhiteSpace(Convert.ToString(appKey.GetValue("DeviceId"))))
            {
                appKey.SetValue(
                    "DeviceId",
                    $"win-{Environment.MachineName}-{Guid.NewGuid():N}",
                    RegistryValueKind.String);
            }
        }

        using (RegistryKey runKey = Registry.CurrentUser.CreateSubKey(RunPath))
        {
            runKey.SetValue(
                RunValueName,
                $"\"{InstalledExecutable}\" --installed",
                RegistryValueKind.String);
        }

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
