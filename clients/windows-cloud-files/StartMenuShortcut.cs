using System.Runtime.InteropServices;

namespace RagCloudFiles;

internal static class StartMenuShortcut
{
    private static string ShortcutPath => Path.Combine(
        Environment.GetFolderPath(Environment.SpecialFolder.Programs), "RAG Cloud Files.lnk");

    public static void EnsureInstalled()
    {
        Directory.CreateDirectory(Path.GetDirectoryName(ShortcutPath)!);
        Type type = Type.GetTypeFromProgID("WScript.Shell")
            ?? throw new InvalidOperationException("Windows shortcut service is unavailable.");
        dynamic shell = Activator.CreateInstance(type)!;
        object? shortcut = null;
        try
        {
            shortcut = shell.CreateShortcut(ShortcutPath);
            dynamic link = shortcut;
            link.TargetPath = WindowsBootstrap.InstalledExecutable;
            link.Arguments = "--installed";
            link.WorkingDirectory = WindowsBootstrap.InstallDirectory;
            link.Description = AppDefaults.ProductName;
            link.IconLocation = WindowsBootstrap.InstalledExecutable + ",0";
            link.Save();
        }
        finally
        {
            if (shortcut is not null) Marshal.FinalReleaseComObject(shortcut);
            Marshal.FinalReleaseComObject(shell);
        }
    }

    public static void Remove() => File.Delete(ShortcutPath);
}
