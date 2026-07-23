using System.Runtime.InteropServices;
using System.Text;

namespace RagCloudFiles;

internal static class VirtualDriveManager
{
    private const uint DddRawTargetPath = 0x00000001;
    private const uint DddRemoveDefinition = 0x00000002;
    private const uint DddExactMatchOnRemove = 0x00000004;
    private const uint ShcneDriveAdd = 0x00000100;
    private const uint ShcneDriveRemoved = 0x00000080;
    private const uint ShcnfPathW = 0x0005;

    public static string NormalizeDriveLetter(string? value)
    {
        string normalized = (value ?? "").Trim().TrimEnd(':').ToUpperInvariant();
        return normalized.Length == 1 && normalized[0] is >= 'D' and <= 'Z'
            ? normalized
            : "R";
    }

    public static string EnsureMounted(string rootPath, string preferredLetter)
    {
        string target = ToDosDeviceTarget(rootPath);
        foreach (string letter in CandidateLetters(preferredLetter))
        {
            string device = letter + ":";
            string? existing = QueryTarget(device);
            if (existing is not null)
            {
                if (TargetsEqual(existing, target))
                {
                    return letter;
                }

                continue;
            }

            if (!DefineDosDevice(DddRawTargetPath, device, target))
            {
                continue;
            }

            NotifyDrive(ShcneDriveAdd, letter);
            AppLog.Info($"Mounted cloud root {Path.GetFullPath(rootPath)} as {device}.");
            return letter;
        }

        throw new IOException("Не удалось найти свободную букву диска для RAG Cloud Drive.");
    }

    public static void RemoveForRoot(string rootPath)
    {
        string target = ToDosDeviceTarget(rootPath);
        foreach (char letter in Enumerable.Range('D', 'Z' - 'D' + 1).Select(value => (char)value))
        {
            string device = $"{letter}:";
            string? existing = QueryTarget(device);
            if (existing is null || !TargetsEqual(existing, target))
            {
                continue;
            }

            if (DefineDosDevice(
                    DddRemoveDefinition | DddExactMatchOnRemove | DddRawTargetPath,
                    device,
                    target))
            {
                NotifyDrive(ShcneDriveRemoved, letter.ToString());
                AppLog.Info($"Unmounted cloud drive {device}.");
            }
        }
    }

    internal static IReadOnlyList<string> CandidateLetters(string preferredLetter)
    {
        string preferred = NormalizeDriveLetter(preferredLetter);
        List<string> candidates = [preferred];
        foreach (char letter in "RSTUVWXYZDEFGHIJKLMNOPQ")
        {
            string value = letter.ToString();
            if (!candidates.Contains(value, StringComparer.OrdinalIgnoreCase))
            {
                candidates.Add(value);
            }
        }

        return candidates;
    }

    private static string ToDosDeviceTarget(string rootPath) =>
        @"\??\" + Path.GetFullPath(rootPath).TrimEnd(Path.DirectorySeparatorChar);

    private static bool TargetsEqual(string left, string right) =>
        left.TrimEnd(Path.DirectorySeparatorChar)
            .Equals(right.TrimEnd(Path.DirectorySeparatorChar), StringComparison.OrdinalIgnoreCase);

    private static string? QueryTarget(string device)
    {
        StringBuilder buffer = new(4096);
        return QueryDosDevice(device, buffer, buffer.Capacity) == 0
            ? null
            : buffer.ToString();
    }

    private static void NotifyDrive(uint eventId, string letter) =>
        SHChangeNotify(eventId, ShcnfPathW, $"{letter}:\\", 0);

    [DllImport("kernel32.dll", EntryPoint = "DefineDosDeviceW", SetLastError = true, CharSet = CharSet.Unicode)]
    [return: MarshalAs(UnmanagedType.Bool)]
    private static extern bool DefineDosDevice(uint flags, string deviceName, string? targetPath);

    [DllImport("kernel32.dll", EntryPoint = "QueryDosDeviceW", SetLastError = true, CharSet = CharSet.Unicode)]
    private static extern uint QueryDosDevice(string deviceName, StringBuilder targetPath, int maxLength);

    [DllImport("shell32.dll", EntryPoint = "SHChangeNotify", CharSet = CharSet.Unicode)]
    private static extern void SHChangeNotify(uint eventId, uint flags, string item1, nint item2);
}
