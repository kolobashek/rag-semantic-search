using Microsoft.Win32.SafeHandles;
using System.Runtime.InteropServices;
using Windows.Win32;
using Windows.Win32.Storage.CloudFilters;

namespace RagCloudFiles;

internal static partial class CloudFilePinning
{
    private const uint FileReadData = 0x0001;
    private const uint FileReadAttributes = 0x0080;
    private const uint FileShareRead = 0x00000001;
    private const uint FileShareWrite = 0x00000002;
    private const uint FileShareDelete = 0x00000004;
    private const uint OpenExisting = 3;
    private const uint FileFlagBackupSemantics = 0x02000000;

    public static void SetPinState(string path, bool pinned, bool recursive)
    {
        using SafeFileHandle handle = CreateFile(
            Path.GetFullPath(path),
            FileReadData | FileReadAttributes,
            FileShareRead | FileShareWrite | FileShareDelete,
            0,
            OpenExisting,
            FileFlagBackupSemantics,
            0);
        if (handle.IsInvalid)
        {
            throw new IOException(
                $"Не удалось открыть placeholder: {path}",
                Marshal.GetExceptionForHR(Marshal.GetHRForLastWin32Error()));
        }

        CF_SET_PIN_FLAGS flags = recursive
            ? CF_SET_PIN_FLAGS.CF_SET_PIN_FLAG_RECURSE
            : CF_SET_PIN_FLAGS.CF_SET_PIN_FLAG_NONE;
        PInvoke.CfSetPinState(
            handle,
            pinned ? CF_PIN_STATE.CF_PIN_STATE_PINNED : CF_PIN_STATE.CF_PIN_STATE_UNPINNED,
            flags).ThrowOnFailure();
    }

    public static void HydrateFile(string path)
    {
        using SafeFileHandle handle = CreateFile(
            Path.GetFullPath(path),
            FileReadData | FileReadAttributes,
            FileShareRead | FileShareWrite | FileShareDelete,
            0,
            OpenExisting,
            0,
            0);
        if (handle.IsInvalid)
        {
            throw new IOException(
                $"Не удалось открыть файл для офлайн-загрузки: {path}",
                Marshal.GetExceptionForHR(Marshal.GetHRForLastWin32Error()));
        }

        PInvoke.CfHydratePlaceholder(
            handle,
            StartingOffset: 0,
            Length: -1,
            CF_HYDRATE_FLAGS.CF_HYDRATE_FLAG_NONE).ThrowOnFailure();
    }

    [LibraryImport("kernel32.dll", EntryPoint = "CreateFileW", SetLastError = true, StringMarshalling = StringMarshalling.Utf16)]
    private static partial SafeFileHandle CreateFile(
        string fileName,
        uint desiredAccess,
        uint shareMode,
        nint securityAttributes,
        uint creationDisposition,
        uint flagsAndAttributes,
        nint templateFile);
}
