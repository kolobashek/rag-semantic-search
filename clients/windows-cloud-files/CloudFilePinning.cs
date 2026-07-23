using Microsoft.Win32.SafeHandles;
using System.Buffers.Binary;
using System.Runtime.InteropServices;
using Windows.Win32;
using Windows.Win32.Storage.CloudFilters;

namespace RagCloudFiles;

internal static partial class CloudFilePinning
{
    private const uint FileReadData = 0x0001;
    private const uint FileWriteData = 0x0002;
    private const uint FileReadAttributes = 0x0080;
    private const uint FileWriteAttributes = 0x0100;
    private const uint FileShareRead = 0x00000001;
    private const uint FileShareWrite = 0x00000002;
    private const uint FileShareDelete = 0x00000004;
    private const uint OpenExisting = 3;
    private const uint FileFlagBackupSemantics = 0x02000000;
    private const uint ShcneUpdateItem = 0x00002000;
    private const uint ShcneUpdateDir = 0x00001000;
    private const uint ShcneAttributes = 0x00000800;
    private const uint ShcnfPathW = 0x0005;
    private const uint ShcnfFlushNoWait = 0x2000;

    public static bool IsPlaceholder(string path) => TryGetPlaceholderInfo(path, out _);

    public static bool IsInSync(string path) =>
        TryGetPlaceholderInfo(path, out CF_IN_SYNC_STATE state)
        && state == CF_IN_SYNC_STATE.CF_IN_SYNC_STATE_IN_SYNC;

    public static void ConvertToPlaceholder(string path, string cloudPath)
    {
        using SafeFileHandle handle = OpenForPlaceholderManagement(path);
        PInvoke.CfConvertToPlaceholder(
            handle,
            FileIdentityCodec.Encode(cloudPath),
            CF_CONVERT_FLAGS.CF_CONVERT_FLAG_MARK_IN_SYNC).ThrowOnFailure();
        RefreshShell(path);
    }

    public static void MarkInSync(string path)
    {
        using SafeFileHandle handle = OpenForPlaceholderManagement(path);
        PInvoke.CfSetInSyncState(
            handle,
            CF_IN_SYNC_STATE.CF_IN_SYNC_STATE_IN_SYNC,
            CF_SET_IN_SYNC_FLAGS.CF_SET_IN_SYNC_FLAG_NONE).ThrowOnFailure();
        RefreshShell(path);
    }

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
        RefreshShell(path);
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
        RefreshShell(path);
    }

    private static bool TryGetPlaceholderInfo(string path, out CF_IN_SYNC_STATE inSyncState)
    {
        inSyncState = CF_IN_SYNC_STATE.CF_IN_SYNC_STATE_NOT_IN_SYNC;
        using SafeFileHandle handle = CreateFile(
            Path.GetFullPath(path),
            FileReadAttributes,
            FileShareRead | FileShareWrite | FileShareDelete,
            0,
            OpenExisting,
            FileFlagBackupSemantics,
            0);
        if (handle.IsInvalid)
        {
            return false;
        }

        Span<byte> buffer = stackalloc byte[8192];
        if (PInvoke.CfGetPlaceholderInfo(
                handle,
                CF_PLACEHOLDER_INFO_CLASS.CF_PLACEHOLDER_INFO_BASIC,
                buffer).Failed)
        {
            return false;
        }

        inSyncState = (CF_IN_SYNC_STATE)BinaryPrimitives.ReadInt32LittleEndian(buffer[4..8]);
        return true;
    }

    private static SafeFileHandle OpenForPlaceholderManagement(string path)
    {
        SafeFileHandle handle = CreateFile(
            Path.GetFullPath(path),
            FileReadData | FileWriteData | FileReadAttributes | FileWriteAttributes,
            FileShareRead | FileShareWrite | FileShareDelete,
            0,
            OpenExisting,
            FileFlagBackupSemantics,
            0);
        if (handle.IsInvalid)
        {
            throw new IOException(
                $"Не удалось открыть файл для обновления облачного статуса: {path}",
                Marshal.GetExceptionForHR(Marshal.GetHRForLastWin32Error()));
        }

        return handle;
    }

    public static void RefreshShell(string path)
    {
        string fullPath = Path.GetFullPath(path);
        uint eventId = Directory.Exists(fullPath) ? ShcneUpdateDir : ShcneUpdateItem;
        SHChangeNotify(
            eventId | ShcneAttributes,
            ShcnfPathW | ShcnfFlushNoWait,
            fullPath,
            0);
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

    [LibraryImport("shell32.dll", EntryPoint = "SHChangeNotify", StringMarshalling = StringMarshalling.Utf16)]
    private static partial void SHChangeNotify(uint eventId, uint flags, string item1, nint item2);
}
