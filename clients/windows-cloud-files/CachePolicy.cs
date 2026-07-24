namespace RagCloudFiles;

internal sealed record CacheEntry(
    string CloudPath,
    long AllocatedBytes,
    DateTimeOffset LastAccess,
    bool Protected,
    bool InUse);

internal static class CachePolicy
{
    public const int DefaultMaxCacheSizeGb = 20;
    public const int DefaultMinimumFreeSpaceGb = 10;
    public static readonly TimeSpan RecentAccessGrace = TimeSpan.FromMinutes(15);

    public static int NormalizeMaxCacheSizeGb(int value) =>
        Math.Clamp(value <= 0 ? DefaultMaxCacheSizeGb : value, 1, 2048);

    public static int NormalizeMinimumFreeSpaceGb(int value) =>
        Math.Clamp(value <= 0 ? DefaultMinimumFreeSpaceGb : value, 1, 1024);

    public static long BytesFromGb(int value) =>
        checked((long)value * 1024 * 1024 * 1024);

    public static long CalculateBytesToReclaim(
        long allocatedBytes,
        long availableFreeBytes,
        long maximumCacheBytes,
        long minimumFreeBytes)
    {
        long cacheExcess = Math.Max(0, allocatedBytes - maximumCacheBytes);
        long reserveDeficit = Math.Max(0, minimumFreeBytes - availableFreeBytes);
        return Math.Max(cacheExcess, reserveDeficit);
    }

    public static IReadOnlyList<CacheEntry> SelectEvictionCandidates(
        IEnumerable<CacheEntry> entries,
        DateTimeOffset now)
    {
        DateTimeOffset newestAllowed = now - RecentAccessGrace;
        return entries
            .Where(entry =>
                entry.AllocatedBytes > 0
                && !entry.Protected
                && !entry.InUse
                && entry.LastAccess <= newestAllowed)
            .OrderBy(entry => entry.LastAccess)
            .ThenBy(entry => entry.CloudPath, StringComparer.OrdinalIgnoreCase)
            .ToList();
    }
}
