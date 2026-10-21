using System.Net;

namespace RagCloudFiles;

internal sealed class AccessDeniedSyncException(
    string path, string message, DateTimeOffset retryAt, bool requestAttempted, Exception? inner = null)
    : HttpRequestException(message, inner, HttpStatusCode.Forbidden)
{
    internal string CloudPath { get; } = path;
    internal DateTimeOffset RetryAt { get; } = retryAt;
    internal bool RequestAttempted { get; } = requestAttempted;
}

// Accessed under the provider refresh lock, shared by watcher and full reconciliation.
internal sealed class AccessDeniedRetries(Func<DateTimeOffset>? clock = null)
{
    private readonly Func<DateTimeOffset> _clock = clock ?? (() => DateTimeOffset.UtcNow);
    private readonly Dictionary<string, AccessDeniedSyncException> _denied = new(StringComparer.OrdinalIgnoreCase);
    internal static readonly TimeSpan Delay = TimeSpan.FromMinutes(5);

    internal void Clear() => _denied.Clear();
    internal void Forget(string path) => _denied.Remove(path);

    internal async Task<T> ExecuteAsync<T>(string path, Func<Task<T>> action)
    {
        if (_denied.TryGetValue(path, out AccessDeniedSyncException? prior) && prior.RetryAt > _clock())
            throw new AccessDeniedSyncException(path, prior.Message, prior.RetryAt, requestAttempted: false);
        try
        {
            T result = await action();
            Forget(path);
            return result;
        }
        catch (HttpRequestException ex) when (ex.StatusCode == HttpStatusCode.Forbidden)
        {
            AccessDeniedSyncException denied = new(path,
                $"{path}: {ex.Message} Повторная проверка доступа через 5 минут или после изменения прав.",
                _clock() + Delay, requestAttempted: true, ex);
            _denied[path] = denied;
            throw denied;
        }
    }
}
