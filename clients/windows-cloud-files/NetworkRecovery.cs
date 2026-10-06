using System.Net;

namespace RagCloudFiles;

internal static class NetworkRecovery
{
    internal static bool IsTransient(Exception error, CancellationToken cancellationToken) =>
        !cancellationToken.IsCancellationRequested && (error is HttpRequestException http &&
            (http.StatusCode is null or HttpStatusCode.RequestTimeout or HttpStatusCode.TooManyRequests
                or HttpStatusCode.BadGateway or HttpStatusCode.ServiceUnavailable or HttpStatusCode.GatewayTimeout)
            || error is OperationCanceledException);

    // Only use for reads or idempotent device registration, never uploads/deletes.
    internal static async Task<T> ExecuteAsync<T>(Func<Task<T>> action, CancellationToken cancellationToken,
        Action<Exception, TimeSpan>? onRetry = null, int maxAttempts = int.MaxValue,
        Func<TimeSpan, CancellationToken, Task>? delay = null)
    {
        delay ??= Task.Delay;
        for (int attempt = 1; ; attempt++)
        {
            cancellationToken.ThrowIfCancellationRequested();
            try { return await action(); }
            catch (Exception error) when (attempt < maxAttempts && IsTransient(error, cancellationToken))
            {
                TimeSpan wait = TimeSpan.FromSeconds(Math.Min(60, 5 * Math.Pow(2, Math.Min(attempt - 1, 4))));
                onRetry?.Invoke(error, wait);
                await delay(wait, cancellationToken);
            }
        }
    }

    internal static void Report(ClientStatusModel status, Exception error, TimeSpan wait)
    {
        status.SetState(ClientRunState.Offline, $"Нет связи с сервером. Повтор через {wait.TotalSeconds:0} с.", error.Message);
        AppLog.Warn($"Temporary server failure; retry in {wait.TotalSeconds:0}s: {error.Message}");
    }
}
