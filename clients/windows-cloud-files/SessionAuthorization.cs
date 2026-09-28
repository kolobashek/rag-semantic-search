using System.Net;

namespace RagCloudFiles;

internal static class SessionAuthorization
{
    public static void ValidateClientIdentity(string previousId, string registeredId)
    {
        if (previousId.Length > 0 && previousId != registeredId)
            throw new InvalidOperationException(
                "Устройство связано с другой учётной записью или изменилось на сервере. " +
                "Войдите под прежним пользователем. Локальные файлы сохранены.");
    }

    public static async Task<string> RegisterAsync(
        string savedToken,
        Func<string, Task<string>> register,
        Func<Task<string>> authorize)
    {
        if (savedToken.Length > 0)
        {
            try
            {
                return await register(savedToken);
            }
            catch (HttpRequestException exception) when (exception.StatusCode == HttpStatusCode.Unauthorized)
            {
                AppLog.Info("Saved session rejected; requesting device authorization.");
            }
        }

        // A newly issued token gets one attempt, never an endless login loop.
        string token = await authorize();
        return await register(token);
    }
}

internal sealed class SessionExpiryHandler(Action expired, HttpMessageHandler inner) : DelegatingHandler(inner)
{
    private int _notified;

    protected override async Task<HttpResponseMessage> SendAsync(
        HttpRequestMessage request, CancellationToken cancellationToken)
    {
        HttpResponseMessage response = await base.SendAsync(request, cancellationToken);
        if (response.StatusCode == HttpStatusCode.Unauthorized &&
            Interlocked.Exchange(ref _notified, 1) == 0)
        {
            expired();
        }
        return response;
    }

    public void Reset() => Interlocked.Exchange(ref _notified, 0);
}
