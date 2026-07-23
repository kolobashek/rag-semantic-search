using System.Diagnostics;
using System.Net;
using System.Net.Http.Headers;
using System.Net.Http.Json;
using System.Text.Json;

namespace RagCloudFiles;

internal sealed class CloudDriveApi : IDisposable
{
    private static readonly JsonSerializerOptions JsonOptions = new()
    {
        PropertyNameCaseInsensitive = true,
    };

    private readonly HttpClient _http;

    public CloudDriveApi(string server, string token)
    {
        _http = new HttpClient(new HttpClientHandler { AllowAutoRedirect = true })
        {
            BaseAddress = new Uri(server.TrimEnd('/') + "/"),
            Timeout = TimeSpan.FromMinutes(10),
        };
        _http.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("Bearer", token);
        _http.DefaultRequestHeaders.UserAgent.ParseAdd($"RAGCloudFiles/{AppDefaults.Version}");
    }

    public static async Task<DeviceTokenResponse> AuthorizeDeviceAsync(string server, CancellationToken cancellationToken)
    {
        using HttpClient client = new() { BaseAddress = new Uri(server.TrimEnd('/') + "/") };
        using HttpResponseMessage response = await client.PostAsync("api/auth/device/code", null, cancellationToken);
        response.EnsureSuccessStatusCode();
        DeviceCodeResponse code = await response.Content.ReadFromJsonAsync<DeviceCodeResponse>(JsonOptions, cancellationToken)
            ?? throw new InvalidOperationException("Сервер вернул пустой device code.");
        string verificationUrl = code.VerificationUriComplete.Length > 0
            ? code.VerificationUriComplete
            : code.VerificationUri;

        AppLog.Info($"Opening device authorization URL {verificationUrl}; code {code.UserCode}.");
        Console.WriteLine($"Откройте {verificationUrl}");
        Console.WriteLine($"Код подтверждения: {code.UserCode}");
        try
        {
            Process.Start(new ProcessStartInfo(verificationUrl) { UseShellExecute = true });
        }
        catch (Exception exception)
        {
            AppLog.Error("Не удалось открыть браузер для авторизации устройства.", exception);
            WindowsBootstrap.ShowError(
                $"Не удалось открыть браузер. Откройте ссылку вручную:\n{verificationUrl}",
                exception);
        }

        DateTimeOffset deadline = DateTimeOffset.UtcNow.AddSeconds(Math.Max(30, code.ExpiresIn));
        while (DateTimeOffset.UtcNow < deadline)
        {
            await Task.Delay(TimeSpan.FromSeconds(Math.Max(1, code.Interval)), cancellationToken);
            string path = $"api/auth/device/token?device_code={Uri.EscapeDataString(code.DeviceCode)}";
            using HttpResponseMessage poll = await client.GetAsync(path, cancellationToken);
            if ((int)poll.StatusCode == 428)
            {
                continue;
            }

            poll.EnsureSuccessStatusCode();
            return await poll.Content.ReadFromJsonAsync<DeviceTokenResponse>(JsonOptions, cancellationToken)
                ?? throw new InvalidOperationException("Сервер не вернул токен устройства.");
        }

        throw new TimeoutException("Истекло время подтверждения устройства.");
    }

    public async Task<string> RegisterAsync(string deviceId, string displayName, CancellationToken cancellationToken)
    {
        string query = QueryString(new Dictionary<string, string>
        {
            ["device_id"] = deviceId,
            ["display_name"] = displayName,
            ["platform"] = "windows-cfapi",
            ["status"] = "online",
            ["metadata_json"] = JsonSerializer.Serialize(new
            {
                mode = "files-on-demand",
                version = AppDefaults.Version,
                update_channel = "stable",
            }),
        });
        using HttpResponseMessage response = await _http.PostAsync("api/cloud-drive/sync/clients?" + query, null, cancellationToken);
        response.EnsureSuccessStatusCode();
        SyncClientResponse client = await response.Content.ReadFromJsonAsync<SyncClientResponse>(JsonOptions, cancellationToken)
            ?? throw new InvalidOperationException("Сервер не вернул идентификатор sync client.");
        return client.Id;
    }

    public async Task HeartbeatAsync(string clientId, CancellationToken cancellationToken)
    {
        string query = QueryString(new Dictionary<string, string>
        {
            ["client_id"] = clientId,
            ["status"] = "online",
        });
        using HttpResponseMessage response = await _http.PostAsync("api/cloud-drive/sync/heartbeat?" + query, null, cancellationToken);
        response.EnsureSuccessStatusCode();
    }

    public async Task<UpdateManifest> GetUpdateManifestAsync(CancellationToken cancellationToken)
    {
        using HttpResponseMessage response = await _http.GetAsync("api/sync-client/version", cancellationToken);
        response.EnsureSuccessStatusCode();
        return await response.Content.ReadFromJsonAsync<UpdateManifest>(JsonOptions, cancellationToken)
            ?? throw new InvalidOperationException("Сервер вернул пустой манифест обновления.");
    }

    public async Task DownloadUpdateAsync(
        string downloadUrl,
        string destination,
        long expectedSize,
        CancellationToken cancellationToken)
    {
        Uri baseUri = _http.BaseAddress
            ?? throw new InvalidOperationException("Не задан адрес сервера обновлений.");
        Uri uri = new(baseUri, downloadUrl);
        if (!string.Equals(uri.Scheme, baseUri.Scheme, StringComparison.OrdinalIgnoreCase) ||
            !string.Equals(uri.Host, baseUri.Host, StringComparison.OrdinalIgnoreCase) ||
            uri.Port != baseUri.Port)
        {
            throw new InvalidDataException("Сервер обновлений вернул URL другого источника.");
        }

        using HttpResponseMessage response = await _http.GetAsync(
            uri,
            HttpCompletionOption.ResponseHeadersRead,
            cancellationToken);
        response.EnsureSuccessStatusCode();
        Directory.CreateDirectory(Path.GetDirectoryName(destination)!);
        await using (Stream source = await response.Content.ReadAsStreamAsync(cancellationToken))
        await using (FileStream target = new(
                         destination,
                         FileMode.Create,
                         FileAccess.Write,
                         FileShare.None,
                         bufferSize: 1024 * 1024,
                         useAsync: true))
        {
            await source.CopyToAsync(target, cancellationToken);
        }

        long actualSize = new FileInfo(destination).Length;
        if (expectedSize <= 0 || actualSize != expectedSize)
        {
            throw new InvalidDataException(
                $"Размер обновления не совпал: ожидалось {expectedSize}, получено {actualSize}.");
        }
    }

    public async Task<VisibleSnapshot> GetVisibleSnapshotAsync(CancellationToken cancellationToken)
    {
        Dictionary<string, CloudNode> nodes = new(StringComparer.OrdinalIgnoreCase);
        string cursor = "";
        string aclRevision = "";
        for (int pageNumber = 0; pageNumber < 10_000; pageNumber++)
        {
            string path = "api/cloud-drive/changes?limit=5000&since=" + Uri.EscapeDataString(cursor);
            using HttpResponseMessage response = await _http.GetAsync(path, cancellationToken);
            response.EnsureSuccessStatusCode();
            ChangePage page = await response.Content.ReadFromJsonAsync<ChangePage>(JsonOptions, cancellationToken)
                ?? throw new InvalidOperationException("Сервер вернул пустую страницу change feed.");
            if (aclRevision.Length == 0)
            {
                aclRevision = page.AclRevision;
            }
            else if (!string.Equals(aclRevision, page.AclRevision, StringComparison.Ordinal))
            {
                throw new InvalidOperationException("ACL изменился во время построения snapshot; операция будет повторена.");
            }
            foreach (CloudNode node in page.Changes)
            {
                if (!CloudPath.TryNormalize(node.Path, out string normalized) || normalized.Length == 0)
                {
                    Console.Error.WriteLine($"Пропущен несовместимый с Windows путь: {node.Path}");
                    continue;
                }

                node.Path = normalized;
                if (node.DeletedAt.Length == 0)
                {
                    nodes[normalized] = node;
                }
                else
                {
                    nodes.Remove(normalized);
                }
            }

            string next = page.NextCursor ?? "";
            if (string.Equals(next, cursor, StringComparison.Ordinal))
            {
                return new VisibleSnapshot(nodes.Values.ToList(), cursor, aclRevision);
            }

            cursor = next;
        }

        throw new InvalidOperationException("Change feed не завершился после 10000 страниц.");
    }

    public async Task<ChangePage> GetChangesAsync(string cursor, CancellationToken cancellationToken)
    {
        string path = "api/cloud-drive/changes?limit=5000&since=" + Uri.EscapeDataString(cursor);
        using HttpResponseMessage response = await _http.GetAsync(path, cancellationToken);
        response.EnsureSuccessStatusCode();
        return await response.Content.ReadFromJsonAsync<ChangePage>(JsonOptions, cancellationToken)
            ?? throw new InvalidOperationException("Сервер вернул пустую страницу change feed.");
    }

    public async Task<byte[]> DownloadRangeAsync(
        string cloudPath,
        long offset,
        int length,
        CancellationToken cancellationToken)
    {
        string path = "api/cloud-drive/download?path=" + Uri.EscapeDataString(cloudPath);
        using HttpRequestMessage request = new(HttpMethod.Get, path);
        request.Headers.Range = new RangeHeaderValue(offset, offset + length - 1);
        using HttpResponseMessage response = await _http.SendAsync(
            request,
            HttpCompletionOption.ResponseHeadersRead,
            cancellationToken);
        response.EnsureSuccessStatusCode();
        await using Stream stream = await response.Content.ReadAsStreamAsync(cancellationToken);

        if (response.StatusCode == HttpStatusCode.OK && offset > 0)
        {
            await SkipExactlyAsync(stream, offset, cancellationToken);
        }

        byte[] result = new byte[length];
        int read = 0;
        while (read < result.Length)
        {
            int count = await stream.ReadAsync(result.AsMemory(read), cancellationToken);
            if (count == 0)
            {
                break;
            }

            read += count;
        }

        return read == result.Length ? result : result[..read];
    }

    public void Dispose() => _http.Dispose();

    private static async Task SkipExactlyAsync(Stream stream, long bytes, CancellationToken cancellationToken)
    {
        byte[] buffer = new byte[64 * 1024];
        long remaining = bytes;
        while (remaining > 0)
        {
            int read = await stream.ReadAsync(buffer.AsMemory(0, (int)Math.Min(buffer.Length, remaining)), cancellationToken);
            if (read == 0)
            {
                throw new EndOfStreamException("Сервер вернул меньше данных, чем ожидалось.");
            }

            remaining -= read;
        }
    }

    private static string QueryString(IReadOnlyDictionary<string, string> values) => string.Join(
        "&",
        values.Select(pair => $"{Uri.EscapeDataString(pair.Key)}={Uri.EscapeDataString(pair.Value)}"));
}

internal static class CloudPath
{
    public static bool TryNormalize(string path, out string normalized)
    {
        try
        {
            normalized = Normalize(path);
            return true;
        }
        catch (InvalidDataException)
        {
            normalized = "";
            return false;
        }
    }

    public static string Normalize(string path)
    {
        string normalized = (path ?? "").Replace('\\', '/').Trim('/');
        if (normalized.Length == 0)
        {
            return "";
        }

        string[] segments = normalized.Split('/', StringSplitOptions.RemoveEmptyEntries);
        if (segments.Any(segment => segment is "." or ".." || segment.IndexOfAny(Path.GetInvalidFileNameChars()) >= 0))
        {
            throw new InvalidDataException($"Недопустимый облачный путь: {path}");
        }

        return string.Join('/', segments);
    }

    public static string Parent(string path)
    {
        int separator = path.LastIndexOf('/');
        return separator < 0 ? "" : path[..separator];
    }

    public static int Depth(string path) => path.Count(character => character == '/') + 1;

    public static string LocalPath(string root, string cloudPath)
    {
        string normalized = Normalize(cloudPath);
        string candidate = Path.GetFullPath(Path.Combine(root, normalized.Replace('/', Path.DirectorySeparatorChar)));
        string fullRoot = Path.GetFullPath(root).TrimEnd(Path.DirectorySeparatorChar) + Path.DirectorySeparatorChar;
        if (!candidate.StartsWith(fullRoot, StringComparison.OrdinalIgnoreCase))
        {
            throw new InvalidDataException($"Путь выходит за пределы sync root: {cloudPath}");
        }

        return candidate;
    }
}
