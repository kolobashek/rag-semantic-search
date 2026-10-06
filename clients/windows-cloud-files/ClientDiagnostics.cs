using System.Net.Http.Headers;
using System.Net.Http.Json;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using System.Text.RegularExpressions;

namespace RagCloudFiles;

internal sealed class ClientDiagnostics : IAsyncDisposable
{
    internal const int MaxLogBytes = 256 * 1024;
    private readonly ProviderConfig _config;
    private readonly HttpClient _http;
    private readonly string _logPath;
    private readonly ClientStatusModel? _status;
    private string _phase = "registration";
    private readonly CancellationTokenSource _stop = new();
    private Task? _task;
    private string _lastHash = "";
    private DateTimeOffset _lastUpload;

    public ClientDiagnostics(ProviderConfig config, HttpMessageHandler? handler = null, string? logPath = null,
        ClientStatusModel? status = null)
    {
        // Freeze the authorized identity while interactive login can change the live config.
        _config = new ProviderConfig { Server = config.Server, Token = config.Token, ClientId = config.ClientId };
        _status = status;
        _logPath = logPath ?? AppLog.FilePath;
        _http = new HttpClient(handler ?? new HttpClientHandler { AllowAutoRedirect = false })
        {
            Timeout = TimeSpan.FromSeconds(15),
        };
    }

    public void Start() => _task = Task.Run(RunAsync);

    public void SetPhase(string phase) => Volatile.Write(ref _phase, phase);

    private async Task RunAsync()
    {
        using PeriodicTimer timer = new(TimeSpan.FromSeconds(30));
        do
        {
            try
            {
                await SendOnceAsync(_stop.Token);
            }
            catch (OperationCanceledException) when (_stop.IsCancellationRequested)
            {
                return;
            }
            catch
            {
                // Diagnostics must not interrupt sync or log its own retry failures.
            }
        } while (await timer.WaitForNextTickAsync(_stop.Token));
    }

    private HttpRequestMessage Request(HttpMethod method, string suffix)
    {
        HttpRequestMessage request = new(method,
            _config.Server.TrimEnd('/') + "/api/cloud-drive/sync/diagnostics" + suffix);
        request.Headers.Authorization = new AuthenticationHeaderValue("Bearer", _config.Token);
        return request;
    }

    internal async Task SendOnceAsync(CancellationToken cancellationToken)
    {
        if (_config.ClientId.Length == 0 || _config.Token.Length == 0) return;
        string query = "?client_id=" + Uri.EscapeDataString(_config.ClientId);
        ClientStatusSnapshot? status = _status?.Current;
        string error = Redact(status?.LastError ?? "");
        using HttpRequestMessage poll = Request(HttpMethod.Post, "/heartbeat" + query);
        poll.Content = JsonContent.Create(new
        {
            app_version = AppDefaults.Version,
            phase = Volatile.Read(ref _phase),
            state = status?.State.ToString() ?? "Starting",
            last_error = error[..Math.Min(error.Length, 1000)],
        });
        using HttpResponseMessage response = await _http.SendAsync(poll, cancellationToken);
        response.EnsureSuccessStatusCode();
        using JsonDocument json = JsonDocument.Parse(await response.Content.ReadAsStringAsync(cancellationToken));
        string requestId = json.RootElement.GetProperty("request_id").GetString() ?? "";
        string log = ReadTail(_logPath);
        string hash = Convert.ToHexString(SHA256.HashData(Encoding.UTF8.GetBytes(log)));
        if (requestId.Length == 0 && hash == _lastHash
            && DateTimeOffset.UtcNow - _lastUpload < TimeSpan.FromHours(1))
        {
            return;
        }
        using HttpRequestMessage upload = Request(HttpMethod.Post, query);
        upload.Content = JsonContent.Create(new
        {
            request_id = requestId,
            app_version = AppDefaults.Version,
            log_text = log,
        });
        using HttpResponseMessage submitted = await _http.SendAsync(upload, cancellationToken);
        submitted.EnsureSuccessStatusCode();
        _lastHash = hash;
        _lastUpload = DateTimeOffset.UtcNow;
    }

    internal static string ReadTail(string path)
    {
        if (!File.Exists(path))
        {
            return "";
        }
        using FileStream stream = new(path, FileMode.Open, FileAccess.Read,
            FileShare.ReadWrite | FileShare.Delete);
        long offset = Math.Max(0, stream.Length - MaxLogBytes);
        stream.Seek(offset, SeekOrigin.Begin);
        byte[] bytes = new byte[Math.Min(stream.Length - offset, MaxLogBytes)];
        int read = 0;
        while (read < bytes.Length)
        {
            int count = stream.Read(bytes, read, bytes.Length - read);
            if (count == 0) break;
            read += count;
        }
        string text = Encoding.UTF8.GetString(bytes, 0, read);
        if (offset > 0)
        {
            int newline = text.IndexOf('\n');
            text = newline >= 0 ? text[(newline + 1)..] : "[Long log line omitted]";
        }
        return Redact(text).TrimStart('\uFEFF');
    }

    internal static string Redact(string text)
    {
        text = Regex.Replace(text, @"(?i)\bbearer\s+[A-Za-z0-9._\-]+", "Bearer <redacted>");
        text = Regex.Replace(text,
            "(?i)\\b(token|(?:access|refresh|id)[_-]?token|password|passwd|secret|api[_-]?key|access[_-]?key)\\b[\"']?\\s*[=:]\\s*(?:\"[^\"]*\"|'[^']*'|[^\\s,;&}\"']+)",
            "<redacted>");
        text = Regex.Replace(text, @"(?i)([?&](?:device_code|code|x-amz-[a-z-]+)=)[^&\s]+", "$1<redacted>");
        text = Regex.Replace(text, @"(?i)(\bcode\s+)[A-Z0-9]{4}-[A-Z0-9]{4}", "$1<redacted>");
        text = Regex.Replace(text, @"://[^/\s:@]+:[^/\s@]+@", "://<redacted>@");
        byte[] encoded = Encoding.UTF8.GetBytes(text);
        return encoded.Length <= MaxLogBytes ? text : Encoding.UTF8.GetString(encoded.AsSpan(0, MaxLogBytes - 4));
    }

    public async ValueTask DisposeAsync()
    {
        _stop.Cancel();
        if (_task is not null)
        {
            try { await _task; }
            catch (OperationCanceledException) { }
        }
        // Capture terminal errors too, but never hold shutdown indefinitely when offline.
        using CancellationTokenSource finalSend = new(TimeSpan.FromSeconds(5));
        try { await SendOnceAsync(finalSend.Token); }
        catch { }
        _http.Dispose();
        _stop.Dispose();
    }
}
