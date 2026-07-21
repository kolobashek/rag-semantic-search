using System.Text.Json.Serialization;

namespace RagCloudFiles;

internal sealed class ProviderConfig
{
    [JsonPropertyName("server")]
    public string Server { get; set; } = "";

    [JsonIgnore]
    public string Token { get; set; } = "";

    [JsonPropertyName("protected_token")]
    public string ProtectedToken { get; set; } = "";

    [JsonPropertyName("root_path")]
    public string RootPath { get; set; } = Path.Combine(
        Environment.GetFolderPath(Environment.SpecialFolder.UserProfile),
        "RAG Cloud Drive");

    [JsonPropertyName("device_id")]
    public string DeviceId { get; set; } = Guid.NewGuid().ToString("N");

    [JsonPropertyName("client_id")]
    public string ClientId { get; set; } = "";

    [JsonPropertyName("poll_seconds")]
    public int PollSeconds { get; set; } = 60;
}

internal sealed class DeviceCodeResponse
{
    [JsonPropertyName("device_code")]
    public string DeviceCode { get; set; } = "";

    [JsonPropertyName("user_code")]
    public string UserCode { get; set; } = "";

    [JsonPropertyName("verification_uri")]
    public string VerificationUri { get; set; } = "";

    [JsonPropertyName("verification_uri_complete")]
    public string VerificationUriComplete { get; set; } = "";

    [JsonPropertyName("expires_in")]
    public int ExpiresIn { get; set; } = 300;

    [JsonPropertyName("interval")]
    public int Interval { get; set; } = 5;
}

internal sealed class DeviceTokenResponse
{
    [JsonPropertyName("token")]
    public string Token { get; set; } = "";

    [JsonPropertyName("server")]
    public string Server { get; set; } = "";
}

internal sealed class SyncClientResponse
{
    [JsonPropertyName("id")]
    public string Id { get; set; } = "";
}

internal sealed class ChangePage
{
    [JsonPropertyName("next_cursor")]
    public string NextCursor { get; set; } = "";

    [JsonPropertyName("changes")]
    public List<CloudNode> Changes { get; set; } = [];

    [JsonPropertyName("acl_revision")]
    public string AclRevision { get; set; } = "";
}

internal sealed record VisibleSnapshot(IReadOnlyList<CloudNode> Nodes, string Cursor, string AclRevision);

internal sealed class CloudNode
{
    [JsonPropertyName("node_type")]
    public string NodeType { get; set; } = "";

    [JsonPropertyName("id")]
    public string Id { get; set; } = "";

    [JsonPropertyName("path")]
    public string Path { get; set; } = "";

    [JsonPropertyName("name")]
    public string Name { get; set; } = "";

    [JsonPropertyName("created_at")]
    public string CreatedAt { get; set; } = "";

    [JsonPropertyName("updated_at")]
    public string UpdatedAt { get; set; } = "";

    [JsonPropertyName("deleted_at")]
    public string DeletedAt { get; set; } = "";

    [JsonPropertyName("current_version_id")]
    public string CurrentVersionId { get; set; } = "";

    [JsonPropertyName("mime_type")]
    public string MimeType { get; set; } = "";

    [JsonPropertyName("size_bytes")]
    public long SizeBytes { get; set; }

    [JsonPropertyName("checksum")]
    public string Checksum { get; set; } = "";

    [JsonIgnore]
    public bool IsFolder => string.Equals(NodeType, "folder", StringComparison.OrdinalIgnoreCase);
}

internal sealed class ProviderState
{
    [JsonPropertyName("managed_paths")]
    public HashSet<string> ManagedPaths { get; set; } = new(StringComparer.OrdinalIgnoreCase);

    [JsonPropertyName("managed_versions")]
    public Dictionary<string, string> ManagedVersions { get; set; } = new(StringComparer.OrdinalIgnoreCase);
}
