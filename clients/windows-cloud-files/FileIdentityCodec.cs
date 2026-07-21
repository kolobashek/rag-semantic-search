using System.Text;

namespace RagCloudFiles;

internal static class FileIdentityCodec
{
    private const string Prefix = "ragcf1\n";

    public static byte[] Encode(string cloudPath)
    {
        byte[] value = Encoding.UTF8.GetBytes(Prefix + CloudPath.Normalize(cloudPath));
        if (value.Length > 4096)
        {
            throw new InvalidDataException("Путь не помещается в CfAPI FileIdentity.");
        }

        return value;
    }

    public static string Decode(ReadOnlySpan<byte> identity)
    {
        string value = Encoding.UTF8.GetString(identity);
        if (!value.StartsWith(Prefix, StringComparison.Ordinal))
        {
            throw new InvalidDataException("Неизвестная версия CfAPI FileIdentity.");
        }

        return CloudPath.Normalize(value[Prefix.Length..]);
    }
}
