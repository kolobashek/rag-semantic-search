namespace RagCloudFiles;

internal static class Program
{
    private static async Task<int> Main(string[] args)
    {
        if (!OperatingSystem.IsWindowsVersionAtLeast(10, 0, 16299))
        {
            Console.Error.WriteLine("RAG Cloud Files требует Windows 10 1709 или новее.");
            return 2;
        }

        Dictionary<string, string> options = ParseOptions(args);
        ConfigStore store = new(options.GetValueOrDefault("config"));
        ProviderConfig config = store.LoadConfig();
        config.Server = options.GetValueOrDefault("server", config.Server).TrimEnd('/');
        config.Token = options.GetValueOrDefault("token", config.Token);
        config.RootPath = options.GetValueOrDefault("root", config.RootPath);
        bool once = options.ContainsKey("once");
        int runSeconds = options.TryGetValue("run-seconds", out string? runSecondsValue)
            ? int.Parse(runSecondsValue)
            : 0;

        if (options.ContainsKey("self-test"))
        {
            SelfTest.Run();
            Console.WriteLine("Self-test: OK");
            return 0;
        }

        if (options.ContainsKey("unregister"))
        {
            CloudFilesProvider.Unregister(config.RootPath);
            Console.WriteLine($"Sync root удалён из регистрации: {Path.GetFullPath(config.RootPath)}");
            return 0;
        }

        if (config.Server.Length == 0)
        {
            Console.Error.WriteLine("Укажите адрес сервера: RagCloudFiles.exe --server https://catalog.example.org");
            return 2;
        }

        using CancellationTokenSource shutdown = new();
        Console.CancelKeyPress += (_, eventArgs) =>
        {
            eventArgs.Cancel = true;
            shutdown.Cancel();
        };

        try
        {
            if (config.Token.Length == 0)
            {
                DeviceTokenResponse auth = await CloudDriveApi.AuthorizeDeviceAsync(config.Server, shutdown.Token);
                config.Token = auth.Token;
                if (auth.Server.Length > 0)
                {
                    config.Server = auth.Server.TrimEnd('/');
                }
            }

            using CloudDriveApi api = new(config.Server, config.Token);
            config.ClientId = await api.RegisterAsync(config.DeviceId, Environment.MachineName, shutdown.Token);
            store.SaveConfig(config);

            await using CloudFilesProvider provider = new(config, store, api);
            await provider.StartAsync(shutdown.Token);
            Console.WriteLine($"RAG Cloud Drive готов: {Path.GetFullPath(config.RootPath)}");
            if (once)
            {
                return 0;
            }

            if (runSeconds > 0)
            {
                await Task.Delay(TimeSpan.FromSeconds(runSeconds), shutdown.Token);
                return 0;
            }

            await provider.RunAsync(shutdown.Token);
            return 0;
        }
        catch (OperationCanceledException) when (shutdown.IsCancellationRequested)
        {
            return 0;
        }
        catch (Exception exception)
        {
            Console.Error.WriteLine(exception);
            return 1;
        }
    }

    private static Dictionary<string, string> ParseOptions(string[] args)
    {
        Dictionary<string, string> result = new(StringComparer.OrdinalIgnoreCase);
        for (int index = 0; index < args.Length; index++)
        {
            string argument = args[index];
            if (!argument.StartsWith("--", StringComparison.Ordinal))
            {
                throw new ArgumentException($"Неизвестный аргумент: {argument}");
            }

            string name = argument[2..];
            if (name is "once" or "self-test" or "unregister")
            {
                result[name] = "true";
                continue;
            }

            if (++index >= args.Length)
            {
                throw new ArgumentException($"Для --{name} требуется значение.");
            }

            result[name] = args[index];
        }

        return result;
    }
}
