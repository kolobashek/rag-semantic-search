namespace RagCloudFiles;

internal static class Program
{
    private const string SingleInstanceName = @"Local\RAGCloudFiles.Provider";

    [STAThread]
    private static async Task<int> Main(string[] args)
    {
        ApplicationConfiguration.Initialize();

        if (!OperatingSystem.IsWindowsVersionAtLeast(10, 0, 16299))
        {
            WindowsBootstrap.ShowError("RAG Cloud Files требует Windows 10 1709 или новее.");
            return 2;
        }

        Dictionary<string, string> options = ParseOptions(args);
        if (options.ContainsKey("apply-update"))
        {
            int waitProcessId = int.Parse(options.GetValueOrDefault("wait-pid", "0"));
            return await WindowsBootstrap.ApplyStagedUpdateAsync(
                waitProcessId,
                options.GetValueOrDefault("sha256", ""));
        }
        if (options.ContainsKey("apply-uninstall"))
        {
            int waitProcessId = int.Parse(options.GetValueOrDefault("wait-pid", "0"));
            return await WindowsBootstrap.ApplyUninstallAsync(
                waitProcessId,
                options.GetValueOrDefault("root", ""));
        }

        ConfigStore store = new(options.GetValueOrDefault("config"));
        if (options.ContainsKey("uninstall"))
        {
            ProviderConfig uninstallConfig = store.LoadConfig();
            WindowsBootstrap.LaunchUninstall(uninstallConfig.RootPath);
            return 0;
        }
        if (ShellCommandHandler.IsRequested(options))
        {
            try
            {
                return await ShellCommandHandler.RunAsync(options, store, CancellationToken.None);
            }
            catch (Exception exception)
            {
                WindowsBootstrap.ShowError("Не удалось выполнить команду RAG Cloud Files.", exception);
                return 1;
            }
        }

        if (WindowsBootstrap.IsInteractiveInstall(args))
        {
            try
            {
                WindowsBootstrap.InstallAndLaunch();
                return 0;
            }
            catch (Exception exception)
            {
                WindowsBootstrap.ShowError("Не удалось установить RAG Cloud Files.", exception);
                return 1;
            }
        }

        if (WindowsBootstrap.IsRunningInstalled)
        {
            WindowsBootstrap.CleanupStagedUpdates();
        }
        ProviderConfig config = store.LoadConfig();
        config.Server = options.GetValueOrDefault("server", config.Server).TrimEnd('/');
        config.Token = options.GetValueOrDefault("token", config.Token);
        config.RootPath = options.GetValueOrDefault("root", config.RootPath);
        config.KeepAllOffline = options.ContainsKey("keep-all-offline") || config.KeepAllOffline;
        if (WindowsBootstrap.IsRunningInstalled)
        {
            WindowsBootstrap.SavePreferences(config);
        }
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
            WindowsBootstrap.ShowError("Не задан адрес сервера Cloud Drive.");
            return 2;
        }

        using EventWaitHandle singleInstance = new(
            initialState: false,
            EventResetMode.ManualReset,
            SingleInstanceName,
            out bool createdNew);
        if (!createdNew)
        {
            WindowsBootstrap.OpenRoot(config.RootPath);
            return 0;
        }

        using CancellationTokenSource shutdown = new();
        ClientStatusModel status = new();
        CloudFilesProvider? activeProvider = null;
        object runtimeSync = new();
        bool restartRequested = false;
        bool reauthorizeRequested = false;
        string unregisterRoot = "";
        bool showTray = !once && runSeconds <= 0;
        TrayApplicationContext? tray = null;
        Thread? trayThread = null;
        using ManualResetEventSlim trayReady = new(initialState: false);

        void RequestStop(bool restart, string oldRoot = "")
        {
            lock (runtimeSync)
            {
                restartRequested |= restart;
                if (oldRoot.Length > 0)
                {
                    unregisterRoot = oldRoot;
                }
            }
            shutdown.Cancel();
            tray?.Stop();
        }

        void RequestAuthorization()
        {
            lock (runtimeSync)
            {
                reauthorizeRequested = true;
            }
            AppLog.Info("Device authorization requested; stopping synchronization before restart.");
            RequestStop(restart: true);
        }

        if (showTray)
        {
            trayThread = new Thread(() =>
            {
                tray = new TrayApplicationContext(
                    status,
                    config,
                    provider: () =>
                    {
                        lock (runtimeSync)
                        {
                            return activeProvider;
                        }
                    },
                    saveSettings: async selection =>
                    {
                        string oldRoot = config.RootPath;
                        config.RootPath = selection.RootPath;
                        config.KeepAllOffline = selection.KeepAllOffline;
                        config.OfflinePaths = new HashSet<string>(
                            selection.OfflinePaths,
                            StringComparer.OrdinalIgnoreCase);
                        config.MaxCacheSizeGb = CachePolicy.NormalizeMaxCacheSizeGb(
                            selection.MaxCacheSizeGb);
                        config.MinimumFreeSpaceGb = CachePolicy.NormalizeMinimumFreeSpaceGb(
                            selection.MinimumFreeSpaceGb);
                        config.StartWithWindows = selection.StartWithWindows;
                        bool driveSettingsChanged = config.MountAsDrive != selection.MountAsDrive
                            || !config.DriveLetter.Equals(
                                selection.DriveLetter,
                                StringComparison.OrdinalIgnoreCase);
                        config.MountAsDrive = selection.MountAsDrive;
                        config.DriveLetter = selection.DriveLetter;
                        store.SaveConfig(config);
                        WindowsBootstrap.SavePreferences(config);

                        if (!string.Equals(
                                oldRoot,
                                config.RootPath,
                                StringComparison.OrdinalIgnoreCase)
                            || driveSettingsChanged)
                        {
                            VirtualDriveManager.RemoveForRoot(oldRoot);
                            RequestStop(restart: true, oldRoot);
                            return;
                        }

                        CloudFilesProvider? provider;
                        lock (runtimeSync)
                        {
                            provider = activeProvider;
                        }
                        if (provider is not null)
                        {
                            await provider.ApplyOfflinePolicyAsync(shutdown.Token);
                            await provider.ApplyCachePolicyAsync(shutdown.Token);
                        }
                    },
                    requestRestart: () => RequestStop(restart: true),
                    requestAuthorization: RequestAuthorization,
                    requestExit: () => RequestStop(restart: false),
                    applicationToken: shutdown.Token);
                trayReady.Set();
                Application.Run(tray);
            })
            {
                IsBackground = true,
                Name = "rag-cloud-files-tray",
            };
            trayThread.SetApartmentState(ApartmentState.STA);
            trayThread.Start();
            trayReady.Wait();
        }

        Console.CancelKeyPress += (_, eventArgs) =>
        {
            eventArgs.Cancel = true;
            RequestStop(restart: false);
        };

        int exitCode = 0;
        try
        {
            AppLog.Info($"Starting provider {AppDefaults.Version} for {config.Server}.");
            bool firstAuthorization = config.Token.Length == 0;
            using CloudDriveApi api = new(config.Server, config.Token);
            string registeredClientId = await SessionAuthorization.RegisterAsync(
                config.Token,
                token =>
                {
                    api.SetCredentials(config.Server, token);
                    return api.RegisterAsync(config.DeviceId, Environment.MachineName, shutdown.Token);
                },
                async () =>
            {
                firstAuthorization = true;
                status.SetState(ClientRunState.Authorizing, "Ожидание подтверждения входа…");
                DeviceTokenResponse auth = await CloudDriveApi.AuthorizeDeviceAsync(config.Server, shutdown.Token);
                config.Token = auth.Token;
                if (auth.Server.Length > 0)
                {
                    config.Server = auth.Server.TrimEnd('/');
                }
                return config.Token;
            });
            SessionAuthorization.ValidateClientIdentity(config.ClientId, registeredClientId);
            config.ClientId = registeredClientId;
            store.SaveConfig(config);
            api.SessionExpired += RequestAuthorization;

            await using CloudFilesProvider provider = new(config, store, api, status);
            lock (runtimeSync)
            {
                activeProvider = provider;
            }
            await provider.StartAsync(shutdown.Token);
            AppLog.Info($"Provider ready at {Path.GetFullPath(config.RootPath)}.");
            Console.WriteLine($"RAG Cloud Drive готов: {Path.GetFullPath(config.RootPath)}");
            if (firstAuthorization)
            {
                WindowsBootstrap.OpenRoot(config.RootPath);
            }
            if (once)
            {
                exitCode = 0;
            }
            else if (runSeconds > 0)
            {
                await Task.Delay(TimeSpan.FromSeconds(runSeconds), shutdown.Token);
                exitCode = 0;
            }
            else
            {
                ClientUpdater updater = new(api, status);
                Task updaterTask = updater.RunAutomaticAsync(
                    () => RequestStop(restart: false),
                    shutdown.Token);
                try
                {
                    await provider.RunAsync(shutdown.Token);
                }
                finally
                {
                    shutdown.Cancel();
                    try
                    {
                        await updaterTask;
                    }
                    catch (OperationCanceledException) when (shutdown.IsCancellationRequested)
                    {
                        // Normal application shutdown stops the update timer.
                    }
                }
            }
        }
        catch (OperationCanceledException) when (shutdown.IsCancellationRequested)
        {
            exitCode = 0;
        }
        catch (HttpRequestException) when (shutdown.IsCancellationRequested && reauthorizeRequested)
        {
            exitCode = 0;
        }
        catch (Exception exception)
        {
            lock (runtimeSync)
            {
                activeProvider = null;
            }
            status.SetState(ClientRunState.Error, "Клиент остановлен из-за ошибки", exception.Message);
            AppLog.Error("RAG Cloud Files остановлен из-за ошибки.", exception);
            exitCode = 1;
            if (showTray)
            {
                while (!shutdown.IsCancellationRequested)
                {
                    await Task.Delay(500);
                }
            }
            else
            {
                WindowsBootstrap.ShowError("RAG Cloud Files остановлен из-за ошибки.", exception);
            }
        }
        finally
        {
            lock (runtimeSync)
            {
                activeProvider = null;
            }
            status.SetState(ClientRunState.Stopped, "Клиент остановлен");
            tray?.Stop();
            if (trayThread is not null && trayThread.IsAlive)
            {
                trayThread.Join(TimeSpan.FromSeconds(10));
            }
        }

        bool restart;
        string oldRegisteredRoot;
        lock (runtimeSync)
        {
            restart = restartRequested;
            oldRegisteredRoot = unregisterRoot;
        }
        if (restart)
        {
            if (reauthorizeRequested)
            {
                // Provider disposal has completed. Preserve the root, cache and sync state.
                config.Token = "";
                store.SaveConfig(config);
            }
            if (oldRegisteredRoot.Length > 0)
            {
                try
                {
                    CloudFilesProvider.Unregister(oldRegisteredRoot);
                }
                catch (Exception exception)
                {
                    AppLog.Error($"Не удалось отменить регистрацию старого корня {oldRegisteredRoot}.", exception);
                }
            }
            // Release the single-instance handle before the new process checks it.
            singleInstance.Dispose();
            WindowsBootstrap.RestartInstalled();
        }
        return exitCode;
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
            if (name is "once"
                or "self-test"
                or "unregister"
                or "uninstall"
                or "installed"
                or "keep-all-offline"
                or "apply-update"
                or "apply-uninstall")
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
