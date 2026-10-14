using System.Diagnostics;
using System.Text.Json;

namespace RagCloudFiles;

internal static class RecoverySmoke
{
    internal static async Task<int> RunChildAsync(string path, string mode, string taskName)
    {
        File.AppendAllText(path, Environment.ProcessId + "\n");
        if (File.ReadAllLines(path).Length > 1 || mode == "exit")
        {
            ProviderRecovery.StopFixture(taskName, path, mode);
            return 0;
        }
        if (mode == "crash") return 42;
        await Task.Delay(TimeSpan.FromMinutes(3));
        return 0;
    }

    internal static async Task RunAsync(string reportPath)
    {
        string directory = Path.Combine(Path.GetTempPath(), "rag-recovery-test-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(directory);
        string[] modes = ["crash", "kill", "exit", "maintenance"];
        Dictionary<string, string> tasks = modes.ToDictionary(mode => mode,
            mode => "RAGCloudFiles-Test-" + mode + "-" + Guid.NewGuid().ToString("N"));
        string CountPath(string mode) => Path.Combine(directory, mode + ".txt");
        int Count(string mode) => File.Exists(CountPath(mode)) ? File.ReadAllLines(CountPath(mode)).Length : 0;
        try
        {
            foreach (string mode in modes)
            {
                ProviderRecovery.StartFixture(tasks[mode], CountPath(mode), mode);
            }
            await WaitUntilAsync(() => modes.All(mode => Count(mode) >= 1), TimeSpan.FromSeconds(30));
            using (Process killed = Process.GetProcessById(int.Parse(File.ReadAllLines(CountPath("kill"))[0])))
                killed.Kill();
            ProviderRecovery.StopFixture(tasks["maintenance"], CountPath("maintenance"), "maintenance");
            using (Process stopped = Process.GetProcessById(int.Parse(File.ReadAllLines(CountPath("maintenance"))[0])))
                stopped.Kill();
            await WaitUntilAsync(() => Count("crash") >= 2 && Count("kill") >= 2, TimeSpan.FromSeconds(100));
            await Task.Delay(TimeSpan.FromSeconds(5));
            if (Count("exit") != 1 || Count("maintenance") != 1)
                throw new InvalidOperationException("Intentional exit or maintenance unexpectedly restarted.");
            if (Count("crash") != 2 || Count("kill") != 2)
                throw new InvalidOperationException("Unexpected recovery count.");
            string report = JsonSerializer.Serialize(new
            {
                ok = true,
                runs = modes.ToDictionary(mode => mode, Count),
                restart_interval_seconds = 60,
            });
            if (reportPath.Length > 0) File.WriteAllText(reportPath, report);
            Console.WriteLine(report);
        }
        catch (Exception error)
        {
            if (reportPath.Length > 0) File.WriteAllText(reportPath, JsonSerializer.Serialize(new
                { ok = false, runs = modes.ToDictionary(mode => mode, Count), error = error.ToString() }));
            throw;
        }
        finally
        {
            foreach (string name in tasks.Values)
            {
                try { ProviderRecovery.Disable(name, stop: true); }
                finally { ProviderRecovery.Delete(name); }
            }
            Directory.Delete(directory, recursive: true);
        }
    }

    private static async Task WaitUntilAsync(Func<bool> condition, TimeSpan timeout)
    {
        Stopwatch clock = Stopwatch.StartNew();
        while (!condition())
        {
            if (clock.Elapsed > timeout) throw new TimeoutException("Windows recovery task did not complete.");
            await Task.Delay(500);
        }
    }
}
