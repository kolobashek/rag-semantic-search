using System.ComponentModel;
using System.Diagnostics;
using System.Runtime.InteropServices;
using System.Security.Principal;
using System.Text.Json;
using System.Xml.Linq;
using Windows.Win32;
using Windows.Win32.Foundation;
using Windows.Win32.Security;

namespace RagCloudFiles;

internal static class ProviderRecovery
{
    private const int FileNotFound = unchecked((int)0x80070002);
    internal static string TaskName => "RAGCloudFiles-" + CurrentSid;
    private static string CurrentSid => WindowsIdentity.GetCurrent().User?.Value
        ?? throw new InvalidOperationException("Windows user SID is unavailable.");
    private static unsafe string CurrentLogonId
    {
        get
        {
            using WindowsIdentity identity = WindowsIdentity.GetCurrent();
            TOKEN_STATISTICS statistics;
            uint length;
            if (!PInvoke.GetTokenInformation(new HANDLE(identity.AccessToken.DangerousGetHandle()),
                    TOKEN_INFORMATION_CLASS.TokenStatistics, &statistics, (uint)sizeof(TOKEN_STATISTICS), &length))
                throw new Win32Exception(Marshal.GetLastWin32Error());
            return $"{statistics.AuthenticationId.HighPart:X8}:{statistics.AuthenticationId.LowPart:X8}";
        }
    }
    private sealed record Context(string TaskName, string StatePath, string Executable,
        string ProviderArguments, string CheckArguments);
    private static Context Installed => new(TaskName,
        Path.Combine(WindowsBootstrap.ConfigDirectory, "recovery.json"),
        WindowsBootstrap.InstalledExecutable, "--installed --managed-provider", "--check-recovery");

    private sealed class State
    {
        public bool Requested { get; set; }
        public string LogonId { get; set; } = "";
        public int ProcessId { get; set; }
        public long StartTicks { get; set; }
        public int Restarts { get; set; }
        public DateTimeOffset RetryAfter { get; set; }
    }

    public static bool TryStart()
    {
        try { Start(Installed); return true; }
        catch (Exception error)
        {
            AppLog.Error("Automatic crash recovery is unavailable; starting the client directly.", error);
            return false;
        }
    }

    public static void Check() => BestEffort(() => WithState(Installed,
        state => Check(Installed, state, automatic: true)));
    public static void SuppressRestarts() => BestEffort(() => Suppress(Installed, stop: false));
    public static void Stop() => BestEffort(() => Suppress(Installed, stop: true));
    public static void Remove() => BestEffort(() =>
    {
        Delete(TaskName);
        File.Delete(Installed.StatePath);
    });

    private static void Start(Context context)
    {
        Register(context);
        WithState(context, state =>
        {
            state.Requested = true;
            state.LogonId = CurrentLogonId;
            state.Restarts = 0;
            state.RetryAfter = DateTimeOffset.MinValue;
            Check(context, state, automatic: false);
        });
    }

    private static void Check(Context context, State state, bool automatic)
    {
        if (!state.Requested) return;
        if (!string.Equals(state.LogonId, CurrentLogonId, StringComparison.Ordinal))
        {
            state.Requested = false;
            Disable(context.TaskName, stop: false);
            return;
        }
        if (IsRunning(context, state))
        {
            if (DateTime.UtcNow - new DateTime(state.StartTicks, DateTimeKind.Utc) >= TimeSpan.FromMinutes(10))
                state.Restarts = 0;
            return;
        }
        if (DateTimeOffset.UtcNow < state.RetryAfter) return;
        if (automatic && state.Restarts >= 3)
        {
            state.Requested = false;
            Disable(context.TaskName, stop: false);
            AppLog.Error("Crash recovery stopped after three attempts; open RAG Cloud Files from Start to retry.");
            return;
        }
        using Process started = Process.Start(new ProcessStartInfo(context.Executable, context.ProviderArguments)
        {
            UseShellExecute = false,
            CreateNoWindow = true,
            WorkingDirectory = Path.GetDirectoryName(context.Executable)!,
        }) ?? throw new InvalidOperationException("Could not start RAG Cloud Files.");
        state.ProcessId = started.Id;
        state.StartTicks = started.StartTime.ToUniversalTime().Ticks;
        state.RetryAfter = DateTimeOffset.UtcNow.AddSeconds(30);
        if (automatic)
        {
            state.Restarts++;
            AppLog.Info($"Recovered cloud client PID {state.ProcessId}; attempt {state.Restarts}/3.");
        }
    }

    private static bool IsRunning(Context context, State state)
    {
        if (state.ProcessId <= 0) return false;
        try
        {
            using Process running = Process.GetProcessById(state.ProcessId);
            return !running.HasExited && running.StartTime.ToUniversalTime().Ticks == state.StartTicks
                && string.Equals(running.MainModule?.FileName, context.Executable, StringComparison.OrdinalIgnoreCase);
        }
        catch (Exception error) when (error is ArgumentException or InvalidOperationException or Win32Exception)
        {
            return false;
        }
    }

    private static void Suppress(Context context, bool stop)
    {
        WithState(context, state => state.Requested = false);
        Disable(context.TaskName, stop);
    }

    private static void WithState(Context context, Action<State> action)
    {
        using Mutex guard = new(false, @"Local\" + context.TaskName + ".Recovery");
        bool locked;
        try { locked = guard.WaitOne(TimeSpan.FromSeconds(5)); }
        catch (AbandonedMutexException) { locked = true; }
        if (!locked) throw new TimeoutException("Client recovery state is busy.");
        try
        {
            Directory.CreateDirectory(Path.GetDirectoryName(context.StatePath)!);
            State state = File.Exists(context.StatePath)
                ? JsonSerializer.Deserialize<State>(File.ReadAllText(context.StatePath))!
                : new State();
            if (state is null) throw new InvalidDataException("Invalid client recovery state.");
            action(state);
            string temporary = context.StatePath + ".tmp";
            File.WriteAllText(temporary, JsonSerializer.Serialize(state));
            File.Move(temporary, context.StatePath, overwrite: true);
        }
        finally { guard.ReleaseMutex(); }
    }

    private static void BestEffort(Action action)
    {
        try { action(); }
        catch (Exception error) { AppLog.Error("Could not update client crash recovery.", error); }
    }

    private static string Definition(Context context)
    {
        XNamespace ns = "http://schemas.microsoft.com/windows/2004/02/mit/task";
        XElement Element(string name, object value) => new(ns + name, value);
        return new XElement(ns + "Task", new XAttribute("version", "1.2"),
            Element("RegistrationInfo", Element("Description", "RAG Cloud Files crash recovery")),
            Element("Triggers", Element("TimeTrigger", new object[]
            {
                Element("Repetition", new object[] { Element("Interval", "PT1M"), Element("StopAtDurationEnd", false) }),
                Element("StartBoundary", DateTime.UtcNow.AddMinutes(1).ToString("yyyy-MM-ddTHH:mm:ss'Z'")),
                Element("Enabled", true),
            })),
            Element("Principals", new XElement(ns + "Principal", new XAttribute("id", "User"),
                Element("UserId", CurrentSid), Element("LogonType", "InteractiveToken"),
                Element("RunLevel", "LeastPrivilege"))),
            Element("Settings", new object[]
            {
                Element("MultipleInstancesPolicy", "IgnoreNew"),
                Element("DisallowStartIfOnBatteries", false), Element("StopIfGoingOnBatteries", false),
                Element("AllowHardTerminate", true), Element("AllowStartOnDemand", true),
                Element("Enabled", true), Element("ExecutionTimeLimit", "PT0S"),
            }),
            new XElement(ns + "Actions", new XAttribute("Context", "User"),
                Element("Exec", new object[] { Element("Command", context.Executable),
                    Element("Arguments", context.CheckArguments),
                    Element("WorkingDirectory", Path.GetDirectoryName(context.Executable)!) }))
        ).ToString(SaveOptions.DisableFormatting);
    }

    private static void Register(Context context) => WithFolder(folder =>
        Release(folder.RegisterTask(context.TaskName, Definition(context), 6, null, null, 3, null)));

    internal static void Disable(string name, bool stop) => WithFolder(folder =>
    {
        object task;
        try { task = folder.GetTask(name); }
        catch (Exception error) when (error.HResult == FileNotFound) { return; }
        try
        {
            dynamic registered = task;
            registered.Enabled = false;
            if (stop) registered.Stop(0);
        }
        finally { Release(task); }
    });

    internal static void Delete(string name) => WithFolder(folder =>
    {
        try { folder.DeleteTask(name, 0); }
        catch (Exception error) when (error.HResult == FileNotFound) { }
    });

    private static void WithFolder(Action<dynamic> action)
    {
        Type type = Type.GetTypeFromProgID("Schedule.Service")
            ?? throw new InvalidOperationException("Windows Task Scheduler is unavailable.");
        dynamic service = Activator.CreateInstance(type)!;
        object? folder = null;
        try { service.Connect(); folder = service.GetFolder(@"\"); action(folder); }
        finally { Release(folder); Release(service); }
    }

    private static void Release(object? value)
    {
        if (value is not null && Marshal.IsComObject(value)) Marshal.FinalReleaseComObject(value);
    }

    private static Context Fixture(string name, string path, string mode) => new(name, path + ".state.json",
        Environment.ProcessPath!,
        $"--recovery-fixture \"{path}\" --fixture-mode {mode} --fixture-task {name}",
        $"--check-recovery-fixture \"{path}\" --fixture-mode {mode} --fixture-task {name}");
    internal static void StartFixture(string name, string path, string mode) => Start(Fixture(name, path, mode));
    internal static void CheckFixture(string name, string path, string mode) =>
        WithState(Fixture(name, path, mode), state => Check(Fixture(name, path, mode), state, automatic: true));
    internal static void StopFixture(string name, string path, string mode) => Suppress(Fixture(name, path, mode), stop: true);
}
