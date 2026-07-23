#include <windows.h>
#include <shobjidl.h>
#include <shlwapi.h>

#include <atomic>
#include <new>
#include <string>
#include <vector>

#pragma comment(lib, "ole32.lib")
#pragma comment(lib, "shell32.lib")
#pragma comment(lib, "shlwapi.lib")

namespace
{
const CLSID CLSID_RagCloudCommand = {
    0xb732c5db, 0xb14f, 0x4f22, {0xa7, 0x29, 0x1d, 0xa4, 0xe4, 0x30, 0xe1, 0xdd}};

std::atomic<long> g_objectCount = 0;

std::wstring ReadPreference(const wchar_t* name)
{
    wchar_t value[32768]{};
    DWORD bytes = sizeof(value);
    if (RegGetValueW(
            HKEY_CURRENT_USER,
            L"Software\\RAGCloudFiles",
            name,
            RRF_RT_REG_SZ,
            nullptr,
            value,
            &bytes) != ERROR_SUCCESS)
    {
        return {};
    }
    return value;
}

std::wstring SelectedPath(IShellItemArray* items)
{
    if (items == nullptr)
    {
        return {};
    }
    DWORD count = 0;
    if (FAILED(items->GetCount(&count)) || count != 1)
    {
        return {};
    }
    IShellItem* item = nullptr;
    if (FAILED(items->GetItemAt(0, &item)))
    {
        return {};
    }
    PWSTR rawPath = nullptr;
    const HRESULT result = item->GetDisplayName(SIGDN_FILESYSPATH, &rawPath);
    item->Release();
    if (FAILED(result) || rawPath == nullptr)
    {
        return {};
    }
    std::wstring path(rawPath);
    CoTaskMemFree(rawPath);
    return path;
}

bool IsInsideSyncRoot(const std::wstring& path)
{
    std::wstring root = ReadPreference(L"RootPath");
    if (root.empty() || path.empty())
    {
        return false;
    }
    while (!root.empty() && (root.back() == L'\\' || root.back() == L'/'))
    {
        root.pop_back();
    }
    if (path.size() < root.size() ||
        CompareStringOrdinal(
            path.c_str(),
            static_cast<int>(root.size()),
            root.c_str(),
            static_cast<int>(root.size()),
            TRUE) != CSTR_EQUAL)
    {
        return false;
    }
    return path.size() == root.size() ||
        path[root.size()] == L'\\' ||
        path[root.size()] == L'/';
}

HRESULT CopyString(const std::wstring& value, PWSTR* output)
{
    if (output == nullptr)
    {
        return E_POINTER;
    }
    return SHStrDupW(value.c_str(), output);
}

HRESULT InvokeClient(const wchar_t* action, IShellItemArray* items)
{
    const std::wstring path = SelectedPath(items);
    if (!IsInsideSyncRoot(path))
    {
        return E_ACCESSDENIED;
    }
    std::wstring executable = ReadPreference(L"Executable");
    if (executable.empty())
    {
        wchar_t localAppData[MAX_PATH]{};
        if (ExpandEnvironmentStringsW(
                L"%LOCALAPPDATA%\\RAG Cloud Files\\RagCloudFiles.exe",
                localAppData,
                ARRAYSIZE(localAppData)) == 0)
        {
            return HRESULT_FROM_WIN32(GetLastError());
        }
        executable = localAppData;
    }
    std::wstring parameters =
        L"--shell-command " + std::wstring(action) + L" --shell-path \"" + path + L"\"";
    SHELLEXECUTEINFOW execute{};
    execute.cbSize = sizeof(execute);
    execute.fMask = SEE_MASK_FLAG_NO_UI | SEE_MASK_NOASYNC;
    execute.lpFile = executable.c_str();
    execute.lpParameters = parameters.c_str();
    execute.nShow = SW_HIDE;
    if (!ShellExecuteExW(&execute))
    {
        return HRESULT_FROM_WIN32(GetLastError());
    }
    return S_OK;
}

struct CommandSpec
{
    const wchar_t* title;
    const wchar_t* action;
};

const CommandSpec kCommands[] = {
    {L"Поделиться…", L"share"},
    {L"Скопировать ссылку", L"copy-link"},
    {L"Управление доступом…", L"manage-access"},
    {L"Всегда хранить на этом устройстве", L"keep-offline"},
};

class ExplorerCommand final : public IExplorerCommand
{
public:
    explicit ExplorerCommand(const CommandSpec* spec = nullptr) : spec_(spec)
    {
        ++g_objectCount;
    }

    ~ExplorerCommand()
    {
        --g_objectCount;
    }

    IFACEMETHODIMP QueryInterface(REFIID iid, void** object) override
    {
        if (object == nullptr)
        {
            return E_POINTER;
        }
        *object = nullptr;
        if (iid == IID_IUnknown || iid == IID_IExplorerCommand)
        {
            *object = static_cast<IExplorerCommand*>(this);
            AddRef();
            return S_OK;
        }
        return E_NOINTERFACE;
    }

    IFACEMETHODIMP_(ULONG) AddRef() override
    {
        return ++references_;
    }

    IFACEMETHODIMP_(ULONG) Release() override
    {
        const ULONG references = --references_;
        if (references == 0)
        {
            delete this;
        }
        return references;
    }

    IFACEMETHODIMP GetTitle(IShellItemArray*, PWSTR* title) override
    {
        return CopyString(spec_ == nullptr ? L"RAG Cloud" : spec_->title, title);
    }

    IFACEMETHODIMP GetIcon(IShellItemArray*, PWSTR* icon) override
    {
        std::wstring executable = ReadPreference(L"Executable");
        return executable.empty() ? E_NOTIMPL : CopyString(executable + L",0", icon);
    }

    IFACEMETHODIMP GetToolTip(IShellItemArray*, PWSTR* tooltip) override
    {
        if (tooltip != nullptr)
        {
            *tooltip = nullptr;
        }
        return E_NOTIMPL;
    }

    IFACEMETHODIMP GetCanonicalName(GUID* canonicalName) override
    {
        if (canonicalName == nullptr)
        {
            return E_POINTER;
        }
        *canonicalName = spec_ == nullptr ? CLSID_RagCloudCommand : GUID_NULL;
        return S_OK;
    }

    IFACEMETHODIMP GetState(
        IShellItemArray* items,
        BOOL,
        EXPCMDSTATE* state) override
    {
        if (state == nullptr)
        {
            return E_POINTER;
        }
        *state = IsInsideSyncRoot(SelectedPath(items)) ? ECS_ENABLED : ECS_HIDDEN;
        return S_OK;
    }

    IFACEMETHODIMP Invoke(IShellItemArray* items, IBindCtx*) override
    {
        return spec_ == nullptr ? E_NOTIMPL : InvokeClient(spec_->action, items);
    }

    IFACEMETHODIMP GetFlags(EXPCMDFLAGS* flags) override
    {
        if (flags == nullptr)
        {
            return E_POINTER;
        }
        *flags = spec_ == nullptr ? ECF_HASSUBCOMMANDS : ECF_DEFAULT;
        return S_OK;
    }

    IFACEMETHODIMP EnumSubCommands(IEnumExplorerCommand** commands) override;

private:
    std::atomic<ULONG> references_{1};
    const CommandSpec* spec_;
};

class CommandEnumerator final : public IEnumExplorerCommand
{
public:
    CommandEnumerator()
    {
        ++g_objectCount;
    }

    ~CommandEnumerator()
    {
        --g_objectCount;
    }

    IFACEMETHODIMP QueryInterface(REFIID iid, void** object) override
    {
        if (object == nullptr)
        {
            return E_POINTER;
        }
        *object = nullptr;
        if (iid == IID_IUnknown || iid == IID_IEnumExplorerCommand)
        {
            *object = static_cast<IEnumExplorerCommand*>(this);
            AddRef();
            return S_OK;
        }
        return E_NOINTERFACE;
    }

    IFACEMETHODIMP_(ULONG) AddRef() override
    {
        return ++references_;
    }

    IFACEMETHODIMP_(ULONG) Release() override
    {
        const ULONG references = --references_;
        if (references == 0)
        {
            delete this;
        }
        return references;
    }

    IFACEMETHODIMP Next(
        ULONG count,
        IExplorerCommand** commands,
        ULONG* fetched) override
    {
        if (commands == nullptr || (count > 1 && fetched == nullptr))
        {
            return E_POINTER;
        }
        ULONG produced = 0;
        while (produced < count && index_ < ARRAYSIZE(kCommands))
        {
            commands[produced] = new (std::nothrow) ExplorerCommand(&kCommands[index_]);
            if (commands[produced] == nullptr)
            {
                break;
            }
            ++produced;
            ++index_;
        }
        if (fetched != nullptr)
        {
            *fetched = produced;
        }
        return produced == count ? S_OK : S_FALSE;
    }

    IFACEMETHODIMP Skip(ULONG count) override
    {
        index_ = min(index_ + count, static_cast<ULONG>(ARRAYSIZE(kCommands)));
        return index_ < ARRAYSIZE(kCommands) ? S_OK : S_FALSE;
    }

    IFACEMETHODIMP Reset() override
    {
        index_ = 0;
        return S_OK;
    }

    IFACEMETHODIMP Clone(IEnumExplorerCommand** result) override
    {
        if (result == nullptr)
        {
            return E_POINTER;
        }
        auto* clone = new (std::nothrow) CommandEnumerator();
        if (clone == nullptr)
        {
            return E_OUTOFMEMORY;
        }
        clone->index_ = index_;
        *result = clone;
        return S_OK;
    }

private:
    std::atomic<ULONG> references_{1};
    ULONG index_ = 0;
};

IFACEMETHODIMP ExplorerCommand::EnumSubCommands(IEnumExplorerCommand** commands)
{
    if (commands == nullptr)
    {
        return E_POINTER;
    }
    *commands = nullptr;
    if (spec_ != nullptr)
    {
        return E_NOTIMPL;
    }
    auto* enumerator = new (std::nothrow) CommandEnumerator();
    if (enumerator == nullptr)
    {
        return E_OUTOFMEMORY;
    }
    *commands = enumerator;
    return S_OK;
}

class ClassFactory final : public IClassFactory
{
public:
    IFACEMETHODIMP QueryInterface(REFIID iid, void** object) override
    {
        if (object == nullptr)
        {
            return E_POINTER;
        }
        *object = nullptr;
        if (iid == IID_IUnknown || iid == IID_IClassFactory)
        {
            *object = static_cast<IClassFactory*>(this);
            AddRef();
            return S_OK;
        }
        return E_NOINTERFACE;
    }

    IFACEMETHODIMP_(ULONG) AddRef() override
    {
        return ++references_;
    }

    IFACEMETHODIMP_(ULONG) Release() override
    {
        const ULONG references = --references_;
        if (references == 0)
        {
            delete this;
        }
        return references;
    }

    IFACEMETHODIMP CreateInstance(
        IUnknown* outer,
        REFIID iid,
        void** object) override
    {
        if (outer != nullptr)
        {
            return CLASS_E_NOAGGREGATION;
        }
        auto* command = new (std::nothrow) ExplorerCommand();
        if (command == nullptr)
        {
            return E_OUTOFMEMORY;
        }
        const HRESULT result = command->QueryInterface(iid, object);
        command->Release();
        return result;
    }

    IFACEMETHODIMP LockServer(BOOL lock) override
    {
        g_objectCount += lock ? 1 : -1;
        return S_OK;
    }

private:
    std::atomic<ULONG> references_{1};
};
} // namespace

extern "C" HRESULT __stdcall DllGetClassObject(
    REFCLSID classId,
    REFIID iid,
    void** object)
{
    if (classId != CLSID_RagCloudCommand)
    {
        return CLASS_E_CLASSNOTAVAILABLE;
    }
    auto* factory = new (std::nothrow) ClassFactory();
    if (factory == nullptr)
    {
        return E_OUTOFMEMORY;
    }
    const HRESULT result = factory->QueryInterface(iid, object);
    factory->Release();
    return result;
}

extern "C" HRESULT __stdcall DllCanUnloadNow()
{
    return g_objectCount == 0 ? S_OK : S_FALSE;
}

BOOL WINAPI DllMain(HINSTANCE, DWORD, void*)
{
    return TRUE;
}
