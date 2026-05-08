; RAG Sync Client — Inno Setup installer script
; Requires Inno Setup 6.x: https://jrsoftware.org/isinfo.php

#define AppName "RAG Sync Client"
#define AppVersion "1.0"
#define AppPublisher "RAG Catalog"
#define AppExeName "rag_sync_client.exe"

[Setup]
AppId={{B3F2A1C4-9E7D-4F88-A2B1-C3D4E5F60011}
AppName={#AppName}
AppVersion={#AppVersion}
AppPublisher={#AppPublisher}
DefaultDirName={autopf}\{#AppName}
DefaultGroupName={#AppName}
DisableProgramGroupPage=yes
OutputDir=dist
OutputBaseFilename=RAGSyncClientSetup
Compression=lzma2/ultra64
SolidCompression=yes
WizardStyle=modern
ArchitecturesInstallIn64BitMode=x64compatible
MinVersion=10.0
ShowLanguageDialog=no
PrivilegesRequired=lowest
PrivilegesRequiredOverridesAllowed=dialog
UninstallDisplayIcon={app}\{#AppExeName}
CloseApplications=no

[Languages]
Name: "russian"; MessagesFile: "compiler:Languages\Russian.isl"

[Files]
Source: "dist\{#AppExeName}"; DestDir: "{app}"; Flags: ignoreversion

[Icons]
Name: "{group}\{#AppName}"; Filename: "{app}\{#AppExeName}"
Name: "{group}\Удалить {#AppName}"; Filename: "{uninstallexe}"

[Code]

var
  ServerPage: TInputQueryWizardPage;
  TokenPage: TInputQueryWizardPage;
  OptionsPage: TInputOptionWizardPage;

function GetComputerName: String;
var
  Len: DWORD;
begin
  Len := 256;
  SetLength(Result, Len);
  if not GetComputerNameExW(0, Result, Len) then
    Result := 'pc';
  SetLength(Result, Len);
end;

procedure InitializeWizard;
begin
  ServerPage := CreateInputQueryPage(
    wpSelectDir,
    'Сервер RAG Catalog',
    'Укажите адрес сервера',
    'Введите URL сервера RAG Catalog, к которому подключается этот компьютер.'
  );
  ServerPage.Add('Адрес сервера (например http://192.168.1.10:8080):', False);
  ServerPage.Values[0] := 'http://';

  TokenPage := CreateInputQueryPage(
    ServerPage.ID,
    'Токен авторизации',
    'Укажите токен для входа',
    'Откройте веб-интерфейс RAG Catalog → профиль, скопируйте сессионный токен и вставьте сюда.'
  );
  TokenPage.Add('Токен:', False);

  OptionsPage := CreateInputOptionPage(
    TokenPage.ID,
    'Параметры запуска',
    'Автозапуск и ярлык на рабочем столе',
    'Выберите дополнительные параметры установки:',
    False, False
  );
  OptionsPage.Add('Запускать автоматически при входе в Windows');
  OptionsPage.Add('Создать ярлык на рабочем столе');
  OptionsPage.Values[0] := True;
  OptionsPage.Values[1] := False;
end;

function NextButtonClick(CurPageID: Integer): Boolean;
var
  Server: String;
begin
  Result := True;
  if CurPageID = ServerPage.ID then begin
    Server := Trim(ServerPage.Values[0]);
    if (Server = '') or (Server = 'http://') or (Server = 'https://') then begin
      MsgBox('Укажите адрес сервера.', mbError, MB_OK);
      Result := False;
    end;
  end;
  if CurPageID = TokenPage.ID then begin
    if Trim(TokenPage.Values[0]) = '' then begin
      MsgBox('Укажите токен авторизации.', mbError, MB_OK);
      Result := False;
    end;
  end;
end;

procedure WriteConfigFile(Server, Token: String);
var
  ConfigDir, ConfigFile, DeviceId, Content: String;
begin
  ConfigDir := ExpandConstant('{userappdata}') + '\rag_sync';
  ForceDirectories(ConfigDir);
  ConfigFile := ConfigDir + '\config.json';

  DeviceId := 'win-' + GetComputerName + '-setup';

  Content :=
    '{' + #13#10 +
    '  "server": "' + Server + '",' + #13#10 +
    '  "token": "' + Token + '",' + #13#10 +
    '  "device_id": "' + DeviceId + '",' + #13#10 +
    '  "display_name": "' + GetComputerName + ' (Windows)"' + #13#10 +
    '}';

  if not SaveStringToFile(ConfigFile, Content, False) then
    MsgBox('Не удалось записать конфигурацию в ' + ConfigFile, mbError, MB_OK);
end;

procedure RegisterAutostart(Enable: Boolean);
var
  ExePath: String;
begin
  ExePath := ExpandConstant('"{app}\{#AppExeName}"');
  if Enable then
    RegWriteStringValue(
      HKCU,
      'Software\Microsoft\Windows\CurrentVersion\Run',
      'RAGSyncClient',
      ExePath
    )
  else
    RegDeleteValue(
      HKCU,
      'Software\Microsoft\Windows\CurrentVersion\Run',
      'RAGSyncClient'
    );
end;

procedure CreateDesktopShortcutEntry;
begin
  CreateShellLink(
    ExpandConstant('{userdesktop}\{#AppName}.lnk'),
    'RAG Catalog sync agent',
    ExpandConstant('{app}\{#AppExeName}'),
    '', '', '', 0, SW_SHOWNORMAL
  );
end;

procedure CurStepChanged(CurStep: TSetupStep);
begin
  if CurStep = ssPostInstall then begin
    WriteConfigFile(
      Trim(ServerPage.Values[0]),
      Trim(TokenPage.Values[0])
    );
    RegisterAutostart(OptionsPage.Values[0]);
    if OptionsPage.Values[1] then
      CreateDesktopShortcutEntry;
  end;
end;

procedure CurUninstallStepChanged(CurUninstallStep: TUninstallStep);
begin
  if CurUninstallStep = usPostUninstall then begin
    RegisterAutostart(False);
    DeleteFile(ExpandConstant('{userdesktop}\{#AppName}.lnk'));
  end;
end;

[Run]
Filename: "{app}\{#AppExeName}"; Description: "Запустить {#AppName} сейчас"; \
  Flags: nowait postinstall skipifsilent unchecked; Parameters: "--status"
