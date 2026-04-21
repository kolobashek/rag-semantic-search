# =============================================================
#  deploy_github.ps1 — Деплой RAG Каталога на GitHub
#  Запускать из PowerShell в папке проекта:
#      cd "путь\к\проекту"
#      .\deploy_github.ps1
# =============================================================

$GITHUB_USER = "kolobashek"
$REPO_NAME   = "rag-semantic-search"
$BRANCH      = "main"

# ── 1. Запрашиваем Personal Access Token ──────────────────────
$Token = Read-Host "Вставьте GitHub Personal Access Token" -AsSecureString
$PlainToken = [Runtime.InteropServices.Marshal]::PtrToStringAuto(
    [Runtime.InteropServices.Marshal]::SecureStringToBSTR($Token)
)

# ── 2. Инициализируем git ──────────────────────────────────────
Write-Host "`n[1/5] Инициализация git репозитория..." -ForegroundColor Cyan
git init
git config user.name  $GITHUB_USER
git config user.email "kolobashek@gmail.com"
git branch -M $BRANCH

# ── 3. Добавляем файлы ─────────────────────────────────────────
Write-Host "[2/5] Добавляем файлы в индекс..." -ForegroundColor Cyan
$FILES = @(
    "app_ui.py",
    "index_rag.py",
    "rag_core.py",
    "rag_search.py",
    "rag_search_fixed.py",
    "ocr_pdfs.py",
    "run_automation.py",
    "test_imports.py",
    "windows_app.py",
    "requirements.txt",
    "build_exe.bat",
    "monitor_index.ps1",
    "run_after_index.ps1",
    ".gitignore",
    "ИНСТРУКЦИЯ.md"
)
foreach ($f in $FILES) {
    if (Test-Path $f) { git add $f }
    else { Write-Host "  Пропуск (не найден): $f" -ForegroundColor Yellow }
}

# ── 4. Первый коммит ───────────────────────────────────────────
Write-Host "[3/5] Создаём первый коммит..." -ForegroundColor Cyan
git commit -m "Initial commit: RAG Semantic Search

- Streamlit UI с поиском, проводником и мониторингом индексирования
- Многоэтапный индексатор (metadata / small / large)
- Поддержка DOCX, XLSX, XLS, PDF (с OCR)
- Qdrant векторная база + sentence-transformers/all-MiniLM-L6-v2"

# ── 5. Создаём репозиторий на GitHub через API ────────────────
Write-Host "[4/5] Создаём репозиторий $GITHUB_USER/$REPO_NAME на GitHub..." -ForegroundColor Cyan
$Headers = @{
    "Authorization" = "token $PlainToken"
    "Accept"        = "application/vnd.github+json"
    "X-GitHub-Api-Version" = "2022-11-28"
}
$Body = @{
    name        = $REPO_NAME
    description = "RAG-система семантического поиска по DOCX/XLSX/PDF. Qdrant + Streamlit + sentence-transformers."
    private     = $false
    auto_init   = $false
} | ConvertTo-Json

try {
    $Response = Invoke-RestMethod `
        -Uri "https://api.github.com/user/repos" `
        -Method POST `
        -Headers $Headers `
        -Body $Body `
        -ContentType "application/json"
    Write-Host "  Репозиторий создан: $($Response.html_url)" -ForegroundColor Green
} catch {
    $Status = $_.Exception.Response.StatusCode.value__
    if ($Status -eq 422) {
        Write-Host "  Репозиторий уже существует — продолжаем." -ForegroundColor Yellow
    } else {
        Write-Host "  Ошибка создания репозитория: $_" -ForegroundColor Red
        exit 1
    }
}

# ── 6. Добавляем remote и пушим ────────────────────────────────
# БЕЗОПАСНО: токен передаётся через http.extraHeader, НЕ встраивается в URL.
# Это не сохраняет секрет в .git/config и не попадает в историю процессов.
Write-Host "[5/5] Пушим в GitHub..." -ForegroundColor Cyan
$RemoteUrl = "https://github.com/$GITHUB_USER/$REPO_NAME.git"

git remote remove origin 2>$null
git remote add origin $RemoteUrl

# Передаём токен через заголовок Authorization — не попадает в .git/config
git -c "http.extraHeader=Authorization: token $PlainToken" push -u origin $BRANCH

if ($LASTEXITCODE -eq 0) {
    Write-Host "`n✅ Готово! Репозиторий: https://github.com/$GITHUB_USER/$REPO_NAME" -ForegroundColor Green
} else {
    Write-Host "`n❌ Ошибка при push. Проверьте токен и права." -ForegroundColor Red
}

# Очищаем токен из памяти
$PlainToken = $null
[System.GC]::Collect()
