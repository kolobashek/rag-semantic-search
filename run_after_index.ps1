# run_after_index.ps1 — Ждёт завершения индексатора, затем запускает OCR
#
# Запуск:
#   .\run_after_index.ps1                        # ждёт любой крупный python-процесс
#   .\run_after_index.ps1 -IndexerPid 10476      # ждёт конкретный PID
#   .\run_after_index.ps1 -SkipWait              # сразу запустить OCR (индексатор уже завершён)

param(
    [int]    $IndexerPid  = 0,
    [switch] $SkipWait    = $false,
    [string] $Python      = "C:\Python314\python.exe",
    [string] $ScriptDir   = "D:\Docs\Claude\Projects\Semantic search",
    [string] $QdrantUrl   = "http://localhost:6333",
    [string] $LogFile     = "O:\rag_automation.log"
)

$OcrScript = Join-Path $ScriptDir "ocr_pdfs.py"

function Write-Status($msg, $color = "Cyan") {
    Write-Host "  [$(Get-Date -Format 'HH:mm:ss')]  $msg" -ForegroundColor $color
}

Write-Host ""
Write-Host "  ╔══════════════════════════════════════════════════════╗" -ForegroundColor Cyan
Write-Host "  ║   RAG — Авто-запуск OCR после индексирования         ║" -ForegroundColor Cyan
Write-Host "  ╚══════════════════════════════════════════════════════╝" -ForegroundColor Cyan
Write-Host ""

# ── 1. Ждём завершения индексатора ───────────────────────────────────────
if (-not $SkipWait) {

    # Определяем PID если не задан — берём самый тяжёлый python-процесс (> 500 МБ)
    if ($IndexerPid -eq 0) {
        $heavy = Get-Process python* -ErrorAction SilentlyContinue |
                 Where-Object { $_.WorkingSet64 -gt 500MB } |
                 Sort-Object WorkingSet64 -Descending | Select-Object -First 1
        if ($heavy) {
            $IndexerPid = $heavy.Id
            Write-Status "Найден индексатор: PID $IndexerPid (RAM $([math]::Round($heavy.WorkingSet64/1MB,0)) МБ)"
        } else {
            Write-Status "Крупный python-процесс не найден — возможно индексатор уже завершился." "Yellow"
            Write-Status "Запускаю OCR немедленно..." "Yellow"
            $SkipWait = $true
        }
    } else {
        Write-Status "Ожидаю завершения PID $IndexerPid..."
    }

    if (-not $SkipWait) {
        $dotCount  = 0
        $lastTotal = 0
        $prevTotal = 0
        $lastBatchTime = Get-Date

        while ($true) {
            $proc = Get-Process -Id $IndexerPid -ErrorAction SilentlyContinue
            if (-not $proc) {
                Write-Host ""
                Write-Status "Индексатор (PID $IndexerPid) завершился!" "Green"
                break
            }

            # Получаем прогресс из Qdrant
            try {
                $resp = Invoke-RestMethod -Uri "$QdrantUrl/collections/catalog" -TimeoutSec 3 -ErrorAction Stop
                $total = [int]$resp.result.points_count
            } catch { $total = $lastTotal }

            if ($total -ne $lastTotal) {
                $lastTotal = $total
                $lastBatchTime = Get-Date
            }

            $secSince = [math]::Round(((Get-Date) - $lastBatchTime).TotalSeconds, 0)
            $statusIcon = if ($secSince -lt 60) { "▶" } elseif ($secSince -lt 300) { "◌" } else { "⚠" }
            $ram = [math]::Round($proc.WorkingSet64/1MB,0)

            Write-Host -NoNewline "`r  $statusIcon  Точек: $total  |  RAM: ${ram}МБ  |  Тишина: ${secSince}с  |  Ctrl+C = прервать   "

            Start-Sleep -Seconds 10
        }
    }
}

Write-Host ""
Write-Host ""

# ── 2. Проверяем Qdrant и итоговое число точек ───────────────────────────
Write-Status "Проверяю состояние коллекции Qdrant..."
try {
    $resp = Invoke-RestMethod -Uri "$QdrantUrl/collections/catalog" -TimeoutSec 5 -ErrorAction Stop
    $finalTotal = [int]$resp.result.points_count
    Write-Status "Коллекция 'catalog': $finalTotal точек" "Green"
} catch {
    Write-Status "Qdrant недоступен: $_" "Red"
}

# ── 3. Запускаем OCR ─────────────────────────────────────────────────────
Write-Host ""
Write-Status "Запускаю ocr_pdfs.py..." "Yellow"
Write-Host "  (Найдёт PDF с пустым текстом → переиндексирует с OCR)" -ForegroundColor DarkGray
Write-Host "  (Ctrl+C для прерывания — прогресс сохраняется)" -ForegroundColor DarkGray
Write-Host ""

if (-not (Test-Path $OcrScript)) {
    Write-Status "ОШИБКА: $OcrScript не найден!" "Red"
    exit 1
}

$ocrArgs = "`"$OcrScript`" --url $QdrantUrl"
try {
    $ocrProc = Start-Process -FilePath $Python -ArgumentList $ocrArgs `
               -WorkingDirectory $ScriptDir -PassThru -NoNewWindow -Wait
    Write-Host ""
    if ($ocrProc.ExitCode -eq 0) {
        Write-Status "OCR завершён успешно!" "Green"
    } else {
        Write-Status "OCR завершился с кодом $($ocrProc.ExitCode)" "Yellow"
    }
} catch {
    Write-Status "Ошибка запуска OCR: $_" "Red"
    exit 1
}

Write-Host ""
Write-Status "Готово. Запускай поиск:" "Green"
Write-Host "  python rag_search.py --url http://localhost:6333" -ForegroundColor White
Write-Host ""
