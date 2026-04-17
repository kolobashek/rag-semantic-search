# monitor_index.ps1 — Мониторинг прогресса RAG-индексатора в реальном времени
# Запуск: .\monitor_index.ps1
# Выход:  Ctrl+C

param(
    [string]$LogFile    = "",
    [string]$QdrantUrl  = "http://localhost:6333",
    [string]$Collection = "catalog",
    [int]   $Interval   = 5,       # секунды между обновлениями
    [int]   $TotalFiles = 58058    # всего файлов в каталоге
)

function Resolve-DefaultLogFile {
    try {
        $cfgPath = Join-Path $PSScriptRoot "config.json"
        if (Test-Path $cfgPath) {
            $cfg = Get-Content $cfgPath -Raw | ConvertFrom-Json
            if ($cfg.log_file -and $cfg.log_file.Trim().Length -gt 0) {
                return [string]$cfg.log_file
            }
        }
    } catch {
    }
    return (Join-Path $PSScriptRoot "rag_automation.log")
}

if (-not $LogFile -or $LogFile.Trim().Length -eq 0) {
    $LogFile = Resolve-DefaultLogFile
}

$ChunksPerFile = 15  # среднее кол-во чанков на файл

# ── Получить реальное число точек из Qdrant ──────────────────────────────
function Get-QdrantTotal {
    try {
        $resp = Invoke-RestMethod -Uri "$QdrantUrl/collections/$Collection" -TimeoutSec 3 -ErrorAction Stop
        return [int]$resp.result.points_count
    } catch { return -1 }
}

# ── Последние N батчей из лога (не только последний) ────────────────────
function Get-LogBatches($n = 10) {
    $lines = Get-Content $LogFile -Tail 300 -ErrorAction SilentlyContinue
    return ($lines | Select-String "Записан батч") | Select-Object -Last $n
}

# ── Время последнего батча (сколько секунд назад) ────────────────────────
function Get-SecondsSinceLastBatch {
    $batches = Get-LogBatches 1
    if (-not $batches) { return -1 }
    if ($batches[-1].Line -match "(\d{2}:\d{2}:\d{2})") {
        $ts = [datetime]::ParseExact($Matches[1], "HH:mm:ss", $null)
        $t = (Get-Date).Date.Add($ts.TimeOfDay)
        return [math]::Round(((Get-Date) - $t).TotalSeconds, 0)
    }
    return -1
}

# ── Время ЛЮБОЙ активности в логе (сколько секунд назад) ─────────────────
function Get-SecondsSinceAnyActivity {
    $lines = Get-Content $LogFile -Tail 50 -ErrorAction SilentlyContinue
    if (-not $lines) { return -1 }
    # Ищем любую строку с временной меткой HH:mm:ss
    $last = ($lines | Where-Object { $_ -match "\d{2}:\d{2}:\d{2}" }) | Select-Object -Last 1
    if (-not $last) { return -1 }
    if ($last -match "(\d{2}:\d{2}:\d{2})") {
        $ts = [datetime]::ParseExact($Matches[1], "HH:mm:ss", $null)
        $t = (Get-Date).Date.Add($ts.TimeOfDay)
        return [math]::Round(((Get-Date) - $t).TotalSeconds, 0)
    }
    return -1
}

# ── Последние значимые строки лога (без HTTP-мусора) ────────────────────
function Get-LastActivity($n = 5) {
    $lines = Get-Content $LogFile -Tail 100 -ErrorAction SilentlyContinue
    return ($lines | Where-Object {
        $_ -match "(Записан батч|Найдено файлов|WARNING|ERROR|Файл .+чанков|Нет текст|OCR|изменил|удаляю|Начало индекс|Завершено|Подключен)"
    }) | Select-Object -Last $n
}

# ── Найти процесс индексатора (самый тяжёлый python, >500 МБ) ────────────
function Get-IndexerProcess {
    $procs = Get-Process python* -ErrorAction SilentlyContinue | Sort-Object WorkingSet64 -Descending
    $heavy = $procs | Where-Object { $_.WorkingSet64 -gt 400MB } | Select-Object -First 1
    if ($heavy) { return $heavy }
    return $procs | Select-Object -First 1
}

# ── Скользящее среднее (очередь последних N замеров скорости) ────────────
$speedBuf = [System.Collections.Generic.Queue[double]]::new()
$prevQTotal = -1
$prevTime   = Get-Date

Write-Host ""
Write-Host "  RAG Монитор запущен. Ctrl+C для выхода." -ForegroundColor Cyan
Write-Host "  Лог: $LogFile  |  Qdrant: $QdrantUrl/$Collection" -ForegroundColor DarkGray
Write-Host ""

while ($true) {
    $now     = Get-Date
    $qTotal  = Get-QdrantTotal
    $dt      = ($now - $prevTime).TotalSeconds

    # Скорость по Qdrant (если доступен), иначе по логу
    if ($qTotal -ge 0 -and $prevQTotal -ge 0 -and $dt -gt 0) {
        $delta = [math]::Max(0, $qTotal - $prevQTotal)
        $speedBuf.Enqueue($delta / $dt)
        if ($speedBuf.Count -gt 6) { [void]$speedBuf.Dequeue() }
    }
    $speed    = if ($speedBuf.Count -gt 0) { ($speedBuf | Measure-Object -Average).Average } else { 0 }
    $speedMin = [math]::Round($speed * 60, 0)
    $fpm      = [math]::Round($speedMin / $ChunksPerFile, 1)

    # Прогресс
    $displayTotal = if ($qTotal -ge 0) { $qTotal } else { 0 }
    $filesEst     = [math]::Round($displayTotal / $ChunksPerFile, 0)
    $filesLeft    = [math]::Max(0, $TotalFiles - $filesEst)
    $pct          = [math]::Min(100, [math]::Round($filesEst / $TotalFiles * 100, 2))

    # ETA
    $etaStr = if ($speed -gt 0) {
        $etaSec = $filesLeft * $ChunksPerFile / $speed
        $etaH = [math]::Floor($etaSec / 3600)
        $etaM = [math]::Floor(($etaSec % 3600) / 60)
        "~${etaH}ч ${etaM}мин"
    } elseif ($speedBuf.Count -eq 0) {
        "расчёт..."
    } else {
        "пауза (0 точек/мин)"
    }

    # Статус: смотрим И на батчи, И на любую активность лога
    $secSince    = Get-SecondsSinceLastBatch
    $secActivity = Get-SecondsSinceAnyActivity

    if ($secSince -lt 0 -and $secActivity -lt 0) {
        $statusMsg = "нет данных по логу"
        $statusColor = "DarkGray"
    } elseif ($secSince -ge 0 -and $secSince -lt 15) {
        $statusMsg = "▶ АКТИВНО  (батч ${secSince}с назад)"
        $statusColor = "Green"
    } elseif ($secActivity -ge 0 -and $secActivity -lt 30) {
        # Файлы читаются, батч ещё не наполнен (скан-PDF = 0 чанков)
        $statusMsg = "▶ ЧИТАЕТ ФАЙЛЫ  (активность ${secActivity}с назад, батч ${secSince}с назад)"
        $statusColor = "Green"
    } elseif ($secActivity -ge 0 -and $secActivity -lt 120) {
        $statusMsg = "◌ обрабатывает файл... (активность ${secActivity}с назад)"
        $statusColor = "Yellow"
    } elseif ($secActivity -ge 0 -and $secActivity -lt 600) {
        $statusMsg = "◌ крупный/скан-PDF по сети (${secActivity}с) — норма, ждём"
        $statusColor = "Yellow"
    } elseif ($secActivity -ge 0 -and $secActivity -lt 1800) {
        $statusMsg = "⚠ ОЧЕНЬ МЕДЛЕННО (${secActivity}с без лога) — возможно завис на файле"
        $statusColor = "DarkYellow"
    } else {
        $noLog = if ($secActivity -ge 0) { "${secActivity}с" } else { "нет данных" }
        $statusMsg = "❌ ВОЗМОЖНО ЗАВИС ($noLog без активности) — проверьте лог!"
        $statusColor = "Red"
    }

    # Процесс
    $indexer = Get-IndexerProcess
    if ($indexer) {
        $procInfo = "PID $($indexer.Id)   RAM $([math]::Round($indexer.WorkingSet64/1MB,0)) МБ   CPU $([math]::Round($indexer.CPU,0))с"
        $procColor = if ($indexer.WorkingSet64 -gt 400MB) { "White" } else { "DarkYellow" }
    } else {
        $procInfo = "❌ python-процесс не найден!"
        $procColor = "Red"
    }

    # Прогресс-бар
    $barWidth = 42
    $filled   = [math]::Round($barWidth * $pct / 100)
    $bar      = ("█" * $filled) + ("░" * ($barWidth - $filled))

    # Последняя активность
    $activity = Get-LastActivity 5

    # ── Отрисовка ──────────────────────────────────────────────────────────
    Clear-Host
    Write-Host ""
    Write-Host "  ╔════════════════════════════════════════════════════════════════╗" -ForegroundColor Cyan
    Write-Host ("  ║   RAG INDEXER — ПРОГРЕСС   {0,-10}                        ║" -f $now.ToString("HH:mm:ss")) -ForegroundColor Cyan
    Write-Host "  ╠════════════════════════════════════════════════════════════════╣" -ForegroundColor Cyan
    Write-Host ("  ║  [{0}] {1,6}%   ║" -f $bar, $pct) -ForegroundColor White
    Write-Host "  ╠════════════════════════════════════════════════════════════════╣" -ForegroundColor Cyan

    if ($qTotal -ge 0) {
        Write-Host ("  ║  Точек в Qdrant:     {0,-8}  (Δ +{1} за {2}с)" -f $qTotal, ([math]::Max(0,$qTotal - $prevQTotal)), [math]::Round($dt,0)) -ForegroundColor White
    } else {
        Write-Host "  ║  Qdrant недоступен   (Docker не запущен?)" -ForegroundColor Red
    }
    Write-Host ("  ║  Файлов (оценка):    {0} / {1}" -f $filesEst, $TotalFiles) -ForegroundColor White
    Write-Host ("  ║  Скорость:           {0} точек/мин  (~{1} файлов/мин)" -f $speedMin, $fpm) -ForegroundColor White
    Write-Host ("  ║  ETA:                {0}" -f $etaStr) -ForegroundColor White
    $batchAgo = if ($secSince -ge 0) { "${secSince}с назад" } else { "нет данных" }
    $actAgo   = if ($secActivity -ge 0) { "${secActivity}с назад" } else { "нет данных" }
    Write-Host ("  ║  Последний батч:     {0,-20}  Активность лога: {1}" -f $batchAgo, $actAgo) -ForegroundColor DarkGray

    Write-Host "  ╠════════════════════════════════════════════════════════════════╣" -ForegroundColor Cyan
    Write-Host -NoNewline "  ║  Статус:  "
    Write-Host $statusMsg -ForegroundColor $statusColor

    Write-Host "  ╠════════════════════════════════════════════════════════════════╣" -ForegroundColor Cyan
    Write-Host -NoNewline "  ║  Процесс: "
    Write-Host $procInfo -ForegroundColor $procColor

    Write-Host "  ╠════════════════════════════════════════════════════════════════╣" -ForegroundColor Cyan
    Write-Host "  ║  Последняя активность в логе:" -ForegroundColor DarkGray
    if ($activity) {
        $activity | ForEach-Object {
            $line = ($_ -replace '^\d{4}-\d{2}-\d{2} ', '' -replace ' - (INFO|DEBUG|WARNING|ERROR) - ', ' ')
            if ($line.Length -gt 70) { $line = $line.Substring(0, 67) + "..." }
            $col = if ($_ -match "WARNING|ERROR") { "DarkYellow" } else { "Gray" }
            Write-Host ("  ║    {0}" -f $line) -ForegroundColor $col
        }
    } else {
        Write-Host "  ║    (нет данных)" -ForegroundColor DarkGray
    }
    Write-Host "  ╚════════════════════════════════════════════════════════════════╝" -ForegroundColor Cyan
    Write-Host ""
    Write-Host ("  Qdrant: {0} точек реально   |   Обновление каждые {1}с   |   Ctrl+C выход" -f $displayTotal, $Interval) -ForegroundColor DarkGray

    $prevQTotal = $qTotal
    $prevTime   = $now

    Start-Sleep -Seconds $Interval
}
