# Local smoke checks for RAG Catalog.
#
# Run from the project root:
#   powershell -ExecutionPolicy Bypass -File scripts\check.ps1
#
# The script returns exit code 0 on success and 1 on any failed check.

$ErrorActionPreference = 'Stop'
$script:Failed = 0

function Section($name) {
    Write-Host ""
    Write-Host "=== $name ===" -ForegroundColor Cyan
}

function Pass($msg) {
    Write-Host "  OK   $msg" -ForegroundColor Green
}

function Fail($msg) {
    Write-Host "  FAIL $msg" -ForegroundColor Red
    $script:Failed += 1
}

$ProjectRoot = Split-Path -Parent $PSScriptRoot
Set-Location $ProjectRoot
Write-Host "Project root: $ProjectRoot"

$Python = 'python'

Section 'py_compile root entrypoints'
$shims = @(
    'rag_search.py',
    'rag_search_fixed.py',
    'index_rag.py',
    'ocr_pdfs.py',
    'telegram_bot.py',
    'windows_app.py',
    'rag_core.py',
    'user_auth_db.py',
    'telemetry_db.py',
    'app_ui.py',
    'nice_app.py',
    'run_automation.py'
)

foreach ($f in $shims) {
    if (-not (Test-Path $f)) {
        Fail "$f is missing"
        continue
    }

    $out = & $Python -m py_compile $f 2>&1
    if ($LASTEXITCODE -eq 0) {
        Pass $f
    } else {
        Fail "$f : $out"
    }
}

Section 'pytest'
& $Python -m pytest -q tests
if ($LASTEXITCODE -eq 0) {
    Pass 'all tests passed'
} else {
    Fail "pytest exit=$LASTEXITCODE"
}

Section 'CLI entrypoint --help'
$cliShims = @(
    @{script='rag_search.py';       marker='--content-only'},
    @{script='rag_search_fixed.py'; marker='--content-only'},
    @{script='index_rag.py';        marker='--stage'},
    @{script='ocr_pdfs.py';         marker='--dry-run'},
    @{script='nice_app.py';         marker='--no-show'}
)

foreach ($c in $cliShims) {
    $oldErrorActionPreference = $ErrorActionPreference
    $ErrorActionPreference = 'Continue'
    $out = & $Python $c.script --help 2>&1 | Out-String
    $exitCode = $LASTEXITCODE
    $ErrorActionPreference = $oldErrorActionPreference

    if ($exitCode -eq 0 -and $out -match 'usage:' -and $out -match [regex]::Escape($c.marker)) {
        Pass "$($c.script) --help contains $($c.marker)"
    } else {
        $preview = $out.Substring(0, [Math]::Min(200, $out.Length))
        Fail "$($c.script) --help missing usage or marker $($c.marker). Output: $preview"
    }
}

Section 'Qdrant ping'
$qdrantUrl = & $Python -c @"
import json, pathlib
p = pathlib.Path('config.json')
if p.exists():
    try:
        raw = p.read_bytes().rstrip(b'\x00')
        data = json.loads(raw.decode('utf-8-sig'))
        print(data.get('qdrant_url', ''))
    except Exception:
        print('')
else:
    print('')
"@ 2>&1
$qdrantUrl = "$qdrantUrl".Trim()

if (-not $qdrantUrl) {
    Write-Host "  SKIP Qdrant (qdrant_url is not set in config.json)" -ForegroundColor Yellow
} else {
    try {
        $resp = Invoke-WebRequest -Uri "$qdrantUrl/collections" -TimeoutSec 5 -UseBasicParsing
        if ($resp.StatusCode -eq 200) {
            Pass "Qdrant $qdrantUrl is reachable"
        } else {
            Fail "Qdrant returned HTTP $($resp.StatusCode)"
        }
    } catch {
        Fail "Qdrant is not reachable at $qdrantUrl : $($_.Exception.Message)"
    }
}

Section 'config.example.json leak check'
if (Test-Path 'config.example.json') {
    $example = Get-Content 'config.example.json' -Raw
    $bad = $example | Select-String -Pattern '"telegram_bot_token"\s*:\s*"\d+:' -AllMatches
    if ($bad.Matches.Count -gt 0) {
        Fail 'config.example.json contains a non-empty telegram_bot_token'
    } else {
        Pass 'config.example.json does not contain a real token'
    }
} else {
    Fail 'config.example.json is missing'
}

Section 'config.json validity'
if (Test-Path 'config.json') {
    $validateCode = @"
import json, pathlib, sys
raw = pathlib.Path('config.json').read_bytes().rstrip(b'\x00')
try:
    data = json.loads(raw.decode('utf-8-sig'))
    required = ['catalog_path','qdrant_url','collection_name','embedding_model']
    missing = [k for k in required if k not in data]
    if missing:
        print('MISSING:', missing)
        sys.exit(1)
    print('OK')
except Exception as e:
    print('INVALID:', e)
    sys.exit(1)
"@
    $out = & $Python -c $validateCode 2>&1
    if ($LASTEXITCODE -eq 0) {
        Pass "config.json is valid ($out)"
    } else {
        Fail "config.json: $out"
    }
} else {
    Write-Host "  SKIP config.json is missing, which is fine for a fresh clone" -ForegroundColor Yellow
}

Section 'Summary'
if ($script:Failed -eq 0) {
    Write-Host "ALL CHECKS PASSED" -ForegroundColor Green
    exit 0
}

Write-Host "FAILED CHECKS: $script:Failed" -ForegroundColor Red
exit 1
