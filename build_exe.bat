@echo off
REM Build RAG Katalog EXE - Fixed version

echo.
echo ========== RAG-Katalog Builder ==========
echo.

REM Check if PyInstaller is installed
echo Checking PyInstaller...
python -m pip show pyinstaller >nul 2>&1
if errorlevel 1 (
    echo Installing PyInstaller...
    python -m pip install pyinstaller
)

REM Check other dependencies
echo Checking dependencies...
python -m pip show PyQt6 >nul 2>&1
if errorlevel 1 python -m pip install PyQt6

python -m pip show sentence-transformers >nul 2>&1
if errorlevel 1 python -m pip install sentence-transformers

python -m pip show qdrant-client >nul 2>&1
if errorlevel 1 python -m pip install qdrant-client

REM Clean previous builds (dist и build — временные артефакты PyInstaller)
REM НЕ удаляем RAG-Katalog.spec — он настроен и переиспользуется!
echo.
echo Cleaning previous builds...
if exist "dist"  rmdir /s /q dist  >nul 2>&1
if exist "build" rmdir /s /q build >nul 2>&1
if exist "__pycache__" rmdir /s /q __pycache__ >nul 2>&1

REM Build executable
REM Используем существующий RAG-Katalog.spec если он есть,
REM иначе собираем с явным списком файлов данных (без ".:." чтобы
REM не включать логи, временные файлы и кэши HuggingFace в EXE).
echo.
echo Building RAG-Katalog.exe...
echo This may take 5-10 minutes...
echo.

if exist "RAG-Katalog.spec" (
    echo Using existing RAG-Katalog.spec...
    python -m PyInstaller RAG-Katalog.spec
) else (
    echo Spec not found — building from scratch...
    python -m PyInstaller --name="RAG-Katalog" ^
        --onefile ^
        --windowed ^
        --add-data="config.json;." ^
        --add-data="rag_core.py;." ^
        --hidden-import=sentence_transformers ^
        --hidden-import=qdrant_client ^
        --hidden-import=PyQt6 ^
        --hidden-import=pdfplumber ^
        --hidden-import=docx ^
        --hidden-import=openpyxl ^
        --hidden-import=xlrd ^
        --collect-all=sentence_transformers ^
        --collect-all=qdrant_client ^
        windows_app.py
)

REM Check result
if errorlevel 1 (
    echo.
    echo ERROR: Build failed!
    echo.
    echo Troubleshooting:
    echo 1. Make sure windows_app.py is in current directory
    echo 2. Check Python: python --version
    echo 3. Check pip: python -m pip --version
    echo 4. Reinstall dependencies: python -m pip install --upgrade PyQt6 sentence-transformers qdrant-client
    echo.
    pause
    exit /b 1
)

echo.
echo ========== BUILD SUCCESSFUL ==========
echo.
echo EXE file: dist\RAG-Katalog.exe
echo File size: ~300 MB
echo.
echo Next steps:
echo 1. dist\RAG-Katalog.exe
echo 2. Or copy dist\ folder to another computer
echo 3. Or create shortcut on desktop
echo.
pause