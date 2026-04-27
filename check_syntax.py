import ast
from pathlib import Path

ROOT = Path(__file__).resolve().parent
FILES = [
    ROOT / "src" / "rag_catalog" / "ui" / "nice_app.py",
    ROOT / "src" / "rag_catalog" / "core" / "telemetry_db.py",
]

for path in FILES:
    try:
        src = path.read_text(encoding="utf-8")
        ast.parse(src, filename=str(path))
        print(f"OK: {path}")
    except SyntaxError as e:
        print(f"SYNTAX ERROR in {path}")
        print(f"  Line {e.lineno}: {e.msg}")
        print(f"  >> {(e.text or '').rstrip()}")
    except Exception as e:
        print(f"ERROR: {path}: {e}")
