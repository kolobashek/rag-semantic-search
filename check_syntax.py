import ast, sys

files = [
    r"src\rag_catalog\ui\nice_app.py",
    r"src\rag_catalog\core\telemetry_db.py",
]

import os
os.chdir(r"D:\Docs\Claude\Projects\Semantic search")

for path in files:
    try:
        src = open(path, encoding="utf-8").read()
        ast.parse(src)
        print(f"OK: {path}")
    except SyntaxError as e:
        print(f"SYNTAX ERROR in {path}")
        print(f"  Line {e.lineno}: {e.msg}")
        print(f"  >> {(e.text or '').rstrip()}")
    except Exception as e:
        print(f"ERROR: {path}: {e}")
