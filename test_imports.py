"""Тест: какой импорт вешает Python 3.11 на Windows."""
import sys
import time

def test(name, fn):
    t = time.time()
    try:
        fn()
        print(f"OK   {name}  ({time.time()-t:.1f}s)", flush=True)
    except Exception as e:
        print(f"ERR  {name}: {e}  ({time.time()-t:.1f}s)", flush=True)

test("platform patch", lambda: None)

from _platform_compat import apply_windows_platform_workarounds
apply_windows_platform_workarounds()

test("import qdrant_client", lambda: __import__("qdrant_client"))
test("import streamlit",     lambda: __import__("streamlit"))
test("connect qdrant",       lambda: __import__("qdrant_client").QdrantClient(url="http://localhost:6333").get_collections())
test("import sentence_transformers", lambda: __import__("sentence_transformers"))

print("ALL DONE", flush=True)
