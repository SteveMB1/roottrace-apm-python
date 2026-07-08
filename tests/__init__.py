import sys
from pathlib import Path

# Let `python3 -m unittest discover` find the src-layout package uninstalled.
_src = str(Path(__file__).resolve().parent.parent / "src")
if _src not in sys.path:
    sys.path.insert(0, _src)
