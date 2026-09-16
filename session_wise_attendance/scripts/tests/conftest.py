import sys
from pathlib import Path

# Scripts live one directory up from tests/ — make them importable.
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
