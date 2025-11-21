"""Generic helpers (paths, IO, etc.)."""
import pathlib

def project_root() -> pathlib.Path:
    return pathlib.Path(__file__).resolve().parents[1]
