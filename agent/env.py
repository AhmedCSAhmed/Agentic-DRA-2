"""Load project root `.env` before the SDK / DB run (``OPENAI_API_KEY``, ``DATABASE_URL``, etc.)."""

from __future__ import annotations

from pathlib import Path

from dotenv import load_dotenv

_loaded = False


def load_project_dotenv() -> None:
    """Load `.env` from the repository root (parent of the `agent/` package).

    Does not override variables already set in the process environment.
    Safe to call more than once.
    """

    global _loaded
    if _loaded:
        return
    root = Path(__file__).resolve().parent.parent
    load_dotenv(root / ".env", override=False)
    _loaded = True
