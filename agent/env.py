"""Load project root `.env` before the SDK / DB run (``OPENAI_API_KEY``, ``DATABASE_URL``, etc.)."""

from __future__ import annotations

from dra.env import load_dotenv_for

_loaded = False


def load_project_dotenv() -> None:
    """Load `.env` from the repository root (parent of the `agent/` package).

    Does not override variables already set in the process environment.
    Safe to call more than once.
    """

    global _loaded
    if _loaded:
        return
    load_dotenv_for(__file__)
    _loaded = True
