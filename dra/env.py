from __future__ import annotations

from pathlib import Path

from dotenv import find_dotenv, load_dotenv


def candidate_dotenv_paths(module_file: str) -> list[Path]:
    """Return dotenv locations to try, preferring the caller's current workspace.

    This lets an installed CLI still honor a repo-local ``.env`` when the user runs
    commands from that checkout, while preserving the package-relative fallback.
    """

    candidates: list[Path] = []

    cwd_env = find_dotenv(usecwd=True)
    if cwd_env:
        candidates.append(Path(cwd_env).resolve())

    package_env = (Path(module_file).resolve().parent.parent / ".env").resolve()
    if package_env not in candidates:
        candidates.append(package_env)

    return candidates


def load_dotenv_for(module_file: str) -> None:
    for path in candidate_dotenv_paths(module_file):
        load_dotenv(path, override=False)
