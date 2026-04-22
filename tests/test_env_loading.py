from __future__ import annotations

import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

from dra.env import candidate_dotenv_paths, load_dotenv_for


class DotenvLoadingTests(unittest.TestCase):
    def test_candidate_dotenv_paths_prefers_cwd_env(self) -> None:
        module_file = "/tmp/fake-site-packages/agent/env.py"
        with patch("dra.env.find_dotenv", return_value="/Users/me/project/.env"):
            paths = candidate_dotenv_paths(module_file)

        self.assertEqual(paths[0], Path("/Users/me/project/.env"))
        self.assertEqual(
            paths[1],
            (Path(module_file).resolve().parent.parent / ".env").resolve(),
        )

    def test_candidate_dotenv_paths_deduplicates_package_env(self) -> None:
        module_file = "/Users/me/project/agent/env.py"
        with patch("dra.env.find_dotenv", return_value="/Users/me/project/.env"):
            paths = candidate_dotenv_paths(module_file)

        self.assertEqual(paths, [Path("/Users/me/project/.env")])

    def test_load_dotenv_for_loads_all_candidates_without_override(self) -> None:
        loader = MagicMock()
        with patch(
            "dra.env.candidate_dotenv_paths",
            return_value=[Path("/repo/.env"), Path("/fallback/.env")],
        ):
            with patch("dra.env.load_dotenv", loader):
                load_dotenv_for("/repo/agent/env.py")

        self.assertEqual(
            loader.call_args_list,
            [
                unittest.mock.call(Path("/repo/.env"), override=False),
                unittest.mock.call(Path("/fallback/.env"), override=False),
            ],
        )


if __name__ == "__main__":
    unittest.main()
