import json
import sys
from pathlib import Path
import tempfile
import unittest


PLUGIN_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PLUGIN_ROOT / "scripts"))

from store_config import ConfigError, StoreConfig, load_store_config, parse_store_config


class StoreConfigTests(unittest.TestCase):
    def test_missing_config_selects_portable_store(self):
        config = load_store_config(home=Path("/tmp/example-home"), environ={})

        self.assertEqual(
            StoreConfig("portable", Path("/tmp/example-home/store"), None),
            config,
        )

    def test_environment_home_takes_precedence(self):
        config = load_store_config(
            home=Path("/tmp/ignored-home"),
            environ={"WORKFLOW_OBSERVATORY_HOME": "/tmp/environment-home"},
        )

        self.assertEqual(Path("/tmp/environment-home/store"), config.root)

    def test_rejects_relative_explicit_home(self):
        with self.assertRaisesRegex(ConfigError, "observation home must be absolute"):
            load_store_config(home=Path("relative-home"), environ={})

    def test_rejects_relative_environment_home(self):
        with self.assertRaisesRegex(ConfigError, "observation home must be absolute"):
            load_store_config(
                home=Path("/tmp/ignored-home"),
                environ={"WORKFLOW_OBSERVATORY_HOME": "relative-home"},
            )

    def test_loads_explicit_portable_config(self):
        with tempfile.TemporaryDirectory() as directory:
            home = Path(directory)
            root = home / "records"
            (home / "config.json").write_text(
                json.dumps(
                    {
                        "schema_version": 1,
                        "adapter": "portable",
                        "root": str(root),
                    }
                ),
                encoding="utf-8",
            )

            self.assertEqual(
                StoreConfig("portable", root, None),
                load_store_config(home=home, environ={}),
            )

    def test_rejects_invalid_json_as_config_error(self):
        with tempfile.TemporaryDirectory() as directory:
            home = Path(directory)
            (home / "config.json").write_text("not json", encoding="utf-8")

            with self.assertRaisesRegex(ConfigError, "invalid config JSON"):
                load_store_config(home=home, environ={})

    def test_rejects_unknown_schema_version(self):
        with self.assertRaisesRegex(ConfigError, "schema_version must be 1"):
            parse_store_config(
                {"schema_version": 2, "adapter": "portable", "root": "/tmp/store"}
            )

    def test_rejects_unknown_keys(self):
        with self.assertRaisesRegex(ConfigError, "unknown config keys: extra"):
            parse_store_config(
                {
                    "schema_version": 1,
                    "adapter": "portable",
                    "root": "/tmp/store",
                    "extra": True,
                }
            )

    def test_rejects_non_string_config_keys(self):
        with self.assertRaisesRegex(ConfigError, "^config keys must be strings$"):
            parse_store_config(
                {
                    "schema_version": 1,
                    "adapter": "portable",
                    "root": "/tmp/store",
                    7: True,
                }
            )

    def test_rejects_unknown_adapter(self):
        with self.assertRaisesRegex(ConfigError, "unsupported adapter"):
            parse_store_config({"schema_version": 1, "adapter": "remote"})

    def test_rejects_non_string_adapter(self):
        with self.assertRaisesRegex(ConfigError, "unsupported adapter"):
            parse_store_config({"schema_version": 1, "adapter": ["portable"]})

    def test_rejects_relative_paths(self):
        with self.assertRaisesRegex(ConfigError, "root must be absolute"):
            parse_store_config(
                {"schema_version": 1, "adapter": "portable", "root": "store"}
            )

    def test_rejects_null_bytes_in_paths(self):
        with self.assertRaisesRegex(ConfigError, "root contains a null byte"):
            parse_store_config(
                {"schema_version": 1, "adapter": "portable", "root": "/tmp/a\0b"}
            )

    def test_llmwiki_requires_existing_cli(self):
        with self.assertRaisesRegex(ConfigError, "cli_path does not exist"):
            parse_store_config(
                {
                    "schema_version": 1,
                    "adapter": "llmwiki",
                    "cli_path": "/missing/wiki_cli.py",
                    "wiki_root": "/missing/wiki",
                }
            )

    def test_llmwiki_requires_existing_root(self):
        with tempfile.TemporaryDirectory() as directory:
            cli_path = Path(directory) / "wiki_cli.py"
            cli_path.write_text("", encoding="utf-8")

            with self.assertRaisesRegex(ConfigError, "wiki_root does not exist"):
                parse_store_config(
                    {
                        "schema_version": 1,
                        "adapter": "llmwiki",
                        "cli_path": str(cli_path),
                        "wiki_root": str(Path(directory) / "missing"),
                    }
                )

    def test_llmwiki_accepts_cli_within_existing_root(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory) / "wiki"
            root.mkdir()
            cli_path = root / "wiki_cli.py"
            cli_path.write_text("", encoding="utf-8")

            self.assertEqual(
                StoreConfig("llmwiki", root.resolve(), cli_path.resolve()),
                parse_store_config(
                    {
                        "schema_version": 1,
                        "adapter": "llmwiki",
                        "cli_path": str(cli_path),
                        "wiki_root": str(root),
                    }
                ),
            )

    def test_llmwiki_rejects_cli_symlink_escape(self):
        with tempfile.TemporaryDirectory() as directory:
            base = Path(directory)
            root = base / "wiki"
            root.mkdir()
            outside_cli = base / "outside_cli.py"
            outside_cli.write_text("", encoding="utf-8")
            cli_link = root / "wiki_cli.py"
            cli_link.symlink_to(outside_cli)

            with self.assertRaisesRegex(ConfigError, "cli_path escapes wiki_root"):
                parse_store_config(
                    {
                        "schema_version": 1,
                        "adapter": "llmwiki",
                        "cli_path": str(cli_link),
                        "wiki_root": str(root),
                    }
                )


if __name__ == "__main__":
    unittest.main()
