"""Tests for the unified `rinnsal` CLI dispatcher."""

from __future__ import annotations

from pathlib import Path

import pytest

from rinnsal.cli.main import main


class TestDbCommands:
    def test_db_upgrade_creates_db(self, tmp_path):
        rc = main(["db", "upgrade", "--db-path", str(tmp_path / ".rinnsal")])
        assert rc == 0
        assert (tmp_path / ".rinnsal" / "metadata.sqlite").exists()

    def test_db_status_on_fresh(self, tmp_path, capsys):
        main(["db", "upgrade", "--db-path", str(tmp_path / ".rinnsal")])
        rc = main(["db", "status", "--db-path", str(tmp_path / ".rinnsal")])
        assert rc == 0
        out = capsys.readouterr().out
        assert "flows          0" in out
        assert "schema_rev    0001" in out

    def test_db_status_missing(self, tmp_path, capsys):
        rc = main(["db", "status", "--db-path", str(tmp_path / "nope")])
        assert rc == 1
        out = capsys.readouterr().out
        assert "no DB" in out

    def test_db_rebuild_with_yes(self, tmp_path):
        # First create a DB with content.
        from rinnsal.data.metadata import RunUpsert, SqliteMetadataStore

        db_dir = tmp_path / ".rinnsal"
        store = SqliteMetadataStore(db_dir / "metadata.sqlite")
        store.upsert_flow("f")
        store.upsert_run(
            RunUpsert(
                run_id="r1", flow_name="f", run_dir="/tmp/r1",
                status="success", started_at=100,
            )
        )
        assert len(store.list_flows()) == 1

        # Rebuild — should drop the DB and recreate empty.
        rc = main(["db", "rebuild", "--db-path", str(db_dir), "--yes"])
        assert rc == 0
        # Reset module-level engine cache so we re-open the freshly-created file.
        from rinnsal.data.metadata.sqlite import _engines
        _engines.clear()
        store2 = SqliteMetadataStore(db_dir / "metadata.sqlite")
        assert store2.list_flows() == []


class TestUpAcceptance:
    def test_up_help_does_not_crash(self, capsys):
        with pytest.raises(SystemExit) as excinfo:
            main(["up", "--help"])
        assert excinfo.value.code == 0
        out = capsys.readouterr().out
        assert "--host" in out
        assert "--debug" in out
