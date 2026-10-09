"""Pytest fixtures shared across tracker tests."""

from __future__ import annotations

import pytest

from lsst.pipe.base._runtime_analyzer.tracker import database as db_mod


@pytest.fixture
def db_path(tmp_path, monkeypatch):
    """Set up a temporary database path for tracker tests."""
    db = tmp_path / "tracker.db"
    monkeypatch.setattr(db_mod, "DEFAULT_DB_PATH", str(db))
    return str(db)
