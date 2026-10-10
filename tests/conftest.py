from __future__ import annotations

import os
import shutil
import uuid
from pathlib import Path

import psycopg
import pytest

PG_DIR = Path(os.environ.get("SRDQ_PGTEST_DIR", "/tmp/srdq_pgtest"))


@pytest.fixture(scope="session")
def pg_server():
    import pgserver

    PG_DIR.mkdir(parents=True, exist_ok=True)
    srv = pgserver.get_server(str(PG_DIR), cleanup_mode="stop")
    yield srv


@pytest.fixture()
def pg_dsn(pg_server):
    """A fresh, migrated database per test."""
    name = "t_" + uuid.uuid4().hex[:12]
    base = pg_server.get_uri()
    with psycopg.connect(base, autocommit=True) as conn:
        conn.execute(f'CREATE DATABASE "{name}"')
    dsn = base.replace("/postgres?", f"/{name}?")
    from srdq.db.db import migrate

    migrate(dsn)
    yield dsn
    with psycopg.connect(base, autocommit=True) as conn:
        conn.execute(f'DROP DATABASE IF EXISTS "{name}" WITH (FORCE)')


@pytest.fixture()
def db(pg_dsn):
    from srdq.db.db import Database

    d = Database(pg_dsn)
    yield d
    d.close()


@pytest.fixture()
def tmp_dir(tmp_path):
    yield tmp_path
    shutil.rmtree(tmp_path, ignore_errors=True)
