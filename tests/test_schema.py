from __future__ import annotations

import pytest
import psycopg

from srdq.db.db import migrate, MigrationError


def test_32_tables(db):
    rows = db.query("SELECT table_name FROM information_schema.tables WHERE table_schema='srdq' "
                    "AND table_type='BASE TABLE'")
    names = {r["table_name"] for r in rows}
    assert len(names) == 32, sorted(names)
    assert all(n.startswith("dq_") for n in names)


def test_migrate_idempotent(pg_dsn):
    assert migrate(pg_dsn) == []


def test_outbox_append_only(db):
    with db.tx() as cur:
        cur.execute("INSERT INTO dq_outbox (event_type, object_type) VALUES ('X', 'Y')")
    with pytest.raises(psycopg.Error):
        with db.tx() as cur:
            cur.execute("UPDATE dq_outbox SET reason='z'")
    with pytest.raises(psycopg.Error):
        with db.tx() as cur:
            cur.execute("DELETE FROM dq_outbox")


def test_newer_schema_refused(pg_dsn):
    with psycopg.connect(pg_dsn, autocommit=True) as conn:
        conn.execute("INSERT INTO srdq_meta.migration VALUES (99, '0099_x.sql', 'x')")
    with pytest.raises(MigrationError):
        migrate(pg_dsn)
