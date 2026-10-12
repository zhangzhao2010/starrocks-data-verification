from __future__ import annotations

import hashlib
import uuid

import pytest
import psycopg
from psycopg import sql

from srdq.db.db import MIGRATIONS_DIR, migrate, MigrationError


def test_33_tables(db):
    rows = db.query("SELECT table_name FROM information_schema.tables WHERE table_schema='srdq' "
                    "AND table_type='BASE TABLE'")
    names = {r["table_name"] for r in rows}
    assert len(names) == 33, sorted(names)
    assert all(n.startswith("dq_") for n in names)


def test_migrate_idempotent(pg_dsn):
    assert migrate(pg_dsn) == []


def test_v13_asset_migrates_to_legacy_pair(pg_server):
    name = "t_upgrade_" + uuid.uuid4().hex[:12]
    base = pg_server.get_uri()
    dsn = base.replace("/postgres?", f"/{name}?")
    migration = MIGRATIONS_DIR / "0001_init.sql"
    source = migration.read_text(encoding="utf-8")
    try:
        with psycopg.connect(base, autocommit=True) as conn:
            conn.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(name)))
        with psycopg.connect(dsn) as conn:
            conn.execute(source)
            conn.execute("CREATE SCHEMA srdq_meta")
            conn.execute("""CREATE TABLE srdq_meta.migration (
                version int PRIMARY KEY, name text NOT NULL, digest text NOT NULL,
                applied_at timestamptz NOT NULL DEFAULT now())""")
            conn.execute("INSERT INTO srdq_meta.migration (version, name, digest) VALUES (1, %s, %s)",
                         (migration.name, hashlib.sha256(source.encode()).hexdigest()))
            conn.execute("""INSERT INTO srdq.dq_source (id, side, role, connection_ref) VALUES
                (%s, 'huawei', 'SOURCE', 'sr-huawei-ro'),
                (%s, 'aliyun', 'TARGET', 'sr-aliyun-ro')""", (uuid.uuid4(), uuid.uuid4()))
            asset_id = uuid.uuid4()
            conn.execute("""INSERT INTO srdq.dq_asset
                (id, layer, source_db, source_table, target_db, target_table)
                VALUES (%s, 'ODS', 'source_db', 'orders', 'target_db', 'orders_copy')""", (asset_id,))
        assert migrate(dsn) == ["0002_multi_cluster.sql"]
        with psycopg.connect(dsn) as conn:
            asset = conn.execute("SELECT pair_id FROM srdq.dq_asset WHERE id=%s", (asset_id,)).fetchone()
            pair = conn.execute("""SELECT source_cluster_id, target_cluster_id
                                 FROM srdq.dq_cluster_pair WHERE id='legacy'""").fetchone()
            assert asset == ("legacy",)
            assert pair == ("huawei", "aliyun")
    finally:
        with psycopg.connect(base, autocommit=True) as conn:
            conn.execute(sql.SQL("DROP DATABASE IF EXISTS {} WITH (FORCE)").format(sql.Identifier(name)))


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
