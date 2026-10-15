from __future__ import annotations

import csv
import datetime as dt
import io
import json
import random
import sys
import tempfile
import unittest
from dataclasses import replace
from decimal import Decimal
from pathlib import Path
from unittest.mock import Mock, patch
from contextlib import redirect_stdout

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import partition_check as app


def settings(directory: Path, **changes) -> app.Settings:
    cluster = {"host": "localhost", "port": 9030, "user": "reader", "password": "secret",
               "ssl": False, "ssl_ca": None}
    defaults = app.Settings(cluster, dict(cluster), "db", "table_1",
                            "partition[2026-09-29 00:00:00, 2026-09-30 00:00:00)", None,
                            1000, 10, 42, 10, 600, 200, directory / "runs", {})
    return replace(defaults, **changes)


def metadata(partition="p_source", value="[2026-09-29 00:00:00, 2026-09-30 00:00:00)", **changes):
    result = {"model": "PRIMARY_KEYS", "keys": ["id"], "partition_columns": ["value"],
              "columns": [{"name": "id", "type": "bigint", "category": "int"},
                          {"name": "value", "type": "datetime", "category": "datetime"}],
              "partitions": [{"name": partition, "value": value}]}
    return {**result, **changes}


class SamplingTests(unittest.TestCase):
    def test_counts_and_tail_round_up(self):
        for rows, expected in [(0, 0), (1, 1), (99, 1), (100, 1), (101, 2), (999, 10),
                               (1000, 10), (1001, 11), (2501, 26)]:
            with self.subTest(rows=rows):
                blocks = list(app.sample_blocks(iter((i,) for i in range(rows)), 1000, 10, random.Random(42)))
                self.assertEqual(sum(len(keys) for _, keys in blocks), expected)
                self.assertEqual(sum(size for size, _ in blocks), rows)
                self.assertEqual(app.estimated_samples(rows, 1000, 10), expected)

    def test_each_ordered_block_is_sampled_without_replacement(self):
        blocks = list(app.sample_blocks(iter((i,) for i in range(5000)), 1000, 10, random.Random(3)))
        for index, (size, keys) in enumerate(blocks):
            self.assertEqual(size, 1000)
            self.assertEqual(len(set(keys)), 10)
            self.assertEqual(keys, sorted(keys))
            self.assertTrue(all(index * 1000 <= key[0] < (index + 1) * 1000 for key in keys))

    def test_seed_reproduces_and_changes_samples(self):
        def sample(seed):
            return list(app.sample_blocks(iter((i,) for i in range(3000)), 1000, 10, random.Random(seed)))
        self.assertEqual(sample(1), sample(1))
        self.assertNotEqual(sample(1), sample(2))

    def test_custom_sampling_and_full_sampling(self):
        blocks = list(app.sample_blocks(iter((i,) for i in range(20)), 7, 2, random.Random(1)))
        self.assertEqual(sum(len(keys) for _, keys in blocks), 6)
        blocks = list(app.sample_blocks(iter((i,) for i in range(20)), 7, 7, random.Random(1)))
        self.assertEqual([key[0] for _, keys in blocks for key in keys], list(range(20)))

    def test_lookup_batching_preserves_stratification(self):
        batches = list(app.sample_batches(iter((i,) for i in range(20123)), settings(Path(".")), random.Random(42)))
        self.assertEqual([(size, len(keys)) for size, keys in batches], [(20000, 200), (123, 2)])
        sampled = [key[0] for _, keys in batches for key in keys]
        for block in range(20):
            self.assertEqual(sum(block * 1000 <= key < (block + 1) * 1000 for key in sampled), 10)


class ConfigTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.path = Path(self.temp.name) / "config.yaml"
        self.raw = {"source": {"host": "source", "user": "reader", "password": ""},
                    "target": {"host": "target", "user": "reader", "password": "pw"},
                    "check": {"database": "db", "table": "table_1",
                              "partition": "partition[2026-09-29 00:00:00, 2026-09-30 00:00:00)"}}

    def load(self):
        import yaml
        self.path.write_text(yaml.safe_dump(self.raw), encoding="utf-8")
        return app.load_config(self.path)

    def test_defaults_and_relative_output(self):
        config = self.load()
        self.assertEqual((config.block_rows, config.sample_rows), (1000, 10))
        self.assertIsNone(config.seed)
        self.assertIsNone(config.target_partition)
        self.assertEqual(config.output_dir, self.path.parent / "runs")
        self.assertEqual(config.source["password"], "")

    def test_environment_password_and_ssl_ca(self):
        self.raw["source"].update(password_env="TEST_SR_PASSWORD", ssl=True, ssl_ca="certs/ca.pem")
        with patch.dict("os.environ", {"TEST_SR_PASSWORD": "env-secret"}):
            config = self.load()
        self.assertEqual(config.source["password"], "env-secret")
        self.assertEqual(config.source["ssl_ca"], str(self.path.parent / "certs/ca.pem"))

    def test_missing_environment_password(self):
        self.raw["source"]["password_env"] = "MISSING_TEST_SR_PASSWORD"
        with patch.dict("os.environ", {}, clear=True), self.assertRaises(app.CheckError):
            self.load()

    def test_sampling_validation(self):
        for sampling in [{"block_rows": 0}, {"sample_rows": 0}, {"block_rows": True},
                         {"sample_rows": 1001}, {"seed": False}, {"seed": "abc"}, {"ratio": 0.01}]:
            with self.subTest(sampling=sampling):
                self.raw["sampling"] = sampling
                with self.assertRaises(app.CheckError):
                    self.load()

    def test_session_and_ssl_validation(self):
        for session in [{"x; DROP TABLE t": 1}, {"time_zone": None}, {"query_timeout": float("inf")}]:
            with self.subTest(session=session):
                self.raw["session"] = session
                with self.assertRaises(app.CheckError):
                    self.load()
        self.raw.pop("session")
        self.raw["source"]["ssl"] = "false"
        with self.assertRaises(app.CheckError):
            self.load()

    def test_invalid_config_and_required_fields(self):
        self.path.write_text("- not-a-mapping\n", encoding="utf-8")
        with self.assertRaises(app.CheckError):
            app.load_config(self.path)
        for key in ["database", "table"]:
            saved = self.raw["check"].pop(key)
            with self.assertRaises(app.CheckError):
                self.load()
            self.raw["check"][key] = saved

    def test_time_range_config_and_optional_target_range(self):
        self.raw["check"]["target_partition"] = "partition[2026-09-30, 2026-10-01)"
        config = self.load()
        self.assertEqual(config.partition, "partition[2026-09-29 00:00:00, 2026-09-30 00:00:00)")
        self.assertEqual(config.target_partition, "partition[2026-09-30, 2026-10-01)")
        for key in ("partition", "target_partition"):
            saved = self.raw["check"][key]
            for value in ("p20260929", "[2026-09-29, 2026-09-30]", "[2026-09-30, 2026-09-29)"):
                self.raw["check"][key] = value
                with self.subTest(key=key, value=value), self.assertRaises(app.CheckError):
                    self.load()
            self.raw["check"][key] = saved

    def test_null_or_omitted_partition_selects_unpartitioned_mode(self):
        self.raw["check"]["partition"] = None
        self.assertIsNone(self.load().partition)
        self.raw["check"].pop("partition")
        self.assertIsNone(self.load().partition)
        self.raw["check"]["target_partition"] = "[2026-09-29, 2026-09-30)"
        with self.assertRaises(app.CheckError):
            self.load()

    def test_blank_or_non_string_partition_is_rejected(self):
        for value in ("", " ", False, 0):
            self.raw["check"]["partition"] = value
            with self.subTest(value=value), self.assertRaises(app.CheckError):
                self.load()

    def test_existing_run_settings_do_not_change_sampling(self):
        self.raw["run"] = {"connect_timeout_s": 5, "query_timeout_s": 300, "concurrency": 4,
                           "retries": 2, "max_diffs_per_rule": 1}
        config = self.load()
        self.assertEqual(config.query_timeout_s, 300)
        self.assertEqual((config.block_rows, config.sample_rows), (1000, 10))


class TimeRangeTests(unittest.TestCase):
    def test_supported_time_range_formats(self):
        expected = (dt.datetime(2026, 9, 29), dt.datetime(2026, 9, 30))
        for text in ["partition[2026-09-29 00:00:00, 2026-09-30 00:00:00)",
                     "[2026-09-29, 2026-09-30)",
                     " PARTITION [ '2026-09-29', '2026-09-30' ) ",
                     '["2026-09-29T00:00:00", "2026-09-30T00:00:00")']:
            with self.subTest(text=text):
                self.assertEqual(app.parse_time_range(text), expected)

    def test_microseconds_are_preserved(self):
        bounds = app.parse_time_range("[2026-09-29 00:00:00.123456, 2026-09-30 00:00:00.654321)")
        self.assertEqual((bounds[0].microsecond, bounds[1].microsecond), (123456, 654321))

    def test_invalid_bounds_and_non_half_open_ranges_rejected(self):
        for text in ["p20260929", "partition[2026-09-29, 2026-09-30]", "(2026-09-29, 2026-09-30)",
                     "[2026-09-30, 2026-09-29)", "[2026-09-29, 2026-09-29)",
                     "[2026-02-30, 2026-03-01)", "[2026-09-29 25:00:00, 2026-09-30)",
                     "[2026-09-29T00:00:00+08:00, 2026-09-30)", "[2026-09-29,)",
                     "[2026-09-29, 2026-09-30); DROP TABLE t", "[0, 10000)"]:
            with self.subTest(text=text), self.assertRaises(app.CheckError):
                app.parse_time_range(text)


class MetadataTests(unittest.TestCase):
    def test_partition_matching_by_value_not_name(self):
        resolved = app.resolve_metadata(settings(Path(".")), metadata(), metadata("p_target"))
        self.assertEqual(resolved["target_partition"], "p_target")

    def test_same_name_with_different_value_does_not_match(self):
        with self.assertRaises(app.CheckError):
            app.resolve_metadata(settings(Path(".")), metadata(), metadata("p_source", "[2026-09-30 00:00:00, 2026-10-01 00:00:00)"))

    def test_time_range_finds_source_and_target_among_other_partitions(self):
        source = metadata(partitions=[{"name": "p_other", "value": "[2026-09-30, 2026-10-01)"},
                                      {"name": "source_987", "value": "['2026-09-29', '2026-09-30')"}])
        target = metadata(partitions=[{"name": "target_456", "value": "[2026-09-29T00:00:00, 2026-09-30T00:00:00)"},
                                      {"name": "p_other", "value": "[2026-09-30, 2026-10-01)"}])
        resolved = app.resolve_metadata(settings(Path(".")), source, target)
        self.assertEqual((resolved["source_partition"], resolved["target_partition"]), ("source_987", "target_456"))

    def test_date_partitions_match_midnight_datetime_input(self):
        columns = [metadata()["columns"][0], {"name": "value", "type": "date", "category": "date"}]
        source = metadata(value="[2026-09-29, 2026-09-30)", columns=columns)
        target = metadata("p_target", "[2026-09-29, 2026-09-30)", columns=columns)
        resolved = app.resolve_metadata(settings(Path(".")), source, target)
        self.assertEqual(resolved["target_partition"], "p_target")

    def test_missing_ambiguous_or_partial_source_range_is_rejected(self):
        for source in [metadata(partitions=[]), metadata(partitions=metadata()["partitions"] * 2)]:
            with self.subTest(source=source), self.assertRaisesRegex(app.CheckError, "source 时间范围"):
                app.resolve_metadata(settings(Path(".")), source, metadata("p_target"))
        for selector in ["[2026-09-29 12:00:00, 2026-09-30)", "[2026-09-29, 2026-10-01)"]:
            with self.subTest(selector=selector), self.assertRaisesRegex(app.CheckError, "完整.*|左右边界"):
                app.resolve_metadata(settings(Path("."), partition=selector), metadata(), metadata("p_target"))

    def test_non_time_or_multiple_partition_columns_are_rejected(self):
        for source in [metadata(partition_columns=["id"]), metadata(partition_columns=["id", "value"])]:
            with self.subTest(source=source), self.assertRaisesRegex(app.CheckError, "DATE/DATETIME"):
                app.resolve_metadata(settings(Path(".")), source, metadata("p_target"))

    def test_invalid_or_unbounded_metadata_does_not_match(self):
        source = metadata(partitions=[{"name": "min", "value": "[MINVALUE, 2026-09-29)"},
                                      {"name": "max", "value": "[2026-09-30, MAXVALUE)"},
                                      {"name": "bad", "value": ""}, *metadata()["partitions"]])
        resolved = app.resolve_metadata(settings(Path(".")), source, metadata("p_target"))
        self.assertEqual(resolved["source_partition"], "p_source")

    def test_explicit_target_time_range(self):
        config = settings(Path("."), target_partition="partition[2026-09-30 00:00:00, 2026-10-01 00:00:00)")
        resolved = app.resolve_metadata(config, metadata(), metadata("p_target", "[2026-09-30 00:00:00, 2026-10-01 00:00:00)"))
        self.assertEqual(resolved["target_value"], "[2026-09-30 00:00:00, 2026-10-01 00:00:00)")

    def test_missing_or_ambiguous_target_partition(self):
        for target in [metadata(partitions=[]), metadata(partitions=[{"name": "a", "value": "[2026-09-29 00:00:00, 2026-09-30 00:00:00)"},
                                                                    {"name": "b", "value": "[2026-09-29 00:00:00, 2026-09-30 00:00:00)"}])]:
            with self.subTest(target=target), self.assertRaises(app.CheckError):
                app.resolve_metadata(settings(Path(".")), metadata(), target)

    def test_column_order_and_case_alignment(self):
        target = metadata("p_target", columns=[{"name": "VALUE", "type": "datetime", "category": "datetime"},
                                               {"name": "ID", "type": "bigint", "category": "int"}], keys=["ID"])
        resolved = app.resolve_metadata(settings(Path(".")), metadata(), target)
        self.assertEqual(resolved["target_columns"], ["ID", "VALUE"])
        self.assertEqual(resolved["target_keys"], ["ID"])

    def test_unsupported_schemas_fail_without_silent_skipping(self):
        invalid = [metadata(model="DUPLICATE_KEYS"), metadata(keys=[]), metadata(keys=["value"]),
                   metadata(partition_columns=[]), metadata(columns=metadata()["columns"][:1]),
                   metadata(columns=[metadata()["columns"][0], {"name": "value", "type": "hll", "category": "hll"}])]
        for target in invalid:
            with self.subTest(target=target), self.assertRaises(app.CheckError):
                app.resolve_metadata(settings(Path(".")), metadata(), target)

    def test_unpartitioned_metadata_selects_whole_table_not_internal_partition(self):
        config = settings(Path("."), partition=None)
        source = metadata(partition_columns=[])
        target = metadata("internal_target_partition", partition_columns=[])
        resolved = app.resolve_metadata(config, source, target)
        self.assertIsNone(resolved["source_partition"])
        self.assertIsNone(resolved["target_partition"])
        self.assertEqual((resolved["source_value"], resolved["target_value"]), ("__ALL__", "__ALL__"))
        self.assertEqual(app.table_sql(config, None), "`db`.`table_1`")

    def test_null_partition_cannot_silently_scan_partitioned_or_mixed_tables(self):
        config = settings(Path("."), partition=None)
        for source, target in [(metadata(), metadata("p_target")),
                               (metadata(partition_columns=[]), metadata("p_target")),
                               (metadata(), metadata("p_target", partition_columns=[]))]:
            with self.subTest(source=source, target=target), self.assertRaisesRegex(app.CheckError, "不会自动全表扫描"):
                app.resolve_metadata(config, source, target)

    def test_time_range_on_unpartitioned_table_is_rejected(self):
        with self.assertRaisesRegex(app.CheckError, "partition: null"):
            app.resolve_metadata(settings(Path(".")), metadata(partition_columns=[]), metadata(partition_columns=[]))

    def test_unpartitioned_metadata_skips_all_partition_queries(self):
        for partition_key in (None, ""):
            conn = Mock()
            conn.query.side_effect = [[("id", "bigint", "bigint", "NO")],
                                      [("PRI_KEYS", "id", partition_key, 10)]]
            result = app.fetch_table_meta(conn, "db", "table_1")
            self.assertEqual(result["partition_columns"], [])
            self.assertEqual(result["partitions"], [])
            self.assertEqual(conn.query.call_count, 2)
            conn.query_dicts.assert_not_called()

    def test_range_and_list_normalization(self):
        raw_range = "[types: [DATETIME]; keys: [2026-09-29 00:00:00]; ..types: [DATETIME]; keys: [2026-09-30 00:00:00]; )"
        self.assertEqual(app.normalize_partition_value(raw_range), "[2026-09-29 00:00:00, 2026-09-30 00:00:00)")
        self.assertEqual(app.normalize_partition_value("(('b'), ('a'))"), app.normalize_partition_value('["a", "b"]'))
        self.assertEqual(app.normalize_partition_value("(('a', 1), ('b', 2))"),
                         app.normalize_partition_value('[["b", 2], ["a", 1]]'))

    def test_partitions_meta_fallback(self):
        import pymysql
        conn = Mock()
        conn.query.side_effect = [[("id", "bigint", "bigint", "NO")],
                                  [("PRI_KEYS", "id", "id", 10)], pymysql.ProgrammingError(1146, "missing")]
        conn.query_dicts.return_value = [{"PartitionName": "p1", "Range": "[2026-09-29 00:00:00, 2026-09-30 00:00:00)", "RowCount": 5},
                                        {"PartitionName": "$shadow", "Range": "[2026-09-29 00:00:00, 2026-09-30 00:00:00)", "RowCount": 0}]
        result = app.fetch_table_meta(conn, "db", "table_1")
        self.assertEqual(result["model"], "PRIMARY_KEYS")
        self.assertEqual(result["partitions"], [{"name": "p1", "value": "[2026-09-29 00:00:00, 2026-09-30 00:00:00)"}])


class LookupAndComparisonTests(unittest.TestCase):
    def test_exact_composite_and_null_key_parameterization(self):
        conn = Mock()
        conn.query.return_value = []
        keys = [(1, "x' OR 1=1"), (None, "b"), (3, "c")]
        app.lookup_rows(conn, "`db`.`t` PARTITION (`p`)", ["id", "na`me"], ["id", "na`me"], keys, 2)
        self.assertEqual(conn.query.call_count, 2)
        sql, params = conn.query.call_args_list[0].args
        self.assertNotIn("x' OR 1=1", sql)
        self.assertIn("`na``me` <=> %s", sql)
        self.assertEqual(params, [1, "x' OR 1=1", None, "b"])
        self.assertEqual(conn.query.call_args_list[1].args[1], [3, "c"])

    def test_csv_format_and_exact_values_without_truncation(self):
        with tempfile.TemporaryDirectory() as directory:
            config = settings(Path(directory))
            resolved = app.resolve_metadata(config, metadata(), metadata("p_target"))
            path = Path(directory) / "table_1_diffs.csv"
            report = app.DiffWriter(path, "test-run", config, resolved)
            source = [(1, "x" * 1000), (2, Decimal("1.0000000000001")), (3, None)]
            target = [(1, "y" * 1000), (2, Decimal("1")), (3, ""), (999, "unselected")]
            self.assertEqual(app.compare_samples([(1,), (2,), (3,)], source, target, resolved, report, "sample-1"), 3)
            report.close()
            self.assertTrue(path.read_bytes().startswith(b"\xef\xbb\xbf"))
            with path.open(encoding="utf-8-sig", newline="") as file:
                reader = csv.DictReader(file)
                rows = list(reader)
                self.assertEqual(reader.fieldnames, app.DIFF_HEADER)
            self.assertEqual(len(rows), 3)
            self.assertEqual(rows[0]["src"], "x" * 1000)
            self.assertEqual(rows[1]["src"], "1.0000000000001")
            self.assertEqual(json.loads(rows[2]["detail"])["target_partition"], "p_target")

    def test_missing_and_duplicate_target_keys(self):
        with tempfile.TemporaryDirectory() as directory:
            config = settings(Path(directory))
            resolved = app.resolve_metadata(config, metadata(), metadata("p_target"))
            path = Path(directory) / "diff.csv"
            report = app.DiffWriter(path, "test", config, resolved)
            self.assertEqual(app.compare_samples([(1,), (2,)], [(1, b"\x00\xff"), (2, "v")],
                                                 [(2, "v"), (2, "v")], resolved, report, "sample-1"), 2)
            report.close()
            with path.open(encoding="utf-8-sig", newline="") as file:
                rows = list(csv.DictReader(file))
            self.assertEqual([row["diff_type"] for row in rows], ["SAMPLE_KEY_SOURCE_ONLY", "SAMPLE_DUPLICATE_KEY"])
            self.assertEqual(json.loads(rows[0]["detail"])["values"]["value"], {"hex": "00ff"})

    def test_source_key_disappearance_is_error(self):
        report = Mock()
        with self.assertRaises(app.CheckError):
            app.compare_samples([(1,)], [], [(1, "v")], {"key_indexes": [0], "source_keys": ["id"]}, report, "s")

    def test_composite_keys_match_exactly(self):
        report = Mock()
        resolved = {"key_indexes": [0, 1], "source_keys": ["a", "b"], "source_columns": ["a", "b", "value"]}
        count = app.compare_samples([(1, None), (1, 2)], [(1, None, "x"), (1, 2, "v")],
                                    [(1, 3, "extra"), (1, None, "x"), (1, 2, "changed")], resolved, report, "s")
        self.assertEqual(count, 1)
        self.assertEqual(report.diff.call_args.kwargs["key_values"], (1, 2))

    def test_native_date_decimal_binary_and_zero_float_tolerance(self):
        report = Mock()
        resolved = {"key_indexes": [0], "source_keys": ["id"], "source_columns": ["id", "d", "n", "b", "f"]}
        a = [(1, dt.datetime(2026, 10, 15, 0, 0, 0, 123), Decimal("1.00"), b"bytes", 1.0)]
        b = [(1, dt.datetime(2026, 10, 15, 0, 0, 0, 123), Decimal("1"), b"bytes", 1.0 + 1e-12)]
        self.assertEqual(app.compare_samples([(1,)], a, b, resolved, report, "s"), 1)
        self.assertEqual(report.diff.call_args.kwargs["column"], "f")

    def test_tls_checks_certificate_and_hostname(self):
        config = settings(Path("."))
        cluster = {**config.source, "ssl": True, "ssl_ca": "/tmp/ca.pem"}
        with patch("pymysql.connect") as connect, patch.object(app.Connection, "query"):
            conn = app.Connection(cluster, config)
            conn.close()
        self.assertEqual(connect.call_args.kwargs["ssl"],
                         {"check_hostname": True, "verify_mode": True, "ca": "/tmp/ca.pem"})

    def test_unfinished_stream_does_not_drain_cursor(self):
        conn = app.Connection.__new__(app.Connection)
        conn.conn = Mock(open=True)
        cursor = Mock()
        cursor.fetchmany.return_value = [(1,), (2,)]
        conn.conn.cursor.return_value = cursor
        iterator = conn.stream("SELECT id FROM t ORDER BY id", 1000)
        self.assertEqual(next(iterator), (1,))
        iterator.close()
        conn.conn.close.assert_called_once()
        self.assertIsNone(cursor.connection)
        cursor.close.assert_not_called()


class FakeConnection:
    def __init__(self, factory, side):
        self.factory = factory
        self.side = side
        self.closed = False

    def query(self, sql, params=None):
        self.factory.events.append((self.side, "query", sql, params))
        if "information_schema.columns" in sql:
            return [("id", "bigint", "bigint", "NO"), ("value", "datetime", "datetime", "YES")]
        if "information_schema.tables_config" in sql:
            partitioned = self.factory.source_partitioned if self.side == "source" else self.factory.target_partitioned
            return [("PRI_KEYS", "id", "value" if partitioned else "", 10)]
        if "information_schema.partitions_meta" in sql:
            value = self.factory.source_partition_value if self.side == "source" else self.factory.target_partition_value
            return [("p_source" if self.side == "source" else "p_target", value, 0)]
        rows = self.factory.source if self.side == "source" else self.factory.target
        if "COUNT(*)" in sql:
            return [(self.factory.source_count if self.side == "source" else len(rows),)]
        if self.side == "source" and self.factory.source_missing:
            return []
        return [row for row in rows if row[0] in params]

    def stream(self, sql, batch_rows):
        self.factory.events.append((self.side, "stream", sql, batch_rows))
        if self.factory.interrupt:
            raise KeyboardInterrupt()
        for row in sorted(self.factory.source):
            yield (row[0],)

    def close(self):
        self.closed = True


class FakeFactory:
    def __init__(self, rows=2501, partitioned=True):
        self.source = [(i, f"value-{i}") for i in range(rows)]
        self.target = list(self.source)
        self.source_count = rows
        self.source_partitioned = partitioned
        self.target_partitioned = partitioned
        self.source_partition_value = "[2026-09-29 00:00:00, 2026-09-30 00:00:00)"
        self.target_partition_value = self.source_partition_value
        self.source_missing = False
        self.interrupt = False
        self.events = []
        self.connections = []

    def __call__(self, cluster, config):
        conn = FakeConnection(self, "target" if len(self.connections) == 1 else "source")
        self.connections.append(conn)
        return conn


class RunTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.directory = Path(self.temp.name)
        self.config = settings(self.directory)
        self.factory = FakeFactory()

    def execute(self, answer="yes"):
        with redirect_stdout(io.StringIO()):
            return app.run(self.config, connect=self.factory, confirm=lambda _: answer)

    def report(self):
        paths = list(self.config.output_dir.glob("*/table_1_diffs.csv"))
        self.assertEqual(len(paths), 1)
        with paths[0].open(encoding="utf-8-sig", newline="") as file:
            rows = list(csv.DictReader(file))
        return rows, json.loads((paths[0].parent / "summary.json").read_text())

    def test_confirmation_occurs_after_counts_before_any_sampling(self):
        def confirm(_):
            self.assertEqual(sum("COUNT(*)" in e[2] for e in self.factory.events), 2)
            self.assertFalse(any(e[1] == "stream" for e in self.factory.events))
            self.assertEqual(len(self.factory.connections), 2)
            self.assertFalse(self.config.output_dir.exists())
            return "no"
        with redirect_stdout(io.StringIO()):
            self.assertEqual(app.run(self.config, connect=self.factory, confirm=confirm), 0)
        self.assertFalse(self.config.output_dir.exists())
        self.assertTrue(all(conn.closed for conn in self.factory.connections))

    def test_eof_cancels_without_output(self):
        with redirect_stdout(io.StringIO()):
            self.assertEqual(app.run(self.config, connect=self.factory, confirm=Mock(side_effect=EOFError)), 0)
        self.assertFalse(self.config.output_dir.exists())

    def test_golden_path_and_different_partition_names(self):
        self.assertEqual(self.execute(), 0)
        rows, summary = self.report()
        self.assertEqual(rows, [])
        self.assertEqual(summary["status"], "PASS")
        self.assertEqual(summary["scanned_rows"], 2501)
        self.assertEqual(summary["sampled_rows"], 26)
        self.assertEqual(summary["estimated_samples"], 26)
        self.assertEqual(summary["target_partition"], "p_target")
        stream = next(e for e in self.factory.events if e[1] == "stream")
        self.assertIn("ORDER BY `id`", stream[2])
        self.assertIn("PARTITION (`p_source`)", stream[2])
        self.assertNotIn("value", stream[2])
        self.assertTrue(all(conn.closed for conn in self.factory.connections))
        self.assertNotIn("secret", json.dumps(summary))

    def test_time_range_with_different_metadata_formats(self):
        self.factory.source_partition_value = (
            "[types: [DATETIME]; keys: [2026-09-29 00:00:00]; "
            "..types: [DATETIME]; keys: [2026-09-30 00:00:00]; )")
        self.factory.target_partition_value = "['2026-09-29', '2026-09-30')"
        self.assertEqual(self.execute(), 0)
        _, summary = self.report()
        self.assertEqual((summary["source_partition"], summary["target_partition"]), ("p_source", "p_target"))
        for event in self.factory.events:
            if event[1] == "query" and "COUNT(*)" in event[2]:
                self.assertIn("PARTITION (`p_source`)" if event[0] == "source" else "PARTITION (`p_target`)", event[2])
                self.assertNotIn("2026-09-29", event[2])

    def test_unmatched_time_range_stops_before_count_or_confirmation(self):
        self.config = replace(self.config, partition="partition[2026-09-30, 2026-10-01)")
        confirm = Mock(return_value="yes")
        with redirect_stdout(io.StringIO()), self.assertRaises(app.CheckError):
            app.run(self.config, connect=self.factory, confirm=confirm)
        confirm.assert_not_called()
        self.assertFalse(any("COUNT(*)" in event[2] or event[1] == "stream" for event in self.factory.events))
        self.assertFalse(self.config.output_dir.exists())
        self.assertTrue(all(conn.closed for conn in self.factory.connections))

    def test_count_diff_missing_key_and_column_diff(self):
        blocks = list(app.sample_blocks(iter((i,) for i in range(2501)), 1000, 10, random.Random(42)))
        first, second = [key[0] for _, keys in blocks for key in keys][:2]
        self.factory.target = [(i, "changed" if i == second else value) for i, value in self.factory.target if i != first]
        self.assertEqual(self.execute(), 1)
        rows, summary = self.report()
        self.assertEqual([row["diff_type"] for row in rows], ["COUNT_DIFF", "SAMPLE_KEY_SOURCE_ONLY", "SAMPLE_COLUMN_DIFF"])
        self.assertEqual(summary["diff_keys"], 2)
        self.assertEqual(summary["status"], "DIFF")

    def test_empty_partition_still_writes_header_after_confirmation(self):
        self.factory = FakeFactory(0)
        self.assertEqual(self.execute(), 0)
        rows, summary = self.report()
        self.assertEqual(rows, [])
        self.assertEqual(summary["scanned_rows"], 0)
        self.assertEqual(len(self.factory.connections), 2)

    def test_target_extra_rows_only_affect_count(self):
        self.factory.target.append((10000, "extra"))
        self.assertEqual(self.execute(), 1)
        rows, summary = self.report()
        self.assertEqual([row["diff_type"] for row in rows], ["COUNT_DIFF"])
        self.assertEqual(summary["diff_keys"], 0)

    def test_scan_count_change_marks_report_error(self):
        self.factory.source_count = 2502
        with self.assertRaises(app.CheckError):
            self.execute()
        _, summary = self.report()
        self.assertEqual(summary["status"], "ERROR")
        self.assertTrue(all(conn.closed for conn in self.factory.connections))

    def test_source_lookup_disappearance_marks_error(self):
        self.factory.source_missing = True
        with self.assertRaises(app.CheckError):
            self.execute()
        _, summary = self.report()
        self.assertEqual(summary["status"], "ERROR")
        self.assertTrue(all(conn.closed for conn in self.factory.connections))

    def test_interruption_marks_incomplete_output(self):
        self.factory.interrupt = True
        with self.assertRaises(KeyboardInterrupt):
            self.execute()
        _, summary = self.report()
        self.assertEqual(summary["status"], "INTERRUPTED")
        self.assertTrue(all(conn.closed for conn in self.factory.connections))

    def test_each_run_keeps_previous_report(self):
        self.assertEqual(self.execute(), 0)
        self.factory = FakeFactory()
        self.assertEqual(self.execute(), 0)
        self.assertEqual(len(list(self.config.output_dir.glob("*/table_1_diffs.csv"))), 2)

    def test_unpartitioned_golden_path_uses_whole_table_sql(self):
        self.config = replace(self.config, partition=None)
        self.factory = FakeFactory(partitioned=False)
        output = io.StringIO()
        with redirect_stdout(output):
            result = app.run(self.config, connect=self.factory, confirm=lambda _: "yes")
        self.assertEqual(result, 0)
        self.assertIn("两端表均未分区", output.getvalue())
        self.assertIn("全表", output.getvalue())
        rows, summary = self.report()
        self.assertEqual(rows, [])
        self.assertEqual(summary["status"], "PASS")
        self.assertEqual(summary["scanned_rows"], 2501)
        self.assertEqual(summary["sampled_rows"], 26)
        self.assertEqual(summary["source_value"], "__ALL__")
        self.assertIsNone(summary["source_partition"])
        self.assertIsNone(summary["target_partition"])
        for event in self.factory.events:
            self.assertNotIn("information_schema.partitions_meta", event[2])
            self.assertNotIn(" PARTITION ", event[2])
        self.assertEqual(sum("COUNT(*)" in event[2] for event in self.factory.events), 2)
        self.assertTrue(any(event[1] == "stream" and "ORDER BY `id`" in event[2] for event in self.factory.events))
        self.assertTrue(all(conn.closed for conn in self.factory.connections))

    def test_unpartitioned_count_and_column_differences_use_whole_table_marker(self):
        self.config = replace(self.config, partition=None)
        self.factory = FakeFactory(rows=1, partitioned=False)
        self.factory.target = [(0, "changed"), (2, "target-only")]
        self.assertEqual(self.execute(), 1)
        rows, summary = self.report()
        self.assertEqual([row["diff_type"] for row in rows], ["COUNT_DIFF", "SAMPLE_COLUMN_DIFF"])
        self.assertTrue(all(row["partition"] == "__ALL__" for row in rows))
        self.assertTrue(all(json.loads(row["detail"])["source_partition"] is None for row in rows))
        self.assertEqual(summary["diff_keys"], 1)

    def test_unpartitioned_empty_table_keeps_csv_header(self):
        self.config = replace(self.config, partition=None)
        self.factory = FakeFactory(rows=0, partitioned=False)
        self.assertEqual(self.execute(), 0)
        rows, summary = self.report()
        self.assertEqual(rows, [])
        self.assertEqual(summary["sampled_rows"], 0)
        self.assertFalse(any(event[1] == "stream" for event in self.factory.events))

    def test_unpartitioned_cancel_after_counts_does_not_scan(self):
        self.config = replace(self.config, partition=None)
        self.factory = FakeFactory(partitioned=False)
        def confirm(_):
            self.assertEqual(sum("COUNT(*)" in event[2] for event in self.factory.events), 2)
            self.assertFalse(any(event[1] == "stream" for event in self.factory.events))
            return "no"
        with redirect_stdout(io.StringIO()):
            self.assertEqual(app.run(self.config, connect=self.factory, confirm=confirm), 0)
        self.assertFalse(self.config.output_dir.exists())
        self.assertFalse(any(event[1] == "stream" for event in self.factory.events))
        self.assertTrue(all(conn.closed for conn in self.factory.connections))

    def test_null_partition_on_partitioned_table_fails_before_counts(self):
        self.config = replace(self.config, partition=None)
        confirm = Mock(return_value="yes")
        with redirect_stdout(io.StringIO()), self.assertRaises(app.CheckError):
            app.run(self.config, connect=self.factory, confirm=confirm)
        confirm.assert_not_called()
        self.assertFalse(any("COUNT(*)" in event[2] or event[1] == "stream" for event in self.factory.events))
        self.assertFalse(self.config.output_dir.exists())


if __name__ == "__main__":
    unittest.main()
