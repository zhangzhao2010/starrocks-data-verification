#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import datetime as dt
import itertools
import json
import math
import os
from pathlib import Path
import random
import re
import secrets
import sys
from collections import defaultdict
from dataclasses import dataclass
from decimal import Decimal
from typing import Any, Iterator

__version__ = "1.0.2"
DIFF_HEADER = ["run_id", "table_id", "rule_key", "rule", "diff_type", "partition", "key",
               "group", "metric", "column", "src", "tgt", "delta", "task_id", "detail"]
MODEL_ALIASES = {"PRI_KEYS": "PRIMARY_KEYS", "UNQ_KEYS": "UNIQUE_KEYS",
                 "DUP_KEYS": "DUPLICATE_KEYS", "AGG_KEYS": "AGGREGATE_KEYS"}
SESSION_NAME = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
RANGE_KEYS = re.compile(r"keys:\s*\[(.*?)\];")
LIST_GROUP = re.compile(r"\(([^()]*)\)")
LIST_VALUE = re.compile(r"'((?:[^'\\]|\\.)*)'|(-?\d+(?:\.\d+)?)")
TIME_RANGE = re.compile(r"(?:partition\s*)?\[\s*([^,\[\]]+?)\s*,\s*([^,\[\]]+?)\s*\)", re.I)
TIME_BOUND = re.compile(r"\d{4}-\d{2}-\d{2}(?:[ T]\d{2}:\d{2}:\d{2}(?:\.\d{1,6})?)?")


class CheckError(ValueError):
    pass


@dataclass(frozen=True)
class Settings:
    source: dict
    target: dict
    database: str
    table: str
    partition: str | None
    target_partition: str | None
    block_rows: int
    sample_rows: int
    seed: int | None
    connect_timeout_s: int
    query_timeout_s: int
    lookup_batch_rows: int
    output_dir: Path
    session: dict


def positive_int(value: Any, name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise CheckError(f"{name} 必须是正整数")
    return value


def mapping(value: Any, name: str) -> dict:
    if not isinstance(value, dict):
        raise CheckError(f"{name} 必须是 YAML 对象")
    return value


def required_name(raw: dict, key: str, context: str) -> str:
    value = raw.get(key)
    if not isinstance(value, str) or not value.strip() or "\x00" in value:
        raise CheckError(f"{context}.{key} 必须是非空字符串")
    return value


def parse_time_range(raw: str) -> tuple[dt.datetime, dt.datetime]:
    match = TIME_RANGE.fullmatch(raw.strip())
    if match is None:
        raise CheckError("分区必须是左闭右开时间范围，例如 partition[2026-09-29 00:00:00, 2026-09-30 00:00:00)")
    bounds = []
    for text in match.groups():
        text = text.strip()
        if len(text) >= 2 and text[0] == text[-1] and text[0] in {"'", '"'}:
            text = text[1:-1].strip()
        if not TIME_BOUND.fullmatch(text):
            raise CheckError(f"非法时间边界: {text!r}；请使用 YYYY-MM-DD 或 YYYY-MM-DD HH:MM:SS")
        try:
            bounds.append(dt.datetime.fromisoformat(text))
        except ValueError as exc:
            raise CheckError(f"非法时间边界: {text!r}") from exc
    if bounds[0] >= bounds[1]:
        raise CheckError("分区开始时间必须早于结束时间")
    return bounds[0], bounds[1]


def cluster_config(raw: Any, side: str, config_dir: Path) -> dict:
    raw = mapping(raw, side)
    host = required_name(raw, "host", side)
    user = required_name(raw, "user", side)
    if raw.get("password_env"):
        env = required_name(raw, "password_env", side)
        if env not in os.environ:
            raise CheckError(f"环境变量 {env} 未设置")
        password = os.environ[env]
    elif "password" in raw:
        password = str(raw["password"] if raw["password"] is not None else "")
    else:
        raise CheckError(f"{side} 需要 password 或 password_env")
    port = positive_int(raw.get("port", 9030), f"{side}.port")
    if port > 65535:
        raise CheckError(f"{side}.port 超出范围")
    ssl = raw.get("ssl", False)
    if not isinstance(ssl, bool):
        raise CheckError(f"{side}.ssl 必须是布尔值")
    ca = raw.get("ssl_ca")
    if ca is not None:
        ca = required_name(raw, "ssl_ca", side)
        ca = str((config_dir / ca).resolve())
    return {"host": host, "port": port, "user": user, "password": password, "ssl": ssl, "ssl_ca": ca}


def load_config(path: Path) -> Settings:
    import yaml

    try:
        raw = mapping(yaml.safe_load(path.read_text(encoding="utf-8")), "config")
    except (OSError, yaml.YAMLError) as exc:
        raise CheckError(f"无法读取配置 {path}: {exc}") from exc
    check = mapping(raw.get("check"), "check")
    sampling = mapping(raw.get("sampling", {}), "sampling")
    run = mapping(raw.get("run", {}), "run")
    session = mapping(raw.get("session", {}), "session")
    for key, value in session.items():
        if not isinstance(key, str) or not SESSION_NAME.fullmatch(key):
            raise CheckError(f"非法 session 变量名: {key!r}")
        if not isinstance(value, (str, int, float, bool)) or (isinstance(value, float) and not math.isfinite(value)):
            raise CheckError(f"session.{key} 必须是有限标量")
    for key in sampling:
        if key not in {"block_rows", "sample_rows", "seed"}:
            raise CheckError(f"未知采样配置 sampling.{key}")
    block_rows = positive_int(sampling.get("block_rows", 1000), "sampling.block_rows")
    sample_rows = positive_int(sampling.get("sample_rows", 10), "sampling.sample_rows")
    if sample_rows > block_rows:
        raise CheckError("sampling.sample_rows 不能大于 sampling.block_rows")
    seed = sampling.get("seed")
    if seed is not None and (isinstance(seed, bool) or not isinstance(seed, int)):
        raise CheckError("sampling.seed 必须是整数或 null")
    partition = check.get("partition")
    if partition is not None:
        partition = required_name(check, "partition", "check")
        parse_time_range(partition)
    target_partition = check.get("target_partition")
    if target_partition is not None:
        target_partition = required_name(check, "target_partition", "check")
        parse_time_range(target_partition)
        if partition is None:
            raise CheckError("check.partition 为 null 时，check.target_partition 也必须为 null")
    output = run.get("output_dir", "runs")
    if not isinstance(output, str) or not output:
        raise CheckError("run.output_dir 必须是非空路径")
    base = path.resolve().parent
    return Settings(
        source=cluster_config(raw.get("source"), "source", base),
        target=cluster_config(raw.get("target"), "target", base),
        database=required_name(check, "database", "check"),
        table=required_name(check, "table", "check"),
        partition=partition, target_partition=target_partition,
        block_rows=block_rows, sample_rows=sample_rows, seed=seed,
        connect_timeout_s=positive_int(run.get("connect_timeout_s", 10), "run.connect_timeout_s"),
        query_timeout_s=positive_int(run.get("query_timeout_s", 7200), "run.query_timeout_s"),
        lookup_batch_rows=positive_int(run.get("lookup_batch_rows", 200), "run.lookup_batch_rows"),
        output_dir=(base / output).resolve(), session=session,
    )


def qi(name: str) -> str:
    return "`" + name.replace("`", "``") + "`"


def table_sql(settings: Settings, partition: str | None) -> str:
    table = f"{qi(settings.database)}.{qi(settings.table)}"
    return table if partition is None else f"{table} PARTITION ({qi(partition)})"


class Connection:
    def __init__(self, cluster: dict, settings: Settings):
        import pymysql

        kwargs = {key: cluster[key] for key in ("host", "port", "user", "password")}
        kwargs.update(charset="utf8mb4", autocommit=True, connect_timeout=settings.connect_timeout_s,
                      read_timeout=settings.query_timeout_s + 60, write_timeout=settings.query_timeout_s + 60)
        if cluster["ssl"]:
            kwargs["ssl"] = {"check_hostname": True, "verify_mode": True}
            if cluster["ssl_ca"]:
                kwargs["ssl"]["ca"] = cluster["ssl_ca"]
        self.conn = pymysql.connect(**kwargs)
        try:
            self.query("SET query_timeout = %s", [settings.query_timeout_s])
            for name, value in settings.session.items():
                self.query(f"SET {qi(name)} = %s", [value])
        except BaseException:
            self.close()
            raise

    def query(self, sql: str, params: list | None = None) -> list[tuple]:
        with self.conn.cursor() as cursor:
            cursor.execute(sql, params)
            return list(cursor.fetchall()) if cursor.description else []

    def query_dicts(self, sql: str) -> list[dict]:
        import pymysql.cursors

        with self.conn.cursor(pymysql.cursors.DictCursor) as cursor:
            cursor.execute(sql)
            return list(cursor.fetchall())

    def stream(self, sql: str, batch_rows: int) -> Iterator[tuple]:
        import pymysql.cursors

        cursor = self.conn.cursor(pymysql.cursors.SSCursor)
        try:
            cursor.execute(sql)
            while rows := cursor.fetchmany(batch_rows):
                yield from rows
        finally:
            # Closing an unfinished SSCursor otherwise drains the entire partition.
            self.close()
            cursor.connection = None

    def close(self) -> None:
        if self.conn.open:
            self.conn.close()


def normalize_partition_value(raw: str) -> str:
    raw = (raw or "").strip()
    keys = RANGE_KEYS.findall(raw)
    if len(keys) == 2:
        return f"[{keys[0].strip()}, {keys[1].strip()})"
    if raw.startswith("(") and raw.endswith(")"):
        groups = LIST_GROUP.findall(raw[1:-1])
        values = [[a if a else b for a, b in LIST_VALUE.findall(group)] for group in groups]
        if not groups:
            values = [[a if a else b] for a, b in LIST_VALUE.findall(raw[1:-1])]
        if not values:
            return raw
    else:
        try:
            values = json.loads(raw)
        except (ValueError, TypeError):
            return raw
        if not isinstance(values, list):
            return raw
        values = [value if isinstance(value, list) else [value] for value in values]
    items = sorted(json.dumps(str(v[0]) if len(v) == 1 else [str(x) for x in v], ensure_ascii=False)
                   for v in values)
    return "IN (" + ", ".join(items) + ")"


def category(data_type: str) -> str:
    data_type = data_type.lower()
    if data_type in {"tinyint", "smallint", "int", "integer", "bigint", "largeint"}:
        return "int"
    if data_type.startswith("decimal"):
        return "decimal"
    if data_type in {"float", "double"}:
        return "float"
    if data_type in {"char", "varchar", "string", "text"}:
        return "string"
    if data_type in {"timestamp", "datetime"}:
        return "datetime"
    return data_type


def fetch_table_meta(conn: Connection, database: str, table: str) -> dict:
    import pymysql

    columns = conn.query(
        "SELECT COLUMN_NAME, DATA_TYPE, COLUMN_TYPE, IS_NULLABLE FROM information_schema.columns "
        "WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s ORDER BY ORDINAL_POSITION", [database, table])
    if not columns:
        raise CheckError(f"表不存在: {database}.{table}")
    config = conn.query(
        "SELECT TABLE_MODEL, PRIMARY_KEY, PARTITION_KEY, DISTRIBUTE_BUCKET "
        "FROM information_schema.tables_config WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s", [database, table])
    if not config:
        raise CheckError(f"无法获取表 Key 元数据: {database}.{table}")
    model = str(config[0][0] or "").upper()
    split_keys = lambda text: [key.strip().strip("`") for key in (text or "").split(",") if key.strip()]
    partition_columns = split_keys(config[0][2])
    rows = []
    if partition_columns:
        try:
            rows = conn.query(
                "SELECT PARTITION_NAME, PARTITION_VALUE, ROW_COUNT FROM information_schema.partitions_meta "
                "WHERE DB_NAME = %s AND TABLE_NAME = %s AND IS_TEMP = 0", [database, table])
        except pymysql.MySQLError:
            for part in conn.query_dicts(f"SHOW PARTITIONS FROM {qi(database)}.{qi(table)}"):
                value = next((str(part[key]).strip() for key in ("Range", "List", "ListValues")
                              if part.get(key) is not None and str(part[key]).strip() not in {"", "-"}), "")
                rows.append((part.get("PartitionName"), value, part.get("RowCount")))
    return {
        "model": MODEL_ALIASES.get(model, model), "keys": split_keys(config[0][1]),
        "partition_columns": partition_columns,
        "columns": [{"name": row[0], "type": str(row[2]).lower(), "category": category(str(row[1]))}
                    for row in columns],
        "partitions": [{"name": row[0], "value": normalize_partition_value(str(row[1] or ""))}
                       for row in rows if not str(row[0]).startswith("$")],
    }


def select_time_partition(meta: dict, selector: str, side: str) -> dict:
    bounds = parse_time_range(selector)
    columns = {col["name"].lower(): col for col in meta["columns"]}
    partition_columns = meta["partition_columns"]
    if len(partition_columns) != 1 or columns[partition_columns[0].lower()]["category"] not in {"date", "datetime"}:
        raise CheckError(f"{side} 必须按单个 DATE/DATETIME 列分区，才能按时间范围选择")
    candidates = []
    for part in meta["partitions"]:
        try:
            part_bounds = parse_time_range(part["value"])
        except CheckError:
            continue
        if part_bounds == bounds:
            candidates.append(part)
    if len(candidates) != 1:
        raise CheckError(f"{side} 时间范围 {selector} 未匹配到唯一分区（匹配 {len(candidates)} 个）；"
                         "时间范围必须与一个实际分区的左右边界完全一致，不支持子区间或跨分区范围")
    return candidates[0]


def resolve_metadata(settings: Settings, source: dict, target: dict) -> dict:
    for side, meta in (("source", source), ("target", target)):
        if meta["model"] not in {"PRIMARY_KEYS", "UNIQUE_KEYS"} or not meta["keys"]:
            raise CheckError(f"{side} 必须是有 Key 元数据的 PRIMARY/UNIQUE KEY 表，实际为 {meta['model']}")
    source_columns = {col["name"].lower(): col for col in source["columns"]}
    target_columns = {col["name"].lower(): col for col in target["columns"]}
    if source_columns.keys() != target_columns.keys():
        raise CheckError("两端列集合不同，无法逐列完整比较")
    if [key.lower() for key in source["keys"]] != [key.lower() for key in target["keys"]]:
        raise CheckError("两端完整 Key 定义不同，无法精确回查")
    for name, col in source_columns.items():
        if col["category"] != target_columns[name]["category"]:
            raise CheckError(f"列 {col['name']} 类型类别不同: {col['type']} / {target_columns[name]['type']}")
        if col["category"] in {"hll", "bitmap", "percentile", "object", "unknown"}:
            raise CheckError(f"列 {col['name']} 为不可直接比较的 {col['type']} 类型，不会静默忽略")
    if settings.partition is None:
        if settings.target_partition is not None:
            raise CheckError("check.partition 为 null 时，check.target_partition 也必须为 null")
        if source["partition_columns"] or target["partition_columns"]:
            raise CheckError("partition: null 仅适用于两端均未分区的表；分区表请填写完整时间范围，不会自动全表扫描")
        source_part = target_part = {"name": None, "value": "__ALL__"}
    else:
        for side, meta in (("source", source), ("target", target)):
            if not meta["partition_columns"]:
                raise CheckError(f"{side} 表未分区，不能指定时间分区；两端均未分区时请设置 partition: null")
        source_part = select_time_partition(source, settings.partition, "source")
        target_part = select_time_partition(target, settings.target_partition or settings.partition, "target")
    names = [col["name"] for col in source["columns"]]
    return {
        "source_partition": source_part["name"], "target_partition": target_part["name"],
        "source_value": source_part["value"], "target_value": target_part["value"],
        "source_columns": names, "target_columns": [target_columns[name.lower()]["name"] for name in names],
        "source_keys": [source_columns[key.lower()]["name"] for key in source["keys"]],
        "target_keys": [target_columns[key.lower()]["name"] for key in source["keys"]],
        "key_indexes": [next(i for i, name in enumerate(names) if name.lower() == key.lower())
                        for key in source["keys"]],
    }


def estimated_samples(rows: int, block_rows: int, sample_rows: int) -> int:
    full, tail = divmod(rows, block_rows)
    return full * sample_rows + (tail * sample_rows + block_rows - 1) // block_rows


def sample_blocks(rows: Iterator[tuple], block_rows: int, sample_rows: int, rng: random.Random) -> Iterator[tuple[int, list]]:
    iterator = iter(rows)
    while block := list(itertools.islice(iterator, block_rows)):
        count = (len(block) * sample_rows + block_rows - 1) // block_rows
        indexes = sorted(rng.sample(range(len(block)), count))
        yield len(block), [block[index] for index in indexes]


def sample_batches(rows: Iterator[tuple], settings: Settings, rng: random.Random) -> Iterator[tuple[int, list]]:
    scanned, selected = 0, []
    for size, keys in sample_blocks(rows, settings.block_rows, settings.sample_rows, rng):
        scanned += size
        selected.extend(keys)
        if len(selected) >= settings.lookup_batch_rows:
            yield scanned, selected
            scanned, selected = 0, []
    if selected:
        yield scanned, selected


def lookup_rows(conn: Connection, table: str, columns: list[str], key_names: list[str], keys: list[tuple],
                batch_rows: int) -> list[tuple]:
    result = []
    for start in range(0, len(keys), batch_rows):
        batch = keys[start:start + batch_rows]
        predicate = " OR ".join("(" + " AND ".join(f"{qi(name)} <=> %s" for name in key_names) + ")"
                                for _ in batch)
        sql = f"SELECT {', '.join(map(qi, columns))} FROM {table} WHERE {predicate}"
        result.extend(conn.query(sql, [value for key in batch for value in key]))
    return result


def json_value(value: Any) -> Any:
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, bytes):
        return {"hex": value.hex()}
    if isinstance(value, (dt.date, dt.datetime, dt.time)):
        return value.isoformat()
    if isinstance(value, dt.timedelta):
        return str(value)
    raise TypeError(f"无法序列化 {type(value).__name__}")


def dumps(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, default=json_value)


class DiffWriter:
    def __init__(self, path: Path, run_id: str, settings: Settings, resolved: dict):
        self.file = path.open("x", encoding="utf-8-sig", newline="")
        self.writer = csv.DictWriter(self.file, fieldnames=DIFF_HEADER)
        self.writer.writeheader()
        self.run_id = run_id
        self.table_id = f"{settings.database}.{settings.table}=>{settings.database}.{settings.table}"
        self.partition = resolved["source_value"] or resolved["source_partition"]
        self.partition_detail = {"source_partition": resolved["source_partition"],
                                 "target_partition": resolved["target_partition"]}
        self.count = 0

    def diff(self, rule: str, kind: str, task_id: str, **fields: Any) -> None:
        extra = {key: value for key, value in fields.items() if key not in DIFF_HEADER}
        row = {"run_id": self.run_id, "table_id": self.table_id,
               "rule_key": "0:ROW_COUNT" if rule == "ROW_COUNT" else "1:SAMPLE", "rule": rule,
               "diff_type": kind, "partition": self.partition, "task_id": task_id,
               **{key: value for key, value in fields.items() if key in DIFF_HEADER},
               "detail": {**self.partition_detail, **extra}}
        self.writer.writerow({key: "" if value is None else dumps(value) if isinstance(value, (dict, list, tuple, bytes))
                              else json_value(value) if isinstance(value, (dt.date, dt.time, dt.timedelta)) else value
                              for key, value in row.items()})
        self.count += 1

    def flush(self) -> None:
        self.file.flush()

    def close(self) -> None:
        self.file.close()


def compare_samples(keys: list[tuple], source_rows: list[tuple], target_rows: list[tuple], resolved: dict,
                    report: DiffWriter, task_id: str) -> int:
    groups = []
    for rows in (source_rows, target_rows):
        by_key = defaultdict(list)
        for row in rows:
            by_key[tuple(row[index] for index in resolved["key_indexes"])].append(row)
        groups.append(by_key)
    source, target = groups
    diff_keys = 0
    for key in keys:
        a, b = source.get(key, []), target.get(key, [])
        key_fields = {"key": key[0] if len(key) == 1 else ", ".join(map(str, key)),
                      "key_columns": resolved["source_keys"], "key_values": key}
        if not a:
            raise CheckError("已抽中的 Key 在源端回查时消失；数据可能正在变更，请稳定数据后重新运行")
        if len(a) > 1 or len(b) > 1:
            report.diff("SAMPLE", "SAMPLE_DUPLICATE_KEY", task_id, **key_fields, src=len(a), tgt=len(b))
            diff_keys += 1
        elif not b:
            report.diff("SAMPLE", "SAMPLE_KEY_SOURCE_ONLY", task_id, **key_fields,
                        values=dict(zip(resolved["source_columns"], a[0])))
            diff_keys += 1
        else:
            changed = False
            for column, va, vb in zip(resolved["source_columns"], a[0], b[0]):
                if va != vb:
                    changed = True
                    report.diff("SAMPLE", "SAMPLE_COLUMN_DIFF", task_id, **key_fields, column=column, src=va, tgt=vb)
            diff_keys += changed
    return diff_keys


def print_estimate(settings: Settings, resolved: dict, source_count: int, target_count: int, seed: int) -> None:
    samples = estimated_samples(source_count, settings.block_rows, settings.sample_rows)
    blocks = (source_count + settings.block_rows - 1) // settings.block_rows
    print(f"表: {settings.database}.{settings.table}")
    if resolved["source_partition"] is None:
        print("范围: 两端表均未分区，将按全表有序扫描主键并抽样对比。")
    else:
        print(f"源分区: {resolved['source_partition']}  范围: {resolved['source_value']}")
        print(f"目标分区: {resolved['target_partition']}  范围: {resolved['target_value']}")
    print(f"实际 COUNT(*): source={source_count:,}, target={target_count:,}, 差额={source_count - target_count:+,}")
    print(f"完整 Key: {', '.join(resolved['source_keys'])}; 比较全部 {len(resolved['source_columns'])} 列，零误差容忍")
    print(f"抽样: 每 {settings.block_rows:,} 行随机抽 {settings.sample_rows:,} 行；末批按比例向上取整")
    print(f"预计抽样阶段源端主键全量扫描 {source_count:,} 行，{blocks:,} 批，采集 {samples:,} 行")
    print(f"预计源端完整行回查 {samples:,} 行，目标端按完整 Key 回查最多 {samples:,} 行；实际内部扫描量取决于索引")
    print(f"随机种子: {seed}; 输出根目录: {settings.output_dir}")
    print("注意：COUNT 与抽样/回查不是跨集群一致快照，请在数据稳定时执行。")


def run(settings: Settings, connect=Connection, confirm=None) -> int:
    source = target = scan = report = None
    summary_path = None
    summary = {}
    try:
        source = connect(settings.source, settings)
        target = connect(settings.target, settings)
        resolved = resolve_metadata(settings, fetch_table_meta(source, settings.database, settings.table),
                                    fetch_table_meta(target, settings.database, settings.table))
        source_table = table_sql(settings, resolved["source_partition"])
        target_table = table_sql(settings, resolved["target_partition"])
        source_count = int(source.query(f"SELECT COUNT(*) FROM {source_table}")[0][0])
        target_count = int(target.query(f"SELECT COUNT(*) FROM {target_table}")[0][0])
        seed = settings.seed if settings.seed is not None else secrets.randbits(64)
        print_estimate(settings, resolved, source_count, target_count, seed)
        prompt = confirm if confirm is not None else input
        try:
            answer = prompt("确认开始扫描、抽样及对比？输入 yes 继续，其他输入取消: ")
        except EOFError:
            answer = ""
        if answer.strip().lower() not in {"yes", "y", "确认"}:
            print("已取消；未执行抽样，也未创建输出文件。")
            return 0
        run_id = dt.datetime.now().strftime("%Y%m%d-%H%M%S") + "-" + secrets.token_hex(4)
        out_dir = settings.output_dir / run_id
        out_dir.mkdir(parents=True, exist_ok=False)
        filename = re.sub(r"[^\w.-]", "_", settings.table) + "_diffs.csv"
        report = DiffWriter(out_dir / filename, run_id, settings, resolved)
        summary_path = out_dir / "summary.json"
        summary = {"run_id": run_id, "status": "RUNNING", "database": settings.database, "table": settings.table,
                   **resolved, "source_count": source_count, "target_count": target_count, "seed": seed,
                   "block_rows": settings.block_rows, "sample_rows_per_block": settings.sample_rows,
                   "estimated_samples": estimated_samples(source_count, settings.block_rows, settings.sample_rows),
                   "scanned_rows": 0, "sampled_rows": 0, "diff_keys": 0, "diff_records": 0}
        summary_path.write_text(dumps(summary) + "\n", encoding="utf-8")
        print(f"报告: {out_dir / filename}", flush=True)
        if source_count != target_count:
            report.diff("ROW_COUNT", "COUNT_DIFF", "row-count", src=source_count, tgt=target_count,
                        delta=source_count - target_count)
        if source_count:
            scan = connect(settings.source, settings)
            sql = f"SELECT {', '.join(map(qi, resolved['source_keys']))} FROM {source_table} "
            sql += f"ORDER BY {', '.join(map(qi, resolved['source_keys']))}"
            keys_stream = scan.stream(sql, settings.block_rows)
            try:
                for block_id, (size, keys) in enumerate(sample_batches(keys_stream, settings, random.Random(seed)), 1):
                    a = lookup_rows(source, source_table, resolved["source_columns"], resolved["source_keys"],
                                    keys, settings.lookup_batch_rows)
                    b = lookup_rows(target, target_table, resolved["target_columns"], resolved["target_keys"],
                                    keys, settings.lookup_batch_rows)
                    summary["diff_keys"] += compare_samples(keys, a, b, resolved, report, f"sample-{block_id}")
                    summary["scanned_rows"] += size
                    summary["sampled_rows"] += len(keys)
                    report.flush()
                    if block_id == 1 or block_id % 100 == 0:
                        print(f"已扫描 {summary['scanned_rows']:,} / 预计 {source_count:,} 行，"
                              f"已抽样 {summary['sampled_rows']:,} 行，差异 Key {summary['diff_keys']:,}", flush=True)
            finally:
                keys_stream.close()
            if summary["scanned_rows"] != source_count:
                raise CheckError("实际扫描行数与确认前 COUNT 不同；数据可能正在变更，请重新运行")
        summary["status"] = "DIFF" if report.count else "PASS"
        print(f"{summary['status']}: 扫描 {summary['scanned_rows']:,} 行，抽样 {summary['sampled_rows']:,} 行，"
              f"差异记录 {report.count:,} 条。")
        return 1 if report.count else 0
    except BaseException as exc:
        if summary_path is not None:
            summary["status"] = "INTERRUPTED" if isinstance(exc, KeyboardInterrupt) else "ERROR"
            summary["error"] = str(exc)
        raise
    finally:
        if report is not None:
            summary["diff_records"] = report.count
            report.close()
        if summary_path is not None:
            summary_path.write_text(dumps(summary) + "\n", encoding="utf-8")
        for conn in (scan, target, source):
            if conn is not None:
                conn.close()


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="StarRocks 时间分区或未分区表按完整 Key 分层随机抽样对比；先 COUNT 和估算，人工确认后才扫描。",
        epilog="分区表 check.partition 填写 partition[开始时间, 结束时间)，范围须与单个实际分区完整边界一致。"
               "两端均未分区时设置 partition: null 或省略，按全表抽样；不会自动扩大分区表扫描范围。"
               "相对 output_dir 基于配置目录。退出码: 0=通过或主动取消，1=存在差异，2=配置/执行错误，130=中断。")
    parser.add_argument("-c", "--config", type=Path, default=Path(__file__).resolve().parent / "config.yaml",
                        help="YAML 配置文件，默认工具目录下 config.yaml")
    parser.add_argument("--version", action="version", version=f"srdq-partition {__version__}")
    args = parser.parse_args(argv)
    try:
        return run(load_config(args.config))
    except KeyboardInterrupt:
        print("已中断；如已开始执行，输出目录中的 summary.json 会标记 INTERRUPTED。", file=sys.stderr)
        return 130
    except Exception as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    sys.exit(main())
