# SRDQ Lite

面向 StarRocks 3.5 的单进程、离线数据核对工具。一个进程只核对一对 source/target 集群；默认覆盖全部分区，支持断点续跑。

## 环境

需要 Python 3.12，以及两个离线依赖：`PyMySQL`、`PyYAML`。

```bash
python3 -m venv .venv
.venv/bin/pip install -r srdq-lite/requirements.txt
```

### 完整离线包

仓库中执行以下命令可构建 Linux x86_64 离线包：

```bash
srdq-lite/scripts/build-offline-bundle.sh
```

上传 `srdq-lite/dist/srdq-lite-offline-*.tar.gz` 和对应 `.sha256` 后，在服务器执行：

```bash
sha256sum -c srdq-lite-offline-*.tar.gz.sha256
tar -xzf srdq-lite-offline-*.tar.gz
cd srdq-lite-offline-*-linux-x86_64
./install.sh
./run.sh --version
```

目标服务器需要 Linux x86_64、glibc 2.17+ 和 Python 3.11 或 3.12；不需要外网、pip 或第三方 Python 包。可通过 `PYTHON_BIN=/path/to/python ./install.sh` 指定解释器。

离线包内置三个生产集群的表清单，每个集群目录下有全量清单和按层拆分的清单：

- `table_info/output/<集群>/tables.jsonl`：该集群全部表。
- `table_info/output/<集群>/tables_<层>.jsonl`：按 `assets.csv` 的 `layer` 拆分，层为 `ods`、`dwd`、`dws`、`ads`、`dim`、`other`（账单集群没有 `ads`、`other`）。

集群目录为 `数仓集群`、`监控集群`、`账单集群`。所有表都会执行：

- 逐分区 `ROW_COUNT`。
- 有金额字段时，对金额字段做 `SUM`。
- 每个状态字段做一次 `GROUP BY + COUNT(*)`。
- PRIMARY KEY / UNIQUE KEY 表执行 `SAMPLE`；DUPLICATE / AGGREGATE 表不生成 SAMPLE。

按层运行时，每层使用单独的输出目录，例如：

```bash
./run.sh run -c config.yaml -t table_info/output/数仓集群/tables_ods.jsonl -o runs/数仓-ods
```

## 配置

复制并修改 `config.example.yaml`。推荐使用 `password_env`，避免密码落盘：

```bash
export SR_SOURCE_PASSWORD='...'
export SR_TARGET_PASSWORD='...'
```

`run` 参数：

- `concurrency`：每侧并发查询数。每条查询在独立子进程中执行，结果解析可用满多核；源端和目标端最多各启动 `concurrency` 个子进程。
- `connect_timeout_s`、`query_timeout_s`：连接及查询超时。
- `retries`：可重试错误的重试次数。
- `max_partitions_per_sql`：一条 UNION ALL SQL 最多合并的分区数。
- `max_rows_per_sql`：一条 UNION ALL SQL 合并分区的元数据行数上限。
- `sample_partitions_per_sql`：SAMPLE 抽样/回查 SQL 最多合并的分区数，默认 10。
- `max_diffs_per_rule`：每张表每条规则最多写出的差异数。

`session` 中的键值会作为 StarRocks session variable 设置。

## 表清单与规则

`tables.jsonl` 每行一个 JSON 对象，完整示例见 `tables.example.jsonl`。`column_mapping` 表示源列名到目标列名的映射。

### ROW_COUNT

逐分区比较行数。可选 `filter`：字符串表示两侧相同条件，也可分别配置：

```json
{"rule_name":"ROW_COUNT","config":{"filter":{"source":"is_deleted=0","target":"deleted=0"}}}
```

### METRIC

支持 `SUM`、`MIN`、`MAX`、`COUNT`、`COUNT_NULL`、`COUNT_DISTINCT`，可设置 `group_by`、`filter`、`max_groups`。只设置 `group_by` 时会按分组比较 `COUNT(*)`。浮点 `SUM` 默认相对误差为 `1e-9`，也可通过 `tolerance` 指定。

```json
{"rule_name":"METRIC","config":{"metrics":[{"column":"amount","func":"SUM","tolerance":1e-8}],"group_by":["status"]}}
```

### SAMPLE

仅适用于 PRIMARY KEY / UNIQUE KEY 表。逐分区执行，只读 source 的少量行，再按主键回查 target：

1. 统计 source 每个分区行数（与同过滤条件的 ROW_COUNT 共用同一次查询）。
2. 每个分区抽 `min(max_rows, max(1, 行数 × ratio))` 行：`SELECT ... FROM t PARTITION(p) LIMIT n`，每条 SQL 用 UNION ALL 合并 `sample_partitions_per_sql` 个分区。
3. 用抽到的主键值到 target 对应分区查询（每个 Key 列一个 `IN` 列表，可走前缀索引），逐行比较各列。

`LIMIT` 没有 `ORDER BY`，取到的是各 tablet 最先读到的行，不是随机样本，重复运行通常抽到相同的行。抽样只检查 source 中的行是否在 target 存在且一致；target 多出的行由 ROW_COUNT 发现。

- `ratio`：每分区抽样比例，范围 `(0, 1]`，默认 `0.001`。
- `max_rows`：每分区最多抽样行数，默认 10000。
- `key`：可选，必须与表 Key 一致；省略时自动使用表 Key。
- `columns`：`"*"` 或列名数组。
- `exclude_columns`：从采样列中排除。
- `filter`：两侧过滤条件。

## 运行

从 `srdq-lite` 目录执行：

```bash
cd srdq-lite
../.venv/bin/python -m srdq_lite run \
  -c config.yaml -t tables.jsonl -o runs/20261012
```

退出码：`0` 全部通过，`1` 存在差异或阻塞，`2` 配置/执行错误，`130` 被中断。

常用选项：

```bash
# 只连接、读取元数据并生成 SQL
python -m srdq_lite run -c config.yaml -t tables.jsonl -o runs/test --dry-run

# 重试已经记录为 FAILED 的任务
python -m srdq_lite run -c config.yaml -t tables.jsonl -o runs/test --retry-failed

# 输入变更后归档旧结果并重新规划
python -m srdq_lite run -c config.yaml -t tables.jsonl -o runs/test --replan

# 不连接 StarRocks，使用已落盘结果重新生成报告
python -m srdq_lite report -o runs/test
```

重复执行相同命令会读取 jsonl 进度并跳过已完成任务。配置或表清单发生变化时默认拒绝续跑，需换新输出目录或显式使用 `--replan`。Ctrl-C/SIGTERM 会取消正在执行的查询，已完成结果会保留。

## 输出

主要文件：

- `run.json`、`01_connectivity.json`：运行参数及连接检查。
- `02_metadata.jsonl`：表、列、Key 和分区检查。
- `03_tasks.jsonl`、`04_results.jsonl`：主任务 SQL（ROW_COUNT、METRIC、SAMPLE 行数统计）及执行状态。
- `06_sample_pick_tasks.jsonl`、`07_sample_pick_results.jsonl`：SAMPLE 在 source 的抽样 SQL 及执行状态。
- `08_sample_lookup_tasks.jsonl`、`09_sample_lookup_results.jsonl`：SAMPLE 在 target 的主键回查 SQL 及执行状态。
- `results/*.jsonl.gz`：流式查询结果。
- `05_compare.jsonl`：分区/规则级比较结果。
- `table_summary.jsonl`、`table_summary.csv`：每张表汇总。
- `diffs.jsonl`、`diffs.csv`：行、列、指标和分区差异。
- `failed_tasks.jsonl`：失败任务及 SQL。
- `summary.json`：本次运行总览。
- `run.log`：执行日志。

所有阶段文件均保存在输出目录。jsonl 采用追加写入并可修复未完整写入的最后一行；最终报告和 gzip 结果使用临时文件原子替换。

## 离线查看报告

直接用浏览器打开 `viewer.html`，选择一个或多个输出目录，或将目录拖入页面。页面不依赖网络和第三方库，可按运行、状态、表名筛选，查看元数据问题、分区结果、具体主键/列差异和失败任务。

浏览器的拖放目录支持因实现而异；若拖放没有加载文件，请使用“选择报告目录”。
