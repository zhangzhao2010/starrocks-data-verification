# srdq：StarRocks 多集群双云数据校验

在华为云和阿里云的三组固定配对 StarRocks 集群之间持续做只读校验，给出数据是否一致的证据。每个逻辑资产只属于一个校验组，不支持跨组比较。

- **三档检查**
  - COUNT：分区行数。
  - METRIC：分组汇总值。
  - SAMPLE：按主键哈希分桶抽样，逐行比对。
- **工作方式**
  - 在每天的执行窗口内自动运行。
  - 每个物理集群的 COUNT、METRIC、SAMPLE、METADATA、DEEP 使用独立并发池并行执行。
  - 差异出现在本机看板上，并写入日报文件。
  - 不对外发送任何通知。

需求、方案和技术设计见 [docs/](docs/README.md)。

## 组成

| 进程 | 系统用户 | 作用 |
|---|---|---|
| `srdq controller` | srdq-ctl | 调度循环，以及面向执行器的 API（127.0.0.1:8471）。负责写 PG 控制库、比较结果、生成报告 |
| `srdq web` | srdq-ctl | 看板和 REST API（0.0.0.0:8470）。单用户，无登录，可直接用 ECS IP 访问 |
| `srdq executor --cluster-id C` | 每集群独立用户 | 每个物理集群一个实例，只持有该集群的只读凭证，执行只读查询并上传结果 |

各进程的凭证互相隔离：

- Controller 和 Web 读不到任何 SR 凭证，启动时会自检，能读到就拒绝启动。
- 每个执行器只能读自己物理集群的凭证文件。
- PG 凭证只给 srdq-ctl。

## 本地开发

需要 [uv](https://docs.astral.sh/uv/)。PostgreSQL 由 `pgserver` 在本地临时拉起，不需要另外安装。

```bash
uv sync                           # 安装依赖，含 dev 组：pytest、hypothesis、pgserver
uv run pytest -q                  # 全部测试，约 20 秒
uv run python scripts/demo.py     # 本地演示，数据目录默认 /tmp/srdq-demo
```

`demo.py` 会依次完成以下准备，然后启动 4 个真实的 srdq 进程：

1. 拉起本地 PG。
2. 生成一组两个模拟 SR 集群（文件存储），预置三张表：
   - `ods.orders`：主键表，7 个日期分区。目标端少了 id=17，id=1001 的金额被改过。
   - `ods.customers`：UNQ_KEYS，字符串主键。
   - `dwd.order_detail`：明细表。
3. 执行 `srdq migrate`。
4. 为两个模拟集群的执行器各签发一个 worker token。
5. 导入资产清单。

演示的其余步骤在浏览器中完成（打开 http://127.0.0.1:8470）：

1. **范围清单** → 发现分区 → 确认。
2. **导入**规则模板，导入后在**规则**页批准：
   - `/tmp/srdq-demo/metric_rules.csv`
   - `/tmp/srdq-demo/sample_rules.csv`

   也可以用命令行导入（导入后同样要在**规则**页批准）：

   ```bash
   uv run srdq --config /tmp/srdq-demo/config.yaml import metric /tmp/srdq-demo/metric_rules.csv --confirm --reason demo
   ```
3. **计划** → 新建（勾选 COUNT / METRIC / SAMPLE）→ 开始运行。
4. 运行大约 10 秒完成，**差异**页会出现：
   - ods.orders 的一个 COUNT 差异；
   - 两个 METRIC 差异。

按 Ctrl-C 停止。再次运行时会复用已有数据；需要重置时，删除 `/tmp/srdq-demo`。

## 离线部署（Alibaba Cloud Linux 4 x86_64，CENTRAL 模式）

离线包固定目标为 CPython 3.11、x86_64、glibc 2.17+，包含应用及依赖 wheel、PostgreSQL 16 二进制、systemd unit 和部署脚本。生产机不需要外网、uv、pip 或单独安装 PostgreSQL；需要带 `venv` 模块的 Python 3.11、systemd 和 root 权限。

1. **在有外网的 x86_64 Linux 构建机准备生产配置**

   在仓库之外创建权限为 0700 的目录，结构固定如下：

   ```text
   /secure/srdq-prod/
   ├── config.yaml
   └── secrets/
       ├── huawei-1.json
       ├── aliyun-1.json
       ├── huawei-2.json
       ├── aliyun-2.json
       ├── huawei-3.json
       ├── aliyun-3.json
       └── <cluster>-ca.pem  # 使用自定义 CA 时才需要
   ```

   - `config.yaml` 可从 `deploy/config.example.yaml` 复制，必须设置 `run_enabled: true`、真实的 `transfer.global_approval_ref`、六个 `physical_clusters` 和三个固定 `cluster_pairs`。
   - 六个 JSON 可参考 `deploy/secrets/side.json.example`，分别填写只读 StarRocks 账号，但删除 `worker_token`；安装程序会为每个物理集群生成令牌并安全写入。
   - `ssl_ca` 如有配置，必须指向 `/etc/srdq/secrets/` 下同时打包的 CA 文件。

2. **构建包含生产配置的离线包**

   ```bash
   ./scripts/build-offline-bundle.sh --provision-dir /secure/srdq-prod
   (cd dist && sha256sum -c srdq-offline-*-alinux4-x86_64.tar.gz.sha256)
   ```

   每次发布必须先递增 `pyproject.toml` 的 `project.version` 并执行 `uv lock`。构建脚本会校验配置、拒绝占位值及预置 worker token。产物包含生产密码，压缩包和校验文件强制为 0600，只能通过受控通道传输，不能提交到 Git 或普通制品库。

3. **经跳板机传输并一键安装**

   ```bash
   scp -J <user>@<jump-host> dist/srdq-offline-*.tar.gz* <user>@<ecs>:/tmp/
   # 在 ECS 上：
   cd /tmp
   VERSION=1.4.0
   sha256sum -c "srdq-offline-${VERSION}-alinux4-x86_64.tar.gz.sha256"
   tar xzf "srdq-offline-${VERSION}-alinux4-x86_64.tar.gz"
   sudo "./srdq-offline-${VERSION}/deploy/install.sh"
   ```

   安装脚本自动写入配置和凭证、创建隔离用户、初始化仅开放 Unix socket 的 PostgreSQL、迁移数据库、生成执行器令牌，并立即启动 controller、web、六个 executor 和每日备份 timer。生产机不需要再编辑文件或执行初始化命令。安装成功后应删除 `/tmp` 中含凭证的压缩包和解压目录。

   程序安装到 `/opt/srdq/releases/<版本>`，`/opt/srdq/current` 原子指向当前版本；数据库和历史数据保存在 `/var/lib/srdq`，不会随代码替换。PostgreSQL 凭证由安装脚本生成，不含密码，只允许 `srdq-ctl` 通过本机 socket 使用。

4. **访问看板**

   在浏览器打开 `http://<ECS IP>:8470/`（安全组需放通 8470 端口）。服务部署后已经处于允许运行状态。

### 离线升级和回滚

把新版本离线包传到 ECS 并校验、解压，然后执行：

```bash
sudo ./srdq-offline-<新版本>/deploy/upgrade.sh
```

升级包同样必须通过 `--provision-dir` 构建。升级脚本会停止当前应用服务，先执行一致性 `pg_dump`，保存旧版本配置快照，安装新版本和包内配置，保留已有 worker token，执行增量迁移，原子切换 `current`，再恢复升级前正在运行的服务。备份路径和精确回滚命令会打印到终端。已发布的迁移文件不能修改，数据库变化只能新增连续编号的迁移文件。

若升级后需要回滚，使用升级命令打印的版本和备份路径：

```bash
sudo /opt/srdq/deploy/rollback.sh <旧版本> /var/lib/srdq/backups/<升级前备份>.dump
```

回滚会先备份当前数据库，再把数据库和配置恢复到旧版本快照、切回旧代码并进入恢复模式；核对执行器状态后按下节确认恢复。这样升级后产生的数据和新版本配置不会被错误地带到旧结构中。每日 02:30 的备份由 `srdq-pg-backup.timer` 创建，默认保留 14 天；也可以手动执行 `sudo /opt/srdq/bin/srdq-pg-backup --label manual`。

systemd unit 使用 `ProtectSystem=strict`、凭证路径隔离和最小权限。PostgreSQL 在独立的 `srdq-pg` 用户下运行，启用 `PrivateNetwork`，不监听 TCP；只有 `srdq-ctl` 能访问数据库 socket。

### 从 PG 备份恢复后

```bash
sudo -u srdq-ctl env SRDQ_CONFIG=/etc/srdq/config.yaml /opt/srdq/current/venv/bin/srdq recovery enter --reason "restored from backup <id>"
```

核对执行器状态和未确认的取消，然后执行：

```bash
sudo -u srdq-ctl env SRDQ_CONFIG=/etc/srdq/config.yaml /opt/srdq/current/venv/bin/srdq recovery confirm --reason "..."
```

## 命令行速查

| 命令 | 说明 |
|---|---|
| `srdq check-config` | 校验配置和凭证文件权限 |
| `srdq migrate` | 执行数据库迁移 |
| `srdq controller [--dev]` / `srdq web [--dev]` | 启动服务。`--dev` 跳过 SR 凭证隔离自检，只用于开发机 |
| `srdq executor --cluster-id C [--token-file F] [--data-dir D] [--sim DIR] [--dev]` | 启动一个物理集群的执行器。控制器暂时不可达时按指数退避重试 |
| `srdq worker-token create --name N --cluster-id C [--rotate] [--out F]` / `disable --name N --reason R` | 管理执行器身份 |
| `srdq import assets\|metric\|sample FILE [--mode UPDATE_EXPLICIT] [--confirm --reason R]` | 预览导入，可选直接确认；CSV 必须包含 `pair_id`，有 ERROR 行时不会确认 |
| `srdq recovery enter\|confirm --reason R` | 进入或确认恢复 |
| `srdq report stage --start D --end D --reason R` / `regenerate-daily KEY --reason R` | 生成阶段报告、重新生成日报 |
| `srdq tick` / `srdq status` | 手动执行一次调度 / 打印看板摘要 |

配置文件路径的查找顺序：`--config`，其次是环境变量 `SRDQ_CONFIG`，最后是 `/etc/srdq/config.yaml`。

开发时可以用 `SRDQ_PG_DSN` 覆盖 PG 连接。

## 代码结构

```
srdq/common      规范编码、规则 DSL
srdq/config.py   严格配置（未知键会被拒绝）
srdq/db          迁移（migrations/0001_init.sql）和连接池
srdq/controller  导入、范围清单、计划、领取/租约、接收、比较、覆盖、控制操作、周期、窗口、
                 负载保护、恢复、报告、清理、worker API
srdq/executor    SR 适配器（pymysql / 模拟集群）、SQL 编译、结果编码、本地待发箱、runner
srdq/web         REST /v1 和服务端渲染页面（无脚本，CSP default-src 'self'）
deploy/          配置示例、凭证模板、systemd unit
scripts/demo.py  本地演示
tests/           单元测试、协议向量、controller/executor 端到端、Web/安全、CLI
```
