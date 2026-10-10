# srdq：StarRocks 双云数据校验

在华为云和阿里云的两套 StarRocks 之间持续做只读校验，给出数据是否一致的证据。

- **三档检查**
  - COUNT：分区行数。
  - METRIC：分组汇总值。
  - SAMPLE：按主键哈希分桶抽样，逐行比对。
- **工作方式**
  - 在每天的执行窗口内自动运行。
  - 差异出现在本机看板上，并写入日报文件。
  - 不对外发送任何通知。

需求、方案和技术设计见 [docs/](docs/README.md)。

## 组成

| 进程 | 系统用户 | 作用 |
|---|---|---|
| `srdq controller` | srdq-ctl | 调度循环，以及面向执行器的 API（127.0.0.1:8471）。负责写 PG 控制库、比较结果、生成报告 |
| `srdq web` | srdq-ctl | 看板和 REST API（127.0.0.1:8470）。单用户，只能经 SSH 端口转发访问 |
| `srdq executor --side huawei` | srdq-hw | 只持有华为 SR 的只读凭证，执行只读查询，上传计数、汇总和样本指纹 |
| `srdq executor --side aliyun` | srdq-al | 同上，负责阿里云 |

各进程的凭证互相隔离：

- Controller 和 Web 读不到任何 SR 凭证，启动时会自检，能读到就拒绝启动。
- 每个执行器只能读自己这一侧的凭证文件。
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
2. 生成两个模拟 SR 集群（文件存储），预置三张表：
   - `ods.orders`：主键表，7 个日期分区。目标端少了 id=17，id=1001 的金额被改过。
   - `ods.customers`：UNQ_KEYS，字符串主键。
   - `dwd.order_detail`：明细表。
3. 执行 `srdq migrate`。
4. 为两侧执行器各签发一个 worker token。
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

## 部署（单台阿里 ECS，CENTRAL 模式）

1. **准备系统用户和目录**

   ```bash
   groupadd srdq
   for u in srdq-ctl srdq-hw srdq-al; do useradd -r -g srdq -s /sbin/nologin $u; done
   install -d -o root -g srdq -m 0750 /etc/srdq /etc/srdq/secrets
   install -d -o srdq-ctl -g srdq -m 0700 /var/lib/srdq/controller /var/lib/srdq/reports
   install -d -o srdq-hw  -g srdq -m 0700 /var/lib/srdq/executor/huawei
   install -d -o srdq-al  -g srdq -m 0700 /var/lib/srdq/executor/aliyun
   chmod 0755 /var/lib/srdq /var/lib/srdq/executor
   ```

2. **安装**

   ```bash
   uv venv /opt/srdq/venv && uv pip install --python /opt/srdq/venv .
   ```

3. **配置**
   - 把 `deploy/config.example.yaml` 复制为 `/etc/srdq/config.yaml`，属主 root:srdq，权限 0640。
   - 首次部署时保持 `run_enabled: false`。
   - 在 `transfer.global_approval_ref` 登记客户的书面审批编号。没有审批编号时，METRIC / SAMPLE 保持 BLOCKED。

4. **准备凭证文件**（都是 0600，属主分别为对应用户）
   - `/etc/srdq/secrets/pg.json`：属主 srdq-ctl，格式见 `deploy/secrets/pg.json.example`。
   - `/etc/srdq/secrets/huawei.json`：属主 srdq-hw，格式见 `deploy/secrets/side.json.example`。
   - `/etc/srdq/secrets/aliyun.json`：属主 srdq-al。
   - SR 账号只授予 SELECT 权限。

5. **初始化数据库并签发执行器令牌**

   令牌只显示一次，控制库里只保存它的 sha256。把令牌写进对应 side 凭证文件的 `worker_token` 字段，然后删除临时文件。

   ```bash
   sudo -u srdq-ctl SRDQ_CONFIG=/etc/srdq/config.yaml /opt/srdq/venv/bin/srdq check-config
   sudo -u srdq-ctl SRDQ_CONFIG=/etc/srdq/config.yaml /opt/srdq/venv/bin/srdq migrate
   sudo -u srdq-ctl SRDQ_CONFIG=/etc/srdq/config.yaml /opt/srdq/venv/bin/srdq worker-token create --name hw-1 --side huawei --out /tmp/hw.token
   sudo -u srdq-ctl SRDQ_CONFIG=/etc/srdq/config.yaml /opt/srdq/venv/bin/srdq worker-token create --name al-1 --side aliyun --out /tmp/al.token
   ```

6. **安装并启动 systemd 服务**

   ```bash
   cp -r deploy/systemd/* /etc/systemd/system/ && systemctl daemon-reload
   systemctl enable --now srdq-controller srdq-web srdq-executor@huawei srdq-executor@aliyun
   ```

   unit 文件里已经做了隔离：
   - `InaccessiblePaths` 屏蔽不属于本进程的凭证。
   - `ProtectSystem=strict`、`UMask=0077`。
   - controller 只允许访问本机网络。

7. **访问看板**

   ```bash
   ssh -L 8470:127.0.0.1:8470 <ecs>
   ```

   然后在本机浏览器打开 http://127.0.0.1:8470。

8. **完成初始化后再开启业务检查**
   - 在 Web 上完成：导入资产清单、发现分区、确认范围、导入并批准规则、建计划或周期，先做一次 PoC 运行。
   - 确认无误后，把 `run_enabled` 改为 `true`，再重启 controller。

### 从 PG 备份恢复后

```bash
srdq recovery enter --reason "restored from backup <id>"
```

核对执行器状态和未确认的取消，然后执行：

```bash
srdq recovery confirm --reason "..."
```

## 命令行速查

| 命令 | 说明 |
|---|---|
| `srdq check-config` | 校验配置和凭证文件权限 |
| `srdq migrate` | 执行数据库迁移 |
| `srdq controller [--dev]` / `srdq web [--dev]` | 启动服务。`--dev` 跳过 SR 凭证隔离自检，只用于开发机 |
| `srdq executor --side S [--token-file F] [--data-dir D] [--sim DIR] [--dev]` | 启动一侧执行器。控制器暂时不可达时按指数退避重试 |
| `srdq worker-token create --name N --side S [--rotate] [--out F]` / `disable --name N --reason R` | 管理执行器身份 |
| `srdq import assets\|metric\|sample FILE [--mode UPDATE_EXPLICIT] [--confirm --reason R]` | 预览导入，可选直接确认。有 ERROR 行时不会确认 |
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
