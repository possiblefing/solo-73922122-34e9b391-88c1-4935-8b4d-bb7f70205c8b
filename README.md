# 夜间包裹扫描查询 · 候选索引成本评估 API

物流数仓工程师在优化夜间包裹扫描查询前，用这套服务判断 **B-tree 候选索引能否降低查询成本**。
基于 **FastAPI + psycopg(v3) + PostgreSQL 15 + HypoPG**，只做 **EXPLAIN 估算**：
不执行查询（`EXPLAIN` 不带 `ANALYZE`），也不创建真实索引（HypoPG 假想索引仅存于后端内存）。

## 能力与约束（与需求逐条对应）

| 需求 | 实现 |
| --- | --- |
| 提交只读 SELECT + 最多 3 个候选（表名 + 有序列名） | `POST /evaluate`，Pydantic 校验候选数 1~3、标识符合法性、候选不重复 |
| 返回无候选 / 全候选启用的 EXPLAIN JSON、总成本、采用的候选 | 响应含 `baseline_plan`、`hypothetical_plan`、两套 `total_cost`、`adopted_candidates` / `rejected_candidates` |
| 只估算，不执行查询、不建真实索引 | `EXPLAIN (FORMAT JSON)`；`hypopg_create_index()` 只在会话内存造假想索引 |
| 单 API 进程最多复用两条数据库连接 | `TwoConnectionPool`：信号量硬上限 2，单 uvicorn worker；`/health` 可观测 |
| 并发评估只能看见各自候选 | HypoPG 假想索引是**会话级**状态，一条租约连接一个会话，天然隔离 |
| 每次基线不继承旧候选 | 借出连接先 `ROLLBACK + hypopg_reset` 并验证为 0 才放行；响应字段留证 |
| 报错 / 断连时等真实 DB 工作停止、事务与假想索引清干净后才归还 | 租约上下文统一 `full_cleanup`：取消在途查询 → ROLLBACK → hypopg_reset → 协议状态与计数双重验证；清理不可确认的连接直接关闭丢弃，绝不回队 |
| 迟到清理不取消下一请求、不删其候选 | 清理全程 `asyncio.shield` 屏蔽取消；下一请求只从队列取已验证干净的连接 |
| 失败不返回部分比较 | 任何异常都不产出半成品响应；非法输入 400，内部错误 500 |
| 可实际启动的 Docker Compose，/docs 可提交评估 | `docker compose up` 后访问 <http://localhost:8000/docs> |
| 能改善 / 不能改善计划的索引样例 | `GET /samples`，内置两组样例及配套候选 |
| 可演示创建候选后暂停、断连再复用连接 | 请求头 `X-Demo-Pause-Seconds`（可选 `X-Demo-Pause-Mode`） |
| **停机交接：关闭开始即拒绝新租约、排队者立即退出** | 可关闭的租约闸门 `_LeaseGate`：`begin_shutdown()` 关闭闸门并唤醒全部排队 `acquire`，新/排队请求统一抛 `PoolClosedError` → HTTP 503 |
| **在途评估完成或取消后，先停 DB 工作、清事务/假想索引再关物理连接** | 停机收尾 `_run_shutdown` 等待全部租约归还（复用 `full_cleanup`：取消查询 → ROLLBACK → hypopg_reset 双重验证），逐条关闭并以 `_live` 集合兜底核对 |
| **重复关闭等待同一轮收尾，close 返回无打开连接、无在途评估** | 所有 `close()` 共享同一个 `asyncio.Event`；`_live` 集合与 `in_use` 双重验证，返回时 `live_connections=0` |
| **在途/排队评估遇到停机的可控演示** | `POST /admin/shutdown`（`cancel_inflight`、`wait` 参数）+ `scripts/shutdown_demo.py` |

## 目录结构

```
.
├── docker-compose.yml          # db(Postgres+HypoPG) + api(FastAPI)，api 单进程
├── db/
│   ├── Dockerfile              # postgres:15-bookworm + postgresql-15-hypopg
│   └── initdb/
│       ├── 01_schema.sql       # package_scan 事实表（仅主键真实索引）
│       └── 02_seed.sql         # 确定性生成 50 万行扫描明细 + ANALYZE
├── api/
│   ├── Dockerfile
│   ├── requirements.txt
│   └── app/
│       ├── main.py             # 路由 / 生命周期 / 异常映射
│       ├── config.py           # 环境变量配置（连接上限、超时、演示开关）
│       ├── db.py               # 两连接租约池 + 取消安全清理
│       ├── service.py          # HypoPG 评估主流程（EXPLAIN、成本、采纳判定）
│       ├── validation.py       # 只读 SELECT 解析守卫
│       ├── schemas.py          # 请求/响应模型
│       └── samples.py          # 预置样例 SQL 与候选
├── tests/
│   ├── test_api.py             # 端到端断言（需运行中的 API + HypoPG Postgres）
│   └── test_pool_shutdown.py   # 连接池停机交接的无数据库单元测试（74 项断言，假连接）
└── scripts/
    ├── demo.sh                 # 命令行演示（正常评估 / 断连复用）
    └── shutdown_demo.py        # 在途与排队评估遇到停机的可控演示（仅标准库）
```

## 启动

```bash
docker compose up --build
# 首次启动会初始化数据库并灌入 50 万行数据，约需十几秒
# - Swagger 文档： http://localhost:8000/docs
# - 健康检查：     http://localhost:8000/health
# - 预置样例：     http://localhost:8000/samples
```

## 快速体验

### 1) 能改善计划的候选（warehouse_id 等值 + scan_time 排序）

```bash
curl -s -X POST http://localhost:8000/evaluate \
  -H 'Content-Type: application/json' \
  -d '{
    "sample_id": "nightly_warehouse_scan",
    "candidates": [
      {"table": "public.package_scan", "columns": ["warehouse_id", "scan_time"]}
    ]
  }'
```

基线 `Seq Scan + Sort`（成本约 15920）→ 假想索引下 `Index Scan Backward`（成本约 772），
候选进入 `adopted_candidates`。

### 2) 不能改善计划的候选（谓词无关列 / 全表聚合）

```bash
curl -s -X POST http://localhost:8000/evaluate \
  -H 'Content-Type: application/json' \
  -d '{
    "sample_id": "nightly_warehouse_scan",
    "candidates": [
      {"table": "public.package_scan", "columns": ["destination"]},
      {"table": "public.package_scan", "columns": ["weight_kg"]}
    ]
  }'
```

成本与计划均不变，两个候选进入 `rejected_candidates`。
`nightly_aggregate` 样例（全表 GROUP BY）下任何 B-tree 都不会被采用。

### 3) 暂停 + 断连 + 连接复用演示

```bash
# 终端 A：发起一个创建候选后暂停 30 秒的请求
curl -X POST http://localhost:8000/evaluate \
  -H 'Content-Type: application/json' -H 'X-Demo-Pause-Seconds: 30' \
  -d '{"sample_id":"nightly_warehouse_scan",
       "candidates":[{"table":"public.package_scan","columns":["warehouse_id","scan_time"]}]}'
# 在另一个终端 Ctrl+C / 杀掉 curl 模拟断连；随后再发普通请求，
# /health 中 cancels_performed 增加，total_discarded 保持 0，新请求看不到任何残留候选。
```

- 默认 `X-Demo-Pause-Mode: asyncio`：暂停不占用数据库工作，断连检测即时，
  服务端走完整取消清理后把连接回队复用。
- 设为 `pg_sleep` 时，暂停是一条真实在途 `SELECT pg_sleep(...)`，断连会先
  `conn.cancel()` 停掉服务端查询再清理（需要操作系统允许进程信号，标准容器内核支持）。

也可直接运行 `bash scripts/demo.sh`。

### 4) 停机交接演示：在途与排队评估同时遇到停机

夜间索引评估服务重启时，旧评估可能仍占用连接，新请求还在排队。本服务的
连接池保证严格的停机交接（实现见 `api/app/db.py`）：

1. **关闭开始即拒绝新租约**：`begin_shutdown()` 关闭可唤醒的租约闸门，
   之后任何 `lease()` 立即抛 `PoolClosedError`，HTTP 层映射为 **503
   `pool_closing`**，绝不会在已关闭的池上继续评估；
2. **排队请求立即退出**：闸门关闭会唤醒所有阻塞在 `acquire()` 上的请求，
   它们拿到同一个 `PoolClosedError` 退出，而不是继续等待并在停机后评估；
3. **在途评估完成或被取消后再关物理连接**：收尾协程等待全部租约归还。
   每条连接在物理关闭前都走同一条 `full_cleanup`——先发 `cancel()` 停掉
   在途数据库工作并等待确认，再 `ROLLBACK`、`hypopg_reset`，并以协议事务
   状态 `IDLE` + 假想索引计数 0 双重验证；不可确认干净的连接直接关闭丢弃；
4. **重复关闭等待同一轮收尾**：所有 `close()` 共享一个 `asyncio.Event`，
   后到调用只等待不重复收尾。`close()` 返回时 `live_connections=0`、
   `in_use=0`，没有打开的连接，也没有仍在执行的评估。

可控演示（推荐，无需手速）：

```bash
python3 scripts/shutdown_demo.py            # 默认 http://localhost:8000
# 脚本会：
#  1) 发起 2 个带 X-Demo-Pause 的在途评估（占满两条连接）
#  2) 再发起 3 个排队评估（阻塞等连接）
#  3) 调用 POST /admin/shutdown 触发停机交接
#  4) 打印在途/排队请求的最终状态（均为 503 pool_closing）
#  5) 调用 /health 确认 state=closed、live_connections=0、in_use=0
```

也可手动触发。`/admin/shutdown` 的两个参数：

| 参数 | 默认 | 含义 |
| --- | --- | --- |
| `cancel_inflight` | `true` | 取消在途评估（等价重启时 uvicorn 取消请求任务）；`false` 则等待在途自然完成 |
| `wait` | `true` | 同步等待本轮收尾（所有物理连接关闭）后再返回 |

```bash
# 终端 A：两个暂停中的在途评估占满连接，第三个请求排队
for i in 1 2; do
  curl -s -X POST http://localhost:8000/evaluate \
    -H 'Content-Type: application/json' -H 'X-Demo-Pause-Seconds: 30' \
    -d '{"sample_id":"nightly_warehouse_scan",
         "candidates":[{"table":"public.package_scan","columns":["warehouse_id","scan_time"]}]}' &
done
# 终端 B：触发停机（取消在途、等待收尾完成）
curl -s -X POST http://localhost:8000/admin/shutdown \
  -H 'Content-Type: application/json' -d '{"cancel_inflight": true, "wait": true}'
# 返回体 pool.live_connections 必为 0；此时新请求一律 503
curl -s http://localhost:8000/health
```

> 注意：`/admin/shutdown` 会让进程内连接池进入不可恢复的 `closed` 状态，
> 仅用于演示/运维验证；触发后需重启 API 容器才能再次评估。真实进程重启
> （`docker compose restart api` / SIGTERM）走的是同一条交接路径，由
> lifespan 的 `shutdown` 事件调用幂等的 `pool.close()`，uvicorn 配置了
> `--timeout-graceful-shutdown 30` 等待在途清理。

无数据库也可验证全部停机语义（假连接驱动真实连接池，74 项断言）：

```bash
python -I tests/test_pool_shutdown.py
```

## 安全设计

1. **SQL 只读双保险**：`sqlparse` 词法层拒绝多语句与一切写/DDL 关键字；
   每条连接 `SET default_transaction_read_only = on`，数据库自身再挡一层。
2. **标识符注入防护**：表名/列名经 Pydantic 正则与 psycopg `Identifier` 双重处理。
3. **慢查询兜底**：会话级 `statement_timeout`、`lock_timeout`、
   `idle_in_transaction_session_timeout`。
4. **脏连接绝不复用**：清理后校验协议事务状态为 IDLE 且假想索引计数为 0，
   不达标即关闭连接并新建，信号量保证任意时刻物理连接数 ≤ 2。

## 本地（无 Docker）运行测试

连接池停机交接的单元测试**不需要数据库**（用假连接驱动真实池实现）：

```bash
pip install -r api/requirements.txt httpx
python -I tests/test_pool_shutdown.py
```

仓库的端到端测试可指向任意已装 HypoPG 的 Postgres：

```bash
pip install -r api/requirements.txt
DB_DSN=postgresql://USER@HOST:PORT/logistics \
DEMO_PAUSE_ENABLED=true \
uvicorn app.main:app --app-dir api --port 8000
python -I tests/test_api.py http://127.0.0.1:8000
```
