"""连接池停机交接的无数据库单元测试。

用 FakeConn 模拟 psycopg AsyncConnection 的行为（含事务状态、cancel、close、
hypopg_reset 计数），驱动真实的 TwoConnectionPool 验证：

1. 停机开始即拒绝新租约（PoolClosedError → HTTP 层映射 503）；
2. 排队等连接的请求在停机后被唤醒并退出，不会继续评估；
3. 在途评估自然完成后，先完成清理（ROLLBACK/hypopg_reset）再物理关闭连接；
4. 在途评估被取消时走同一条取消安全清理路径，随后关闭连接；
5. close() 可重复并发调用，共享同一轮收尾；返回时无打开连接、无在途评估；
6. 正常负载下物理连接数始终 <= 上限 2，/evaluate 成本对照能力不受影响。

运行（仓库根目录）：
  python -I tests/test_pool_shutdown.py
"""
from __future__ import annotations

import asyncio
import contextlib
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "api"))

import psycopg  # noqa: E402

from app import db as pool_db  # noqa: E402
from app.config import Settings  # noqa: E402

IDLE = psycopg.pq.TransactionStatus.IDLE
INTRANS = psycopg.pq.TransactionStatus.INTRANS

_passed = 0
_failed: list[str] = []


def check(name: str, cond: bool, detail: str = "") -> None:
    global _passed
    if cond:
        _passed += 1
        print(f"  PASS  {name}")
    else:
        _failed.append(f"{name} :: {detail}")
        print(f"  FAIL  {name}  {detail}")


class FakeCursor:
    _COLUMNS = [
        "scan_id", "package_no", "warehouse_id", "conveyor_id", "scan_time",
        "scan_event", "destination", "service_type", "length_cm",
        "width_cm", "height_cm", "weight_kg",
    ]

    def __init__(self, conn: "FakeConn", dict_row: bool = False) -> None:
        self._conn = conn
        self._dict_row = dict_row
        self._last_sql = ""

    async def __aenter__(self) -> "FakeCursor":
        return self

    async def __aexit__(self, *exc) -> bool:
        return False

    async def execute(self, sql, params=None) -> None:
        text = str(sql)
        self._last_sql = text
        self._conn.statements.append(text)
        if "hypopg_reset" in text:
            self._conn.resets += 1
            self._conn.reset_times.append(time.monotonic())
        # 非自动提交模式下任意语句开启隐式事务；ROLLBACK/COMMIT 使其回到 IDLE
        if text.strip().upper().startswith(("ROLLBACK", "COMMIT")):
            self._conn.txn_status = IDLE
        else:
            self._conn.txn_status = INTRANS

    async def fetchone(self):
        if self._dict_row:
            if "information_schema.tables" in self._last_sql:
                return {"table_name": "package_scan"}
            if "hypopg_create_index" in self._last_sql:
                return {"indexrelid": 100001, "index_name": "<100001>btree_demo"}
            return {"column_name": "warehouse_id"}
        if "hypopg_create_index" in self._last_sql:
            return (100001, "<100001>btree_demo")
        return (0,)  # 计数类查询恒为 0

    async def fetchall(self):
        if self._dict_row:
            if "information_schema.columns" in self._last_sql:
                return [{"column_name": c} for c in self._COLUMNS]
            if "hypopg_list_indexes" in self._last_sql:
                return [{
                    "indexrelid": 100001,
                    "index_name": "<100001>btree_demo",
                    "schema_name": "public",
                    "table_name": "package_scan",
                }]
            return []
        if "EXPLAIN" in self._last_sql.upper():
            # rows[0][0][0]：单行、单列、JSON 数组首元素
            return [[[{"Plan": {"Node Type": "Seq Scan", "Total Cost": 100.0}}]]]
        return []


class FakeConn:
    """记录完整生命周期的假连接。"""

    _registry: list["FakeConn"] = []
    _live_lock = asyncio.Lock()
    max_live = 0

    def __init__(self, dsn: str, **kwargs) -> None:
        self.dsn = dsn
        self.closed = False
        self.txn_status = IDLE
        self.cancel_called = 0
        self.resets = 0
        self.statements: list[str] = []
        self.reset_times: list[float] = []
        self.close_time: float | None = None
        self.info = self
        self._in_query = asyncio.Event()
        # psycopg.sql.Composed.as_string(conn)：context.connection 取连接本身，
        # 再经 conn_encoding -> conn.pgconn._encoding 取编码
        self.pgconn = type("_PGconn", (), {"_encoding": "utf-8"})()

    # 连接池通过 conn.info.transaction_status 访问
    @property
    def transaction_status(self):
        return self.txn_status

    @property
    def connection(self):
        # AdaptContext 协议：返回连接自身
        return self

    def cursor(self, **kwargs):
        from psycopg.rows import dict_row as _dict_row
        return FakeCursor(self, dict_row=kwargs.get("row_factory") is _dict_row)

    async def execute(self, sql, params=None):
        cur = FakeCursor(self)
        await cur.execute(sql, params)

    def cancel(self) -> None:
        # 模拟 psycopg3 同步 cancel：阻塞到查询终止
        self.cancel_called += 1
        self.txn_status = IDLE

    async def rollback(self) -> None:
        self.txn_status = IDLE

    async def close(self) -> None:
        if self.closed:
            return
        await asyncio.sleep(0)  # 模拟关闭开销，暴露竞态
        self.close_time = time.monotonic()
        self.closed = True

    @classmethod
    async def connect(cls, dsn, **kwargs):
        conn = cls(dsn, **kwargs)
        cls._registry.append(conn)
        async with cls._live_lock:
            live = sum(1 for c in cls._registry if not c.closed)
            cls.max_live = max(cls.max_live, live)
        return conn

    @classmethod
    def reset_registry(cls) -> None:
        cls._registry.clear()
        cls.max_live = 0


def make_settings(max_conn: int = 2) -> Settings:
    return Settings(
        db_dsn="postgresql://u:p@localhost/db",
        pool_max_connections=max_conn,
        statement_timeout_ms=30_000,
        demo_pause_enabled=True,
        demo_pause_max_seconds=30.0,
    )


@contextlib.asynccontextmanager
async def patched_pool(max_conn: int = 2):
    FakeConn.reset_registry()
    original = pool_db.AsyncConnection
    pool_db.AsyncConnection = FakeConn  # type: ignore[assignment]
    try:
        pool = pool_db.TwoConnectionPool(make_settings(max_conn))
        await pool.open()
        yield pool
        if pool.state != "closed":
            await pool.close()
    finally:
        pool_db.AsyncConnection = original  # type: ignore[assignment]


async def case_basic_lease_and_cap() -> None:
    print("== 1. 正常租约复用与物理连接上限 ==")
    async with patched_pool(2) as pool:
        async with pool.lease() as conn:
            check("租约可用", conn is not None)
            check("在途计数为 1", pool.stats()["in_use"] == 1, str(pool.stats()))
        await asyncio.sleep(0.05)
        check("归还后在途归零", pool.stats()["in_use"] == 0)
        check("归还后空闲为 1", pool.stats()["idle"] == 1)

        async def one(_idx: int) -> None:
            async with pool.lease():
                await asyncio.sleep(0.05)
                check("租约内实时物理连接 <= 2",
                      pool.stats()["live_connections"] <= 2, str(pool.stats()))

        await asyncio.gather(*(one(i) for i in range(10)))
        check("10 并发后物理连接峰值 <= 2", FakeConn.max_live <= 2,
              f"peak={FakeConn.max_live}")
        check("压力后无连接泄漏", pool.stats()["in_use"] == 0
              and pool.stats()["live_connections"] <= 2)


async def case_shutdown_idle_pool() -> None:
    print("== 2. 空闲池停机：拒绝新租约、全部物理连接关闭 ==")
    async with patched_pool(2) as pool:
        before = len(FakeConn._registry)
        check("open 预建 1 条连接", before == 1, str(before))
        await pool.close()
        check("close 后状态 closed", pool.state == "closed")
        check("live_connections 为 0", pool.stats()["live_connections"] == 0)
        check("所有物理连接均已关闭", all(c.closed for c in FakeConn._registry))
        try:
            async with pool.lease():
                check("停机后获取租约应失败", False)
        except pool_db.PoolClosedError:
            check("停机后新租约抛 PoolClosedError", True)
        check("拒绝计数递增", pool.stats()["rejected_while_closing"] >= 1)


async def case_queued_waiters_woken() -> None:
    print("== 3. 排队请求在停机时被唤醒退出 ==")
    async with patched_pool(2) as pool:
        gate_release = asyncio.Event()

        async def holder() -> None:
            async with pool.lease():
                await gate_release.wait()

        holders = [asyncio.create_task(holder()) for _ in range(2)]
        await asyncio.sleep(0.05)  # 等两条租约全部占满

        async def queued() -> str:
            async with pool.lease():
                return "acquired"

        waiters = [asyncio.create_task(queued()) for _ in range(3)]
        await asyncio.sleep(0.05)  # 确保进入排队
        check("排队期间尚无空闲许可", pool.stats()["in_use"] == 2)

        event = await pool.begin_shutdown()
        results = await asyncio.gather(*waiters, return_exceptions=True)
        closed_errors = [r for r in results if isinstance(r, pool_db.PoolClosedError)]
        check("3 个排队者全部收到 PoolClosedError", len(closed_errors) == 3,
              str([type(r).__name__ for r in results]))
        check("没有排队者拿到租约",
              all(not (isinstance(r, str) and r == "acquired") for r in results))

        gate_release.set()
        await event.wait()
        check("收尾结束后无在途评估", pool.stats()["in_use"] == 0)
        check("收尾结束后无打开连接",
              pool.stats()["live_connections"] == 0
              and all(c.closed for c in FakeConn._registry))
        await asyncio.gather(*holders, return_exceptions=True)


async def case_inflight_completes_then_cleanup_then_close() -> None:
    print("== 4. 在途评估自然完成：先清理后关连接，close 等待 ==")
    async with patched_pool(2) as pool:
        can_finish = asyncio.Event()
        close_done = asyncio.Event()

        async def evaluation() -> None:
            async with pool.lease() as conn:
                await conn.execute("SELECT 1")  # 制造打开的事务
                await can_finish.wait()
                # 正常结束，租约退出时 full_cleanup 自动 ROLLBACK + reset

        task = asyncio.create_task(evaluation())
        await asyncio.sleep(0.05)
        conn = next(c for c in FakeConn._registry if not c.closed)

        async def closer() -> None:
            await pool.close()
            close_done.set()

        close_task = asyncio.create_task(closer())
        await asyncio.sleep(0.05)
        check("在途评估未结束时 close 不返回", not close_done.is_set())
        check("停机期间该连接仍打开", not conn.closed)

        can_finish.set()
        await asyncio.wait_for(close_task, timeout=5)
        await task
        check("close 已返回", close_done.is_set())
        check("物理连接已关闭", conn.closed)
        check("关闭前执行过 hypopg_reset", conn.resets >= 1, str(conn.resets))
        check("关闭前执行过 ROLLBACK",
              any(s.strip().upper().startswith("ROLLBACK") for s in conn.statements))
        check("reset 不晚于 close",
              conn.close_time is not None and min(conn.reset_times) <= conn.close_time)


async def case_inflight_cancelled() -> None:
    print("== 5. 在途评估被取消：取消安全清理后关闭连接 ==")
    async with patched_pool(2) as pool:
        async def long_evaluation() -> None:
            async with pool.lease() as conn:
                # 模拟真实在途数据库工作（事务打开），暂停直到被取消
                await conn.execute("SELECT pg_sleep(30)")
                await asyncio.Event().wait()

        task = asyncio.create_task(long_evaluation())
        await asyncio.sleep(0.05)
        conn = next(c for c in FakeConn._registry if not c.closed)
        check("评估开始后事务非 IDLE", conn.txn_status != IDLE)

        event = await pool.begin_shutdown()
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task
        await event.wait()

        check("取消计数 >= 1", pool.stats()["cancels_performed"] >= 1,
              str(pool.stats()))
        check("取消后连接被关闭", conn.closed)
        check("取消路径执行了 hypopg_reset", conn.resets >= 1)
        check("取消后无在途租约", pool.stats()["in_use"] == 0)
        check("取消后无打开连接", all(c.closed for c in FakeConn._registry))


async def case_repeated_close_shares_round() -> None:
    print("== 6. 重复/并发 close 等待同一轮收尾 ==")
    async with patched_pool(2) as pool:
        can_finish = asyncio.Event()

        async def evaluation() -> None:
            async with pool.lease():
                await can_finish.wait()

        task = asyncio.create_task(evaluation())
        await asyncio.sleep(0.05)

        event = await pool.begin_shutdown()
        event2 = await pool.begin_shutdown()  # 幂等：同一事件
        check("重复 begin_shutdown 返回同一事件", event is event2)

        closes = [asyncio.create_task(pool.close()) for _ in range(3)]
        await asyncio.sleep(0.05)
        check("多个 close 均在等待同一轮收尾",
              all(not t.done() for t in closes))
        can_finish.set()
        await asyncio.wait_for(asyncio.gather(*closes), timeout=5)
        check("全部 close 正常返回", all(t.done() and not t.exception() for t in closes))
        check("最终状态 closed", pool.state == "closed")
        await task


async def case_no_new_connection_after_shutdown() -> None:
    print("== 7. 停机后绝不新建物理连接 ==")
    async with patched_pool(2) as pool:
        created_before = pool.stats()["total_created"]
        await pool.begin_shutdown()
        for _ in range(3):
            try:
                async with pool.lease():
                    pass
            except pool_db.PoolClosedError:
                pass
        check("停机后建连总数不增长",
              pool.stats()["total_created"] == created_before,
              str(pool.stats()))


async def case_dirty_connection_discarded() -> None:
    print("== 8. 清理不可确认的连接被丢弃而非回队 ==")
    async with patched_pool(2) as pool:
        # 标志位为真时，任何 SQL 都抛 OperationalError（模拟连接断裂）
        FakeConn.break_next = False  # type: ignore[attr-defined]
        original_execute = FakeCursor.execute

        async def boom(self, sql, params=None):
            if getattr(FakeConn, "break_next", False):
                raise psycopg.OperationalError("simulated broken pipe")
            return await original_execute(self, sql, params)

        FakeCursor.execute = boom  # type: ignore[assignment]
        try:
            async with pool.lease() as conn:
                FakeConn.break_next = True  # type: ignore[attr-defined]
                # 退出租约时 full_cleanup 的所有 SQL 都会失败 → 判定丢弃
        finally:
            FakeConn.break_next = False  # type: ignore[attr-defined]
            FakeCursor.execute = original_execute  # type: ignore[assignment]
        await asyncio.sleep(0.05)
        check("脏连接被丢弃计数 >= 1", pool.stats()["total_discarded"] >= 1,
              str(pool.stats()))
        check("脏连接已物理关闭", conn.closed)
        check("丢弃后池仍可正常服务", pool.state == "open")
        async with pool.lease():
            check("丢弃后新租约成功（自动重建）", True)


async def case_http_503_after_shutdown() -> None:
    print("== 9. HTTP 层：停机后 /evaluate 返回 503，/health 汇报 closed ==")
    import httpx
    from app import main as api_main

    FakeConn.reset_registry()
    original = pool_db.AsyncConnection
    pool_db.AsyncConnection = FakeConn  # type: ignore[assignment]
    try:
        api_main.pool = pool_db.TwoConnectionPool(make_settings(2))
        await api_main.pool.open()
        transport = httpx.ASGITransport(app=api_main.app)
        async with httpx.AsyncClient(transport=transport, base_url="http://t") as c:
            h = (await c.get("/health")).json()
            check("停机前 /health 状态 open", h["pool"]["state"] == "open")

            r = await c.post("/admin/shutdown", json={"cancel_inflight": True, "wait": True})
            check("管理停机接口 200", r.status_code == 200, r.text)
            body = r.json()
            check("停机返回体确认无存活连接", body["pool"]["live_connections"] == 0,
                  r.text)

            r = await c.post(
                "/evaluate",
                json={"sample_id": "nightly_warehouse_scan",
                      "candidates": [{"table": "public.package_scan",
                                      "columns": ["warehouse_id"]}]},
            )
            check("停机后 /evaluate 返回 503", r.status_code == 503, r.text)
            check("503 错误码为 pool_closing",
                  r.json().get("error") == "pool_closing", r.text)

            h = (await c.get("/health")).json()
            check("停机后 /health 汇报 closed", h["pool"]["state"] == "closed")
    finally:
        pool_db.AsyncConnection = original  # type: ignore[assignment]


async def case_http_inflight_and_queued_during_shutdown() -> None:
    print("== 10. HTTP 演示：在途与排队评估同时遇到停机（真实租约 + 精确取消）==")
    import httpx
    from app import main as api_main

    FakeConn.reset_registry()
    original = pool_db.AsyncConnection
    pool_db.AsyncConnection = FakeConn  # type: ignore[assignment]
    try:
        api_main.pool = pool_db.TwoConnectionPool(make_settings(2))
        await api_main.pool.open()

        # 用真实租约模拟“在途评估”，并登记进与生产路由同一个在途注册表，
        # /admin/shutdown 据此精确取消（不再依赖协程命名启发式）。
        async def inflight_evaluation() -> None:
            can_leave = asyncio.Event()
            async with api_main.pool.lease() as conn:
                await conn.execute("SELECT 1")  # 打开事务，模拟在途数据库工作
                await can_leave.wait()  # 仅靠停机取消退出

        async def lease_waiter() -> None:
            # 模拟排队等连接的评估请求
            async with api_main.pool.lease():
                pass

        holders = [asyncio.create_task(inflight_evaluation()) for _ in range(2)]
        for t in holders:
            api_main._inflight_evaluations.add(t)
        await asyncio.sleep(0.05)
        queued = asyncio.create_task(lease_waiter())
        await asyncio.sleep(0.05)
        check("两条在途占满连接、第三条排队（in_use=2）",
              api_main.pool.stats()["in_use"] == 2,
              str(api_main.pool.stats()))

        transport = httpx.ASGITransport(app=api_main.app)
        async with httpx.AsyncClient(transport=transport, base_url="http://t",
                                     timeout=30) as c:
            r = await c.post("/admin/shutdown",
                             json={"cancel_inflight": True, "wait": True})
            check("停机接口 200 且同步等完收尾", r.status_code == 200, r.text)
            admin = r.json()
            check("停机接口确认零存活连接",
                  admin["pool"]["live_connections"] == 0, r.text)
            check("识别并取消 2 个在途评估任务",
                  admin["cancelled_inflight_tasks"] == 2, r.text)

        # 排队者拿到 PoolClosedError（HTTP 场景下即 503 的池层来源）
        with contextlib.suppress(pool_db.PoolClosedError):
            await queued
        check("排队者以 PoolClosedError 退出",
              queued.done() and isinstance(queued.exception(),
                                           pool_db.PoolClosedError))
        await asyncio.gather(*holders, return_exceptions=True)
        check("在途任务均已结束", all(t.done() for t in holders))

        # 停机后新到的真实 HTTP 请求 → 503
        transport = httpx.ASGITransport(app=api_main.app)
        async with httpx.AsyncClient(transport=transport, base_url="http://t") as c:
            r = await c.post(
                "/evaluate",
                json={"sample_id": "nightly_warehouse_scan",
                      "candidates": [{"table": "public.package_scan",
                                      "columns": ["warehouse_id"]}]},
            )
            check("停机后新到 /evaluate 返回 503", r.status_code == 503, r.text)
            check("503 错误码为 pool_closing",
                  r.json().get("error") == "pool_closing", r.text)
            h = (await c.get("/health")).json()
            check("收尾后无在途评估", h["pool"]["in_use"] == 0, str(h["pool"]))
            check("收尾后无打开物理连接",
                  h["pool"]["live_connections"] == 0, str(h["pool"]))
        check("物理连接峰值未超过 2", FakeConn.max_live <= 2,
              f"peak={FakeConn.max_live}")
    finally:
        pool_db.AsyncConnection = original  # type: ignore[assignment]


async def case_shutdown_waits_for_natural_completion() -> None:
    print("== 11. cancel_inflight=false：停机等待在途评估自然完成 ==")
    import httpx
    from app import main as api_main

    FakeConn.reset_registry()
    original = pool_db.AsyncConnection
    pool_db.AsyncConnection = FakeConn  # type: ignore[assignment]
    try:
        api_main.pool = pool_db.TwoConnectionPool(make_settings(2))
        await api_main.pool.open()
        release = asyncio.Event()

        async def inflight_evaluation() -> None:
            async with api_main.pool.lease() as conn:
                await conn.execute("SELECT 1")
                await release.wait()

        holder = asyncio.create_task(inflight_evaluation())
        api_main._inflight_evaluations.add(holder)
        await asyncio.sleep(0.05)
        conn = next(c for c in FakeConn._registry if not c.closed)

        transport = httpx.ASGITransport(app=api_main.app)
        async with httpx.AsyncClient(transport=transport, base_url="http://t") as c:
            admin_task = asyncio.create_task(c.post(
                "/admin/shutdown",
                json={"cancel_inflight": False, "wait": True}))
            await asyncio.sleep(0.1)
            check("不取消在途时停机接口仍在等待", not admin_task.done())
            check("等待期间连接保持打开", not conn.closed)
            release.set()
            r = await asyncio.wait_for(admin_task, timeout=5)
            check("在途自然完成后停机接口返回 200", r.status_code == 200, r.text)
            check("取消任务数为 0",
                  r.json()["cancelled_inflight_tasks"] == 0, r.text)
            check("收尾后零存活连接",
                  r.json()["pool"]["live_connections"] == 0, r.text)
        await holder
        check("在途评估正常结束未被取消",
              holder.cancelled() is False and holder.exception() is None)
    finally:
        pool_db.AsyncConnection = original  # type: ignore[assignment]


async def case_uncancellable_survives_repeated_cancel() -> None:
    print("== 12. _uncancellable：连续取消下清理仍跑完且可复用结果 ==")

    async def cleanup_work() -> str:
        await asyncio.sleep(0.03)
        await asyncio.sleep(0.03)
        return "cleaned"

    async def driver() -> str:
        return await pool_db._uncancellable(cleanup_work())

    task = asyncio.create_task(driver())
    await asyncio.sleep(0.01)
    for _ in range(3):
        task.cancel()
        await asyncio.sleep(0.005)
    result = await task
    check("多次取消后仍返回清理结果", result == "cleaned", str(result))
    check("无取消时路径同样正常",
          await pool_db._uncancellable(cleanup_work()) == "cleaned")


async def main() -> int:
    for case in [
        case_basic_lease_and_cap,
        case_shutdown_idle_pool,
        case_queued_waiters_woken,
        case_inflight_completes_then_cleanup_then_close,
        case_inflight_cancelled,
        case_repeated_close_shares_round,
        case_no_new_connection_after_shutdown,
        case_dirty_connection_discarded,
        case_http_503_after_shutdown,
        case_http_inflight_and_queued_during_shutdown,
        case_shutdown_waits_for_natural_completion,
        case_uncancellable_survives_repeated_cancel,
    ]:
        try:
            await case()
        except Exception as exc:  # noqa: BLE001
            check(f"{case.__name__} 未抛异常", False, repr(exc))
    print(f"\n通过 {_passed} 项，失败 {len(_failed)} 项")
    if _failed:
        print("失败明细：")
        for f in _failed:
            print(" -", f)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
