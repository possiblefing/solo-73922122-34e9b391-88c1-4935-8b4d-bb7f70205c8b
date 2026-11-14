"""两条连接上限的租约式连接池 + 取消安全的清理 + 严格的停机交接。

关键设计：
- 物理连接数硬上限为 2（可关闭的租约闸门 + 空闲队列），单 uvicorn worker
  单进程内全局复用；
- HypoPG 的假想索引是 **会话级** 状态，天然按连接隔离，并发评估互不可见；
- 每次借出连接先做 reset 校验（ROLLBACK + hypopg_reset），基线绝不继承上一请求遗留候选；
- 请求报错或客户端断连（CancelledError）时，先让服务端查询真正停止
  （conn.cancel() 并等待协程退出），再回滚事务、清空假想索引，全部完成后才归还；
- 清理无法确认干净的连接直接关闭丢弃，绝不放回队列；迟到的清理作用于当前请求自己的
  连接，下一请求只会从队列取到已验证干净的连接，因此不会被取消或误删候选。

停机交接（close / begin_shutdown）保证：
1. 停机一开始即拒绝一切新租约，且唤醒所有正在排队等连接的请求，使其立刻退出；
2. 已持有连接的在途评估可以自然跑完，或被外部取消后走标准取消安全清理；
3. 每条连接在物理关闭前，都必须等其数据库工作停止、事务回滚、假想索引清空
   （即复用同一条 full_cleanup 路径）；
4. close() 等待同一轮停机收尾全部完成，重复调用共享同一个等待事件，
   返回时不存在任何打开的物理连接，也不存在仍在执行的评估。
"""
from __future__ import annotations

import asyncio
import contextlib
import logging

import psycopg
from psycopg import AsyncConnection

from .config import Settings

log = logging.getLogger("hypopg_api.pool")

_CANCEL_WAIT_SECONDS = 10.0
_RESET_RETRIES = 2
_SHUTDOWN_POLL_SECONDS = 0.02


class PoolClosedError(RuntimeError):
    """连接池已进入停机（或已关闭），不再发放新租约。"""


class _LeaseGate:
    """可关闭的有界租约闸门：等价于一个支持“关闭即唤醒全部等待者”的信号量。

    普通 Semaphore 不支持在关闭时唤醒排队者（等待 acquire 的协程会继续阻塞，
    停机后仍可能拿到许可继续评估），因此基于 Condition 自行实现：
    - 可用许可数 _permits 始终等于“当前可再发放的租约数”（上限 limit）；
    - close() 后所有当前与未来的 acquire() 立即抛 PoolClosedError，排队者全部唤醒。
    """

    def __init__(self, limit: int) -> None:
        self._cond = asyncio.Condition()
        self._permits = limit
        self._closed = False

    async def acquire(self) -> None:
        async with self._cond:
            await self._cond.wait_for(lambda: self._closed or self._permits > 0)
            if self._closed:
                raise PoolClosedError("连接池正在关闭，排队请求已被拒绝")
            self._permits -= 1

    async def release(self) -> None:
        async with self._cond:
            self._permits += 1
            # 停机期间不再发放，只负责唤醒等待“全部租约归还”的收尾协程
            self._cond.notify_all()

    async def close(self) -> None:
        async with self._cond:
            self._closed = True
            # 唤醒所有排队 acquire 与等待租约归零的收尾协程
            self._cond.notify_all()

    @property
    def closed(self) -> bool:
        return self._closed

    async def available_permits(self) -> int:
        async with self._cond:
            return self._permits


class TwoConnectionPool:
    """租约闸门限流 + 空闲队列复用，物理连接永不超过 max_connections（=2）。"""

    def __init__(self, settings: Settings) -> None:
        self._settings = settings
        self._max = settings.pool_max_connections
        self._gate = _LeaseGate(self._max)
        self._idle: asyncio.Queue[AsyncConnection] = asyncio.Queue()
        self._closing = False
        self._closed = False
        # 本轮停机的等待事件：所有 close() 调用共享，重复关闭等待同一轮收尾
        self._close_event: asyncio.Event | None = None
        self._shutdown_task: asyncio.Task | None = None
        self._close_lock = asyncio.Lock()
        # 全量在管连接（空闲 + 在途），停机收尾时逐条核对，杜绝遗漏
        self._live: set[AsyncConnection] = set()
        self._total_created = 0
        self._total_discarded = 0
        self._verifications = 0
        self._resets = 0
        self._cancels = 0
        self._rejected_while_closing = 0
        self._in_use = 0
        self._stats_lock = asyncio.Lock()

    # ---------- 生命周期 ----------

    async def open(self) -> None:
        if self._closed or self._closing:
            raise PoolClosedError("连接池已关闭，不能重新打开")
        # 预建一条连接，提前暴露数据库不可达问题
        conn = await self._new_connection()
        await self._idle.put(conn)

    async def begin_shutdown(self) -> asyncio.Event:
        """开始停机：拒绝新租约、唤醒全部排队者，并安排后台收尾。

        幂等：重复调用返回同一个事件，不会开启新一轮停机。
        """
        async with self._close_lock:
            if self._close_event is None:
                self._closing = True
                self._close_event = asyncio.Event()
                await self._gate.close()
                self._shutdown_task = asyncio.create_task(self._run_shutdown())
                log.info("连接池停机开始：已拒绝新租约，排队请求将立即退出")
            return self._close_event

    async def close(self) -> None:
        """关闭连接池并等待同一轮停机收尾全部结束。

        可安全重复调用：所有调用共享同一个停机事件，后到者只等待不重复收尾。
        close 返回时保证：没有任何打开的物理连接，也没有仍在执行的评估。
        """
        event = await self.begin_shutdown()
        await event.wait()

    async def _run_shutdown(self) -> None:
        """停机收尾主体：等在途评估结束/取消，逐条清理后关闭物理连接。"""
        try:
            # 1) 等待全部在途租约归还。租约退出前会完成 full_cleanup
            #    （停掉数据库工作、ROLLBACK、hypopg_reset 并验证），
            #    因此等到这里时在途评估必然已经结束。
            while await self._gate.available_permits() < self._max:
                await asyncio.sleep(_SHUTDOWN_POLL_SECONDS)

            # 2) 排空空闲队列（正常归还的连接在此；停机期间归还的连接已在
            #    _checkin 直接关闭，这里主要处理停机前就空闲的连接）
            while True:
                try:
                    conn = self._idle.get_nowait()
                except asyncio.QueueEmpty:
                    break
                # 防御性再清理一次：归还路径已验证干净，这里失败则直接关闭
                with contextlib.suppress(Exception):
                    await full_cleanup(conn, cancelled=False)
                await self._dispose(conn, reason="shutdown-idle")

            # 3) 核对全量在管连接，任何漏网之鱼都在此兜底关闭
            for conn in list(self._live):
                await self._dispose(conn, reason="shutdown-final")

            # 4) 双重验证：不存在打开的物理连接
            leaked = [c for c in self._live if not c.closed]
            if leaked:  # 理论不可达
                log.error("停机后仍有 %d 条未关闭连接，强制关闭", len(leaked))
                for conn in leaked:
                    await self._silent_close(conn)
                self._live.difference_update(leaked)

            if self._in_use != 0:
                log.error("停机收尾时 in_use=%s（期望 0）", self._in_use)
            log.info(
                "连接池停机收尾完成：累计建连=%d 累计丢弃=%d",
                self._total_created, self._total_discarded,
            )
        except Exception:
            log.exception("停机收尾发生未预期异常")
            raise
        finally:
            self._closing = False
            self._closed = True
            assert self._close_event is not None
            self._close_event.set()

    # ---------- 内部工具 ----------

    async def _new_connection(self) -> AsyncConnection:
        # 停机开始后绝不新建物理连接（防御：避免收尾期间冒出新连接）
        if self._gate.closed:
            raise PoolClosedError("连接池正在关闭，禁止新建物理连接")
        conn = await AsyncConnection.connect(
            self._settings.db_dsn,
            autocommit=False,
            application_name="hypopg-eval-api",
        )
        self._total_created += 1
        self._live.add(conn)
        # 会话级只读 + 超时：即使校验被绕过，数据库也拒绝写入；慢查询有兜底
        async with conn.cursor() as cur:
            await cur.execute(
                psycopg.sql.SQL("SET statement_timeout = {}").format(
                    psycopg.sql.Literal(self._settings.statement_timeout_ms)
                )
            )
            await cur.execute("SET default_transaction_read_only = on")
            await cur.execute("SET lock_timeout = 2000")
            await cur.execute("SET idle_in_transaction_session_timeout = 60000")
            await cur.execute("ROLLBACK")
            await cur.execute("SELECT hypopg_reset()")
            await cur.execute("COMMIT")
        return conn

    async def _silent_close(self, conn: AsyncConnection) -> None:
        with contextlib.suppress(Exception):
            await conn.close()

    async def _dispose(self, conn: AsyncConnection, *, reason: str) -> None:
        """关闭一条连接并从在管集合移除（幂等）。"""
        if conn not in self._live:
            await self._silent_close(conn)
            return
        self._live.discard(conn)
        if not conn.closed:
            log.info("关闭物理连接（%s）", reason)
            await self._silent_close(conn)

    async def _checkin(self, conn: AsyncConnection, *, discard: bool) -> None:
        async with self._stats_lock:
            self._in_use -= 1
            if discard:
                self._total_discarded += 1
        if discard:
            log.warning("连接无法确认已清理干净，丢弃该物理连接（不归还队列）")
            await self._dispose(conn, reason="cleanup-failed")
        elif self._gate.closed:
            # 停机期间归还的干净连接不再回队，确认清理完成后直接物理关闭
            await self._dispose(conn, reason="return-during-shutdown")
        else:
            await self._idle.put(conn)
        await self._gate.release()

    async def _acquire_verified(self) -> AsyncConnection:
        """取得一条经过干净校验的连接（队列复用或新建）。"""
        try:
            conn = self._idle.get_nowait()
        except asyncio.QueueEmpty:
            return await self._new_connection()

        # 复用前验证：事务已结束、无遗留假想索引；不达标则重置，重置失败则丢弃重建。
        self._verifications += 1
        ok = await full_cleanup(conn, cancelled=False)
        if not ok:
            await self._dispose(conn, reason="verify-failed")
            async with self._stats_lock:
                self._total_discarded += 1
            return await self._new_connection()
        self._resets += 1
        return conn

    async def _finish(
        self, conn: AsyncConnection | None, *, cancelled: bool, acquired: bool
    ) -> None:
        """统一收尾：完整清理后才归还/释放闸门。整个过程屏蔽取消。"""
        if conn is None:
            if acquired:
                await self._gate.release()
                async with self._stats_lock:
                    self._in_use -= 1
            return
        # 屏蔽取消：停机/断连期间清理必须跑完，确保服务端工作停止、状态清空
        ok = await _uncancellable(full_cleanup(conn, cancelled=cancelled))
        await _uncancellable(self._checkin(conn, discard=not ok))

    # ---------- 对外租约 API ----------

    @contextlib.asynccontextmanager
    async def lease(self):
        # 快速路径：已停机则立即拒绝，不进入排队
        if self._gate.closed:
            async with self._stats_lock:
                self._rejected_while_closing += 1
            raise PoolClosedError("连接池已关闭")

        acquired = False
        conn: AsyncConnection | None = None
        try:
            # 停机可能恰好在检查之后发生：闸门关闭会唤醒此处的排队等待并抛错，
            # 因此“正在等待连接的请求”在停机后一定退出，不会继续评估。
            await self._gate.acquire()
            acquired = True
            async with self._stats_lock:
                self._in_use += 1
            conn = await self._acquire_verified()
            yield conn
        except asyncio.CancelledError:
            # 客户端断连或停机取消：当前请求任务被取消。先让服务端工作真正停下来、
            # 清掉事务与假想索引，再释放闸门，否则下一个请求可能拿到脏连接。
            log.info("请求被取消（客户端断连/停机），开始取消安全清理")
            async with self._stats_lock:
                self._cancels += 1
            await self._finish(conn, cancelled=True, acquired=acquired)
            conn = None
            raise
        except PoolClosedError:
            # 排队期间被停机唤醒：直接退出，绝不触碰任何连接
            if acquired:
                await self._gate.release()
                async with self._stats_lock:
                    self._in_use -= 1
            async with self._stats_lock:
                self._rejected_while_closing += 1
            raise
        except BaseException:
            # 其他异常同样完整清理后再归还，失败不返回部分比较
            await self._finish(conn, cancelled=False, acquired=acquired)
            conn = None
            raise
        else:
            await self._finish(conn, cancelled=False, acquired=acquired)
            conn = None

    @property
    def state(self) -> str:
        if self._closed:
            return "closed"
        if self._closing:
            return "closing"
        return "open"

    def stats(self) -> dict:
        return {
            "state": self.state,
            "max_connections": self._max,
            "in_use": self._in_use,
            "idle": self._idle.qsize(),
            "live_connections": len(self._live),
            "total_created": self._total_created,
            "total_discarded": self._total_discarded,
            "checkout_verifications": self._verifications,
            "resets_performed": self._resets,
            "cancels_performed": self._cancels,
            "rejected_while_closing": self._rejected_while_closing,
        }


# ---------------- 模块级清理原语 ----------------


async def _uncancellable(coro):
    """让清理逻辑屏蔽再次送达的 CancelledError，跑完为止。

    协程只允许被调度一次，因此先包成 Task（底层协程仅运行一次），再对 Task
    反复 shield/await；Task 被取消时抛 CancelledError，而其包装的清理协程
    仍在后台继续运行，重试 await 同一个 Task 即可拿到最终结果。
    （直接对同一协程重复 asyncio.shield 会在首次取消后抛
    "cannot reuse already awaited coroutine"。）
    """
    task = asyncio.ensure_future(coro)
    while not task.done():
        try:
            return await asyncio.shield(task)
        except asyncio.CancelledError:
            # 清理期间再次收到取消：继续等，绝不半途而废
            continue
    return task.result()


async def _reset_session_state(conn: AsyncConnection) -> bool:
    """ROLLBACK + hypopg_reset，带少量重试并验证。成功返回 True。

    注意：非自动提交模式下每条 SQL 都会隐式开启事务，因此 hypopg_reset()
    之后必须再发一个 ROLLBACK 收尾，协议层事务状态才能回到 IDLE。
    HypoPG 假想索引是后端内存中的非事务状态，ROLLBACK 不会让 reset 失效。
    """
    for attempt in range(_RESET_RETRIES + 1):
        try:
            async with conn.cursor() as cur:
                await cur.execute("ROLLBACK")
                await cur.execute("SELECT hypopg_reset()")
                # 验证查询要放在收尾 ROLLBACK 之前（查询本身会开启隐式事务）
                await cur.execute("SELECT count(*) FROM hypopg_list_indexes")
                hypo_count = (await cur.fetchone())[0]
                await cur.execute("ROLLBACK")
            # 验证 1：协议层事务状态必须回到 IDLE（无打开事务/在途查询）
            if conn.info.transaction_status != psycopg.pq.TransactionStatus.IDLE:
                log.warning(
                    "重置后事务状态非 IDLE: %s", conn.info.transaction_status
                )
                continue
            # 验证 2：HypoPG 假想索引必须为零
            if hypo_count == 0:
                return True
            log.warning("重置后仍残留 %s 个假想索引", hypo_count)
        except (psycopg.OperationalError, psycopg.InterfaceError):
            raise
        except Exception as exc:
            log.warning("会话重置第 %s 次尝试失败: %r", attempt + 1, exc)
            with contextlib.suppress(Exception):
                await conn.rollback()
    return False


async def full_cleanup(conn: AsyncConnection, *, cancelled: bool) -> bool:
    """确保连接上的真实数据库工作已停止，且事务/假想索引全部清空。

    返回 True 表示连接干净可复用；False 表示无法保证，调用方必须丢弃。
    """
    # 1) 连接不在 idle（查询在跑/事务打开）：先发取消并等待，确保服务端工作停止。
    #    注意 psycopg3 的 cancel() 是同步阻塞方法（内部等待查询确认终止），
    #    必须放到线程中调用，绝不能直接 await（其返回值为 None）。
    if conn.info.transaction_status != psycopg.pq.TransactionStatus.IDLE:
        try:
            await asyncio.wait_for(
                asyncio.to_thread(conn.cancel), timeout=_CANCEL_WAIT_SECONDS
            )
        except Exception as exc:
            # 取消信号无法送达或查询未在时限内终止：连接无法保证可复用，
            # 交由下方 reset 验证失败后关闭丢弃（绝不脏连接回队）
            log.warning("conn.cancel() 未成功终止查询: %r", exc)
        # 让出事件循环，让被取消的查询协程收到 QueryCanceled 并退出
        await asyncio.sleep(0)

    # 2) 回滚事务并清空 HypoPG 假想索引（重试 + 验证）
    try:
        return await _reset_session_state(conn)
    except (psycopg.OperationalError, psycopg.InterfaceError) as exc:
        log.warning("连接已不可用，清理判定为丢弃: %r", exc)
        await _silent_close_module(conn)
        return False
    except Exception as exc:  # 防御性兜底
        log.error("清理发生未预期异常，连接丢弃: %r", exc)
        await _silent_close_module(conn)
        return False


async def _silent_close_module(conn: AsyncConnection) -> None:
    with contextlib.suppress(Exception):
        await conn.close()
