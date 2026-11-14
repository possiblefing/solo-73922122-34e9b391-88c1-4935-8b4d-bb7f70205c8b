"""FastAPI 入口：HypoPG 候选索引成本评估 API。

仅暴露只读评估接口；查询不执行（EXPLAIN 无 ANALYZE），索引不落地（HypoPG 会话级假想）。
"""
from __future__ import annotations

import asyncio
import logging
import uuid

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from psycopg.rows import dict_row
from pydantic import BaseModel, Field

from .config import get_settings
from .db import PoolClosedError, TwoConnectionPool
from .samples import SAMPLES
from .schemas import EvaluateRequest, EvaluateResponse, HealthResponse
from .service import EvaluationError, evaluate
from .validation import SQLValidationError

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
log = logging.getLogger("hypopg_api")

settings = get_settings()
pool = TwoConnectionPool(settings)

# 在途评估任务注册表：仅登记持有/等待连接租约的评估，停机时据此精确取消，
# 不依赖脆弱的协程名匹配，也不会误取消停机收尾任务本身。
_inflight_evaluations: set[asyncio.Task] = set()

app = FastAPI(
    title="夜间包裹扫描查询 · 候选索引成本评估 API",
    description=(
        "提交只读 SELECT 与最多 3 个 B-tree 候选索引（表名 + 有序列名），"
        "基于 PostgreSQL + HypoPG 返回无候选/启用全部候选的 EXPLAIN JSON、"
        "总成本对比与被采纳候选。**只估算**：不执行查询，也不创建真实索引。"
    ),
    version="1.0.0",
)


@app.on_event("startup")
async def _startup() -> None:
    await pool.open()
    log.info("连接池已就绪，物理连接上限 = %s", settings.pool_max_connections)


@app.on_event("shutdown")
async def _shutdown() -> None:
    # 真实进程停机走与管理接口完全相同的交接路径：拒绝新租约、唤醒排队者、
    # 等在途评估结束/被取消并完成清理，再逐条关闭物理连接。close 幂等，
    # 即使管理接口已经触发过停机，这里也只是等待同一轮收尾。
    await pool.close()
    log.info("连接池已关闭")


@app.exception_handler(SQLValidationError)
async def _sql_validation_handler(_: Request, exc: SQLValidationError):
    return JSONResponse(status_code=400, content={"error": "invalid_sql", "detail": str(exc)})


@app.exception_handler(EvaluationError)
async def _evaluation_handler(_: Request, exc: EvaluationError):
    return JSONResponse(status_code=400, content={"error": "evaluation_rejected", "detail": str(exc)})


@app.exception_handler(PoolClosedError)
async def _pool_closed_handler(_: Request, exc: PoolClosedError):
    # 停机期间：排队/新到请求统一 503，不会在关闭后的池上继续评估
    return JSONResponse(
        status_code=503,
        content={"error": "pool_closing", "detail": str(exc)},
        headers={"Retry-After": "30"},
    )


@app.get("/health", response_model=HealthResponse, tags=["ops"])
async def health() -> HealthResponse:
    """存活探针 + 连接池观测（演示两条连接上限与复用/丢弃/停机计数）。"""
    db_info: dict
    if pool.state != "open":
        # 停机中/已关闭不再申请租约，直接汇报状态（探针本身不阻碍收尾）
        db_info = {"connected": False, "state": pool.state}
    else:
        db_info = {"connected": False}
        try:
            async with pool.lease() as conn:
                async with conn.cursor(row_factory=dict_row) as cur:
                    await cur.execute(
                        "SELECT current_database() AS db, current_user AS usr, "
                        "       (SELECT extversion FROM pg_extension WHERE extname='hypopg') AS hypopg"
                    )
                    db_info = {"connected": True, **(await cur.fetchone())}
        except PoolClosedError as exc:
            db_info = {"connected": False, "state": pool.state, "error": repr(exc)}
        except Exception as exc:  # 健康检查不应抛 500 掩盖状态
            db_info = {"connected": False, "error": repr(exc)}
    return HealthResponse(status="ok", pool=pool.stats(), db=db_info)


@app.get("/samples", tags=["evaluation"])
async def list_samples() -> dict:
    """预置样例 SQL 及能/不能改善计划的候选示例。"""
    return {
        "samples": [
            {
                "sample_id": s.sample_id,
                "title": s.title,
                "sql_text": s.sql_text,
                "helpful_candidates": [c.__dict__ for c in s.helpful_candidates],
                "useless_candidates": [c.__dict__ for c in s.useless_candidates],
            }
            for s in SAMPLES
        ]
    }


class ShutdownRequest(BaseModel):
    cancel_inflight: bool = Field(
        default=True,
        description="是否取消正在执行的评估任务（模拟重启时 uvicorn 取消请求任务）；"
        "false 则等待在途评估自然完成",
    )
    wait: bool = Field(
        default=True,
        description="是否同步等待本轮停机收尾完成（所有物理连接关闭）后再返回",
    )


@app.post("/admin/shutdown", tags=["ops"])
async def admin_shutdown(req: ShutdownRequest, request: Request) -> JSONResponse:
    """演示/运维入口：触发连接池停机交接。

    - 调用后立即拒绝新租约，排队等连接的请求收到 503 退出；
    - cancel_inflight=true 时取消在途评估（等价于进程重启对请求任务的取消），
      连接经“取消查询 → ROLLBACK → hypopg_reset 验证”后物理关闭；
      false 时等待在途评估自然跑完再关闭其连接；
    - wait=true 时等同一轮收尾结束再返回，返回体中 live_connections 必为 0。
    """
    event = await pool.begin_shutdown()

    # cancel_inflight=true：取消全部在途评估（等价于进程重启时 uvicorn 取消
    # 请求任务）。租约层会保证每个被取消的评估完成“停查询 → ROLLBACK →
    # hypopg_reset 验证 → 释放闸门 → 物理关连接”的完整收尾。
    # false：不取消，收尾事件会等待在途评估自然跑完、连接清理归还后再关闭。
    inflight = list(_inflight_evaluations) if req.cancel_inflight else []
    for task in inflight:
        task.cancel()

    if req.wait:
        await event.wait()

    return JSONResponse(
        content={
            "shutdown_initiated": True,
            "pool_state": pool.state,
            "cancelled_inflight_tasks": len(inflight),
            "waited_for_completion": req.wait,
            "pool": pool.stats(),
        }
    )


@app.post(
    "/evaluate",
    response_model=EvaluateResponse,
    tags=["evaluation"],
    responses={
        400: {"description": "SQL 非法、表/列不存在或候选参数错误（不返回部分比较）"},
        500: {"description": "数据库内部错误；连接已清理后归还或丢弃"},
        503: {"description": "连接池停机中，新租约被拒绝（排队请求同步退出）"},
    },
)
async def evaluate_query(req: EvaluateRequest, request: Request) -> EvaluateResponse:
    """执行一次完整的基线 vs 全候选成本评估。

    客户端在处理中断连时，服务端仍会等待服务端查询取消、事务回滚与
    hypopg_reset 全部完成后才归还连接（详见连接池实现）。
    """
    request_id = uuid.uuid4().hex
    pause_seconds = 0.0
    raw_pause = request.headers.get("x-demo-pause-seconds")
    if raw_pause:
        try:
            pause_seconds = float(raw_pause)
            if pause_seconds < 0:
                raise ValueError
        except ValueError:
            raise SQLValidationError(
                "X-Demo-Pause-Seconds 必须是非负数字（单位秒）"
            )
    pause_mode = request.headers.get("x-demo-pause-mode", "asyncio").lower()
    if pause_mode not in ("asyncio", "pg_sleep"):
        raise SQLValidationError(
            "X-Demo-Pause-Mode 只允许 asyncio 或 pg_sleep"
        )

    log.info(
        "评估请求 %s 开始：候选数=%d 演示暂停=%.1fs 模式=%s",
        request_id, len(req.candidates), pause_seconds, pause_mode,
    )
    current_task = asyncio.current_task()
    if current_task is not None:
        # 登记为在途评估，供 /admin/shutdown 精确取消（进程停机由 lifespan 处理）
        _inflight_evaluations.add(current_task)
    try:
        return await _run_evaluation(request_id, req, request,
                                     pause_seconds, pause_mode)
    finally:
        if current_task is not None:
            _inflight_evaluations.discard(current_task)


async def _run_evaluation(request_id, req, request, pause_seconds, pause_mode):
    try:
        result = await evaluate(
            pool, settings, req,
            pause_seconds=pause_seconds,
            pause_mode=pause_mode,
            is_disconnected=request.is_disconnected,
        )
    except (SQLValidationError, EvaluationError):
        raise
    except PoolClosedError:
        # 让全局处理器统一映射为 503
        raise
    except asyncio.CancelledError:
        # 走到这里时租约层已完成取消安全清理（停查询 → ROLLBACK → hypopg_reset
        # 验证 → 释放闸门）。若取消来自连接池停机，转成干净的 503 响应，
        # 避免客户端只看到无响应的连接中断；普通客户端断连则继续向上抛。
        if pool.state != "open":
            raise PoolClosedError(
                "评估在连接池停机期间被取消：数据库工作已停止，事务与假想索引已清理"
            )
        raise
    except Exception as exc:
        # 失败不返回部分比较；租约上下文已保证清理完成
        log.exception("评估请求 %s 失败: %r", request_id, exc)
        return JSONResponse(
            status_code=500,
            content={
                "error": "evaluation_failed",
                "request_id": request_id,
                "detail": "数据库评估失败，未产生比较结果；连接已完成清理",
            },
        )
    log.info(
        "评估请求 %s 完成：基线成本=%.2f 候选成本=%.2f 采纳=%d 拒绝=%d",
        request_id,
        result.baseline_total_cost,
        result.hypothetical_total_cost,
        len(result.adopted_candidates),
        len(result.rejected_candidates),
    )
    return result
