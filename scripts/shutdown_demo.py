#!/usr/bin/env python3
"""在途与排队评估遇到停机的可控演示（仅用 Python 标准库）。

前置：服务已通过 `docker compose up` 启动（DEMO_PAUSE_ENABLED=true）。

演示时序：
  1. 发起 N_INFLIGHT 个带 X-Demo-Pause 的在途评估，占满两条物理连接；
  2. 再发起 N_QUEUED 个普通评估，它们在连接池闸门上排队等待连接；
  3. 调用 POST /admin/shutdown 触发停机交接：
     - 闸门关闭，排队请求被唤醒并以 503 pool_closing 退出；
     - 在途评估被取消，连接经“停查询 → ROLLBACK → hypopg_reset”后物理关闭；
  4. 汇总每个请求的最终 HTTP 状态；
  5. /health 确认 state=closed、无在途评估、无打开物理连接。

用法：
  python3 scripts/shutdown_demo.py [BASE_URL] [--no-cancel] [--pause SECONDS]

选项：
  --no-cancel      对应 cancel_inflight=false：不取消在途，等待其自然完成
  --pause SECONDS  在途评估的演示暂停秒数（默认 20）
"""
from __future__ import annotations

import json
import sys
import threading
import time
import urllib.error
import urllib.request

DEFAULT_URL = "http://localhost:8000"
N_INFLIGHT = 2
N_QUEUED = 3

PAYLOAD = {
    "sample_id": "nightly_warehouse_scan",
    "candidates": [
        {"table": "public.package_scan", "columns": ["warehouse_id", "scan_time"]}
    ],
}


def _request(method: str, url: str, body: dict | None = None,
             headers: dict | None = None, timeout: float = 60.0):
    """返回 (status_code, json_body_or_text)。网络层异常也归一化返回。"""
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(url, data=data, method=method)
    req.add_header("Content-Type", "application/json")
    for k, v in (headers or {}).items():
        req.add_header(k, v)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            raw = resp.read().decode()
            return resp.status, _try_json(raw)
    except urllib.error.HTTPError as exc:
        raw = exc.read().decode()
        return exc.code, _try_json(raw)
    except Exception as exc:  # 连接被拒/超时等
        return -1, repr(exc)


def _try_json(raw: str):
    try:
        return json.loads(raw)
    except Exception:
        return raw


class Call(threading.Thread):
    def __init__(self, label: str, url: str, headers: dict | None = None):
        super().__init__(daemon=True)
        self.label = label
        self._url = url
        self._headers = headers
        self.status: int | None = None
        self.body = None
        self.elapsed = 0.0

    def run(self) -> None:
        start = time.monotonic()
        self.status, self.body = _request(
            "POST", self._url, PAYLOAD, self._headers)
        self.elapsed = time.monotonic() - start


def main(argv: list[str]) -> int:
    base = argv[1] if len(argv) > 1 and not argv[1].startswith("--") else DEFAULT_URL
    cancel_inflight = "--no-cancel" not in argv
    pause = 20
    if "--pause" in argv:
        pause = int(argv[argv.index("--pause") + 1])

    print(f"目标服务: {base}")
    print(f"模式: cancel_inflight={cancel_inflight}, 在途暂停={pause}s\n")

    health = _request("GET", f"{base}/health")[1]
    if isinstance(health, dict):
        print(f"[启动前] pool={json.dumps(health.get('pool'), ensure_ascii=False)}\n")

    # 1) 在途评估（占满两条连接，暂停期间持有租约）
    inflight = [
        Call(f"在途#{i+1}", f"{base}/evaluate",
             headers={"X-Demo-Pause-Seconds": str(pause)})
        for i in range(N_INFLIGHT)
    ]
    for t in inflight:
        t.start()
    time.sleep(1.5)  # 等两条连接被占满、候选已建立

    # 2) 排队评估（无空闲连接，阻塞在租约闸门上）
    queued = [Call(f"排队#{i+1}", f"{base}/evaluate") for i in range(N_QUEUED)]
    for t in queued:
        t.start()
    time.sleep(1.0)  # 确保全部进入排队

    mid = _request("GET", f"{base}/health")[1]
    if isinstance(mid, dict):
        p = mid.get("pool", {})
        print(f"[停机前] state={p.get('state')} in_use={p.get('in_use')} "
              f"idle={p.get('idle')} live={p.get('live_connections')}")
        print(f"         预期：{N_INFLIGHT} 个在途持有连接，{N_QUEUED} 个排队等待\n")

    # 3) 触发停机交接并等待同一轮收尾
    print("==> 调用 POST /admin/shutdown 触发停机交接 ...")
    status, body = _request(
        "POST", f"{base}/admin/shutdown",
        body={"cancel_inflight": cancel_inflight, "wait": True},
        timeout=pause + 30,
    )
    print(f"[停机接口] HTTP {status}")
    if isinstance(body, dict):
        print(f"  pool_state={body.get('pool_state')} "
              f"cancelled_inflight_tasks={body.get('cancelled_inflight_tasks')} "
              f"waited={body.get('waited_for_completion')}")
        print(f"  pool={json.dumps(body.get('pool'), ensure_ascii=False)}\n")

    # 4) 汇总各评估请求的最终状态
    for t in inflight + queued:
        t.join(timeout=pause + 30)
        body_err = ""
        if isinstance(t.body, dict):
            body_err = t.body.get("error", "")
        print(f"  {t.label:>6}: HTTP {t.status} error={body_err!r} "
              f"耗时={t.elapsed:.2f}s")

    # 5) 收尾验证：停机后新请求 503；/health 零连接零在途
    post_status, post_body = _request(
        "POST", f"{base}/evaluate", PAYLOAD)
    print(f"\n[停机后新请求] HTTP {post_status} "
          f"error={post_body.get('error') if isinstance(post_body, dict) else post_body!r}")

    h_status, health_after = _request("GET", f"{base}/health")
    ok = False
    if isinstance(health_after, dict):
        p = health_after.get("pool", {})
        ok = (
            p.get("state") == "closed"
            and p.get("live_connections") == 0
            and p.get("in_use") == 0
        )
        print(f"[收尾后 /health] {json.dumps(p, ensure_ascii=False)}")

    all_503 = all(t.status == 503 for t in inflight + queued)
    print("\n== 断言 ==")
    print(f"  在途与排队评估全部以 503 退出: {'PASS' if all_503 else 'FAIL'}")
    print(f"  停机后无打开连接、无在途评估: {'PASS' if ok else 'FAIL'}")
    print(f"  停机后新请求被拒绝(503):      {'PASS' if post_status == 503 else 'FAIL'}")
    print("\n演示结束。连接池已处于 closed，重启 API 容器后可再次评估。")
    return 0 if (all_503 and ok and post_status == 503) else 1


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
