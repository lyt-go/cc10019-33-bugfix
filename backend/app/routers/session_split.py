"""会话切割路由：执行会话切割并获取结果。"""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException, Request

from app.schemas import ActionPayload, ActionResult
from app.services.session_split import split_sessions, get_session_result, resegment_with_strategy
from app.services.event_stream import SessionSplitError
from app.store import store, query_filters

router = APIRouter(prefix="/api/session-split", tags=["session_split"])


@router.get("")
def index(request: Request, page: int = 1, size: int = 20) -> dict[str, Any]:
    rows = store.rows("session_split")
    filters=query_filters(request)
    if filters:
        rows = [r for r in rows if all(str(r.get(k)) == str(v) for k, v in filters.items())]
    total = len(rows)
    start = (page - 1) * size
    return {"items": rows[start : start + size], "total": total, "page": page, "size": size}


@router.get("/{entry_id}")
def detail(entry_id: int) -> dict[str, Any]:
    row = store.find("session_split", entry_id)
    if row is None:
        raise HTTPException(status_code=404, detail="Not found")
    return row


@router.post("/{strategy_id}/run")
def run(strategy_id: str) -> ActionResult:
    try:
        result = split_sessions(strategy_id)
        return ActionResult(ok=True, message="Split", entry=result.model_dump())
    except SessionSplitError as e:
        # 切割失败：未生成可确认的新产物，重试必须基于当前配置重新切割
        raise HTTPException(status_code=409, detail=str(e))
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))


@router.get("/{strategy_id}/result")
def result(strategy_id: str) -> dict[str, Any]:
    res = get_session_result(strategy_id)
    if res is None:
        raise HTTPException(status_code=404, detail="Result not found")
    return res


@router.post("/{strategy_id}/resegment")
def resegment(strategy_id: str, payload: dict[str, Any]) -> ActionResult:
    timeout = payload.get("timeout_sec")
    try:
        result = resegment_with_strategy(strategy_id, timeout)
        return ActionResult(ok=True, message="Resegmented", entry=result.model_dump())
    except SessionSplitError as e:
        raise HTTPException(status_code=409, detail=str(e))
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))


@router.post("/{entry_id}/actions")
def run_action(entry_id: int, payload: ActionPayload) -> ActionResult:
    row = store.find("session_split", entry_id)
    if row is None:
        raise HTTPException(status_code=404, detail="Not found")
    action = payload.action
    row["status"] = action
    row["pending"] = False
    return ActionResult(ok=True, message=action, entry=row)
