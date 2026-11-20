"""行为路径路由：构建路径Sankey并获取分析结果。"""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException, Request

from app.schemas import ActionPayload, ActionResult
from app.services.behavior_path import (
    build_path_sankey,
    get_path_result,
    find_common_paths,
    update_time_window,
)
from app.services.event_stream import SessionSplitError
from app.store import store, query_filters

router = APIRouter(prefix="/api/behavior-path", tags=["behavior_path"])


def _graph_view() -> dict[str, Any]:
    """把路径模型与其构图产物组装成 DAG 视图（节点/边）。"""
    nodes: list[dict[str, Any]] = []
    edges: list[dict[str, Any]] = []
    for row in store.rows("behavior_path"):
        nodes.append({
            "id": row["id"],
            "model_id": row["model_id"],
            "name": row["model_id"],
            "label": row["model_id"],
            "status": row.get("status", ""),
            "start_event": row.get("start_event"),
            "target_event": row.get("target_event"),
            "time_window": row.get("time_window"),
        })
        artifact = store.get_artifact("behavior_path", row["model_id"])
        if artifact:
            for link in artifact.get("sankey_links", []):
                edges.append({
                    "from": link.get("source_name"),
                    "to": link.get("target_name"),
                    "label": link.get("value"),
                    "model_id": row["model_id"],
                })
    return {"nodes": nodes, "edges": edges}


@router.get("")
def index(request: Request, page: int = 1, size: int = 20) -> dict[str, Any]:
    rows = store.rows("behavior_path")
    filters = query_filters(request)
    if filters:
        rows = [r for r in rows if all(str(r.get(k)) == str(v) for k, v in filters.items())]
    total = len(rows)
    start = (page - 1) * size
    graph = _graph_view()
    return {
        "items": rows[start : start + size],
        "total": total,
        "page": page,
        "size": size,
        "nodes": graph["nodes"],
        "edges": graph["edges"],
    }


@router.get("/export")
def export_entries() -> dict[str, Any]:
    rows = store.rows("behavior_path")
    return {"items": rows, "total": len(rows)}


@router.post("")
def create(payload: dict[str, Any]) -> ActionResult:
    values = payload.get("values", payload)
    row = store.insert("behavior_path", dict(values))
    return ActionResult(ok=True, message="Created", entry=row)


@router.put("/{entry_id}")
def update(entry_id: int, payload: dict[str, Any]) -> ActionResult:
    row = store.find("behavior_path", entry_id)
    if row is None:
        raise HTTPException(status_code=404, detail="Not found")
    values = payload.get("values", payload)
    updated = store.update("behavior_path", entry_id, values)
    return ActionResult(ok=True, message="Updated", entry=updated)


@router.post("/{model_id}/time-window")
def adjust_time_window(model_id: str, payload: dict[str, Any]) -> ActionResult:
    """调整时间窗口：旧图立即失效，下次构图按新窗口重新切分事件。"""
    try:
        time_window = int(payload.get("time_window"))
    except (TypeError, ValueError):
        raise HTTPException(status_code=422, detail="time_window 必须为正整数秒")
    if time_window <= 0:
        raise HTTPException(status_code=422, detail="time_window 必须为正整数秒")
    try:
        row = update_time_window(model_id, time_window)
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    return ActionResult(ok=True, message="时间窗口已更新，旧图已失效", entry=row)


@router.get("/{entry_id}")
def detail(entry_id: int) -> dict[str, Any]:
    row = store.find("behavior_path", entry_id)
    if row is None:
        raise HTTPException(status_code=404, detail="Not found")
    return row


@router.post("/{model_id}/build")
def build(model_id: str) -> ActionResult:
    try:
        result = build_path_sankey(model_id)
    except SessionSplitError as e:
        # 切割失败：没有可确认的新图，不允许沿用旧时间边界
        raise HTTPException(status_code=409, detail=str(e))
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    return ActionResult(ok=True, message="Built", entry=result.model_dump())


@router.get("/{model_id}/result")
def result(model_id: str) -> dict[str, Any]:
    res = get_path_result(model_id)
    if res is None:
        raise HTTPException(status_code=404, detail="Result not found")
    return res


@router.get("/{model_id}/path-details")
def path_details(model_id: str) -> dict[str, Any]:
    """路径明细：每条目标边可追溯到窗口内一笔事件，含晚到排除记录。"""
    res = get_path_result(model_id)
    if res is None:
        raise HTTPException(status_code=404, detail="Result not found")
    return {
        "model_id": model_id,
        "stale": res.get("stale", False),
        "stale_reason": res.get("stale_reason", ""),
        "time_window": res.get("time_window"),
        "late_target_excluded": res.get("late_target_excluded", 0),
        "out_of_window_events": res.get("out_of_window_events", 0),
        "items": res.get("path_details", []),
        "total": len(res.get("path_details", [])),
    }


@router.get("/{model_id}/common-paths")
def common_paths(model_id: str, min_count: int = 3) -> list[dict[str, Any]]:
    return find_common_paths(model_id, min_count)


@router.post("/{entry_id}/actions")
def run_action(entry_id: int, payload: ActionPayload) -> ActionResult:
    row = store.find("behavior_path", entry_id)
    if row is None:
        raise HTTPException(status_code=404, detail="Not found")
    action = payload.action
    row["status"] = action
    row["pending"] = False
    return ActionResult(ok=True, message=action, entry=row)
