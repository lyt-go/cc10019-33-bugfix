"""行为路径分析引擎：基于会话切割产物，按配置时间窗口切分有效事件范围并构图。

核心规则：
1. 事件只来源于关联会话切割策略（session_split_rule）的切割产物，不再自行造数；
2. 每个会话以首个起始事件为锚点，有效范围为 [锚点时间, 锚点时间 + time_window)；
3. 窗口外（含晚到）的目标事件不计入转化，只记录为可核对的排除明细；
4. 未转化的窗口内片段同样进入 Sankey 与常见路径统计，并显式标注 converted=false；
5. 每条边（目标边）都能通过 path_details 追溯到窗口内的一笔具体事件；
6. 切割失败时不覆盖旧产物、不产生可确认的新图，重试基于当前配置重新切割。
"""
from __future__ import annotations

import time
from typing import Any
from collections import defaultdict, Counter

from app.store import store
from app.schemas import PathResult
from app.services.event_stream import SessionSplitError
from app.services.session_split import get_confirmed_session_artifact


def model_fingerprint(model: dict[str, Any]) -> str:
    """路径模型配置指纹：窗口/深度/目标等变化后旧图即过期。"""
    return ":".join([
        str(model.get("start_event")),
        str(model.get("target_event")),
        str(model.get("time_window")),
        str(model.get("path_depth")),
        str(model.get("session_split_rule")),
        str(model.get("path_filter")),
    ])


def build_path_sankey(model_id: str) -> PathResult:
    """构建行为路径的 Sankey 数据。"""
    model = store.find_by_field("behavior_path", "model_id", model_id)
    if model is None:
        raise ValueError(f"Path model not found: {model_id}")

    start_event = model["start_event"]
    target_event = model["target_event"]
    time_window = int(model.get("time_window", 86400))
    path_depth = int(model.get("path_depth", 5))
    split_rule = model["session_split_rule"]
    fingerprint = model_fingerprint(model)

    # 读取关联的会话切割产物；切割失败/配置过期都会在此处重新切割，
    # 失败时抛 SessionSplitError，不写入任何新的路径产物，杜绝沿用旧边界。
    session_artifact = get_confirmed_session_artifact(split_rule)

    sessions = session_artifact.get("sessions", [])

    edge_counts: dict[tuple[str, str], int] = defaultdict(int)
    edge_evidence: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    node_counts: Counter[str] = Counter()
    conversion_paths: list[list[str]] = []
    common_counter: Counter[tuple[str, ...]] = Counter()
    common_meta: dict[tuple[str, ...], dict[str, Any]] = {}
    path_details: list[dict[str, Any]] = []
    total_conversions = 0
    converted_sessions = 0
    unconverted_sessions = 0
    late_target_excluded = 0
    out_of_window_events = 0

    for session in sessions:
        sid = session["session_id"]
        events = sorted(session.get("events", []), key=lambda x: x["timestamp"])
        user_id = events[0]["user_id"] if events else session.get("session_key", "")

        # 锚点：会话内首个起始事件；没有起始事件的会话不构成路径片段
        anchor_idx = next(
            (i for i, e in enumerate(events) if e["event_id"] == start_event), None
        )
        if anchor_idx is None:
            continue

        anchor_ts = events[anchor_idx]["timestamp"]
        window_end = anchor_ts + time_window

        # 窗口内事件链：起始锚点（含）到窗口结束（不含）
        in_window = [e for e in events[anchor_idx:] if e["timestamp"] < window_end]
        out_events = [e for e in events[anchor_idx:] if e["timestamp"] >= window_end]
        out_of_window_events += len(out_events)

        # 晚到目标：窗口外出现的目标事件，永远不能计入转化
        late_targets = [e for e in out_events if e["event_id"] == target_event]
        for late in late_targets:
            late_target_excluded += 1
            path_details.append({
                "session_id": sid,
                "user_id": user_id,
                "source_event": in_window[-1]["event_id"] if in_window else start_event,
                "target_event": target_event,
                "target_timestamp": late["timestamp"],
                "window_start": anchor_ts,
                "window_end": window_end,
                "in_window": False,
                "converted": False,
                "excluded_reason": "target_arrived_after_window",
            })

        # 路径深度截断作用在窗口内事件链上
        window_chain = in_window[:path_depth]
        if len(window_chain) < 2:
            # 只有锚点一个事件，不产生边，但仍统计节点
            for e in window_chain:
                node_counts[e["event_id"]] += 1
            continue

        path = [e["event_id"] for e in window_chain]
        converted = target_event in path
        if converted:
            total_conversions += 1
            converted_sessions += 1
            conversion_paths.append(path)
        else:
            unconverted_sessions += 1

        # 节点统计（窗口内片段，含未转化）
        for e in window_chain:
            node_counts[e["event_id"]] += 1

        # 边统计 + 每条边追溯到窗口内一笔事件
        for i in range(len(window_chain) - 1):
            edge = (path[i], path[i + 1])
            edge_counts[edge] += 1
            evidence_event = window_chain[i + 1]
            edge_evidence[edge].append({
                "session_id": sid,
                "user_id": user_id,
                "target_timestamp": evidence_event["timestamp"],
            })
            path_details.append({
                "session_id": sid,
                "user_id": user_id,
                "source_event": edge[0],
                "target_event": edge[1],
                "target_timestamp": evidence_event["timestamp"],
                "window_start": anchor_ts,
                "window_end": window_end,
                "in_window": True,
                "converted": converted,
                "excluded_reason": "",
            })

        # 常见路径：未转化片段同样进入入口，但带 converted 标记
        key = tuple(path)
        common_counter[key] += 1
        meta = common_meta.setdefault(key, {"converted_sessions": 0, "unconverted_sessions": 0})
        if converted:
            meta["converted_sessions"] += 1
        else:
            meta["unconverted_sessions"] += 1

    # 构建 Sankey 节点
    nodes = []
    node_index: dict[str, int] = {}
    for node, count in sorted(node_counts.items(), key=lambda x: (-x[1], x[0])):
        node_index[node] = len(nodes)
        nodes.append({
            "name": node,
            "value": count,
            "category": _get_event_category(node),
        })

    # 构建 Sankey 边（携带可追溯证据）
    links = []
    for (source, target), count in sorted(edge_counts.items(), key=lambda x: (-x[1], x[0])):
        if source not in node_index or target not in node_index:
            continue
        links.append({
            "source": node_index[source],
            "target": node_index[target],
            "value": count,
            "source_name": source,
            "target_name": target,
            "is_target_edge": target == target_event,
            "evidence": edge_evidence[(source, target)],
        })

    common_paths = [
        {
            "path": list(path),
            "count": count,
            "converted": meta["converted_sessions"] > 0,
            "converted_sessions": meta["converted_sessions"],
            "unconverted_sessions": meta["unconverted_sessions"],
        }
        for path, count in common_counter.most_common(20)
        if (meta := common_meta[path])
    ]

    result = PathResult(
        model_id=model_id,
        start_event=start_event,
        target_event=target_event,
        time_window=time_window,
        session_split_rule=split_rule,
        sankey_nodes=nodes,
        sankey_links=links,
        conversion_paths=conversion_paths[:50],
        common_paths=common_paths,
        path_details=path_details,
        total_conversions=total_conversions,
        total_sessions=converted_sessions + unconverted_sessions,
        converted_sessions=converted_sessions,
        unconverted_sessions=unconverted_sessions,
        late_target_excluded=late_target_excluded,
        out_of_window_events=out_of_window_events,
        config_fingerprint=fingerprint,
        generated_at=int(time.time()),
        stale=False,
    )

    # 只有成功构图才覆盖产物，旧图中的逾期节点随之清除
    store.set_artifact("behavior_path", model_id, result.model_dump())
    store.invalidate_compute_cache(f"behavior_path:{model_id}")

    store.update("behavior_path", model["id"], {
        "status": "已完成",
        "pending": False,
        "abnormal": False,
    })

    return result


def _get_event_category(event_id: str) -> str:
    """获取事件分类。"""
    event = store.find_by_field("event_track", "event_id", event_id)
    if event:
        return event.get("category", "unknown")
    return "unknown"


def get_path_result(model_id: str) -> dict[str, Any] | None:
    """获取路径分析结果。

    产物配置指纹与当前模型不一致时（例如刚缩短时间窗口）按新配置重建；
    若重建因会话切割失败而中断，保留旧产物但显式标注 stale，
    调用方不得把它当作可确认的新图使用。
    """
    model = store.find_by_field("behavior_path", "model_id", model_id)
    if model is None:
        return None

    artifact = store.get_artifact("behavior_path", model_id)
    if artifact is not None and artifact.get("config_fingerprint") == model_fingerprint(model):
        return artifact

    try:
        result = build_path_sankey(model_id)
        return result.model_dump()
    except SessionSplitError as exc:
        if artifact is None:
            return None
        marked = dict(artifact)
        marked["stale"] = True
        marked["stale_reason"] = str(exc)
        return marked
    except ValueError:
        return artifact


def find_common_paths(model_id: str, min_count: int = 3) -> list[dict[str, Any]]:
    """查找高频路径模式，直接读取关联构图产物中的常见路径切片。"""
    result = get_path_result(model_id)
    if result is None:
        return []
    return [p for p in result.get("common_paths", []) if p["count"] >= min_count]


def update_time_window(model_id: str, time_window: int) -> dict[str, Any]:
    """调整时间窗口并失效旧图，下一次构图必须按新边界重新切分。"""
    model = store.find_by_field("behavior_path", "model_id", model_id)
    if model is None:
        raise ValueError(f"Path model not found: {model_id}")
    if not isinstance(time_window, int) or time_window <= 0:
        raise ValueError("time_window 必须为正整数秒")

    store.update("behavior_path", model["id"], {"time_window": time_window})
    # 旧产物的指纹即刻与配置不匹配，get_path_result 会触发重建
    store.invalidate_compute_cache(f"behavior_path:{model_id}")
    return store.find("behavior_path", model["id"])  # type: ignore[return-value]
