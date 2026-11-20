"""会话切割引擎：按超时阈值和跨天/跨渠道策略切割用户会话。

切割产物（store 中的 session_split artifact）是行为路径分析的唯一事件来源：
每个事件都会被打上 session_id，路径服务据此按时间窗口切分有效事件范围。
"""
from __future__ import annotations

import time
from typing import Any
from collections import defaultdict

from app.store import store
from app.schemas import SessionResult
from app.services.event_stream import SessionSplitError, generate_raw_events


def config_fingerprint(strategy: dict[str, Any]) -> str:
    """切割配置指纹：参数变化后旧产物即过期，重试不得沿用旧时间边界。"""
    return ":".join([
        str(strategy.get("timeout_sec")),
        str(strategy.get("cross_day_policy")),
        str(strategy.get("cross_channel_policy")),
        str(strategy.get("session_key")),
        str(strategy.get("version")),
    ])


def split_sessions(strategy_id: str) -> SessionResult:
    """执行会话切割。

    切割失败时抛出 SessionSplitError，且不会写入/覆盖任何产物，
    调用方重试时只能基于当前配置重新计算，不存在可确认的新图。
    """
    strategy = store.find_by_field("session_split", "strategy_id", strategy_id)
    if strategy is None:
        raise ValueError(f"Strategy not found: {strategy_id}")

    timeout_sec = strategy["timeout_sec"]
    cross_day_policy = strategy["cross_day_policy"]
    cross_channel_policy = strategy["cross_channel_policy"]
    session_key = strategy["session_key"]

    # 非法切割参数直接判失败：不产生产物，避免下游拿到过期/残缺边界
    if not isinstance(timeout_sec, int) or timeout_sec <= 0:
        raise SessionSplitError(
            f"会话切割失败：超时阈值非法（timeout_sec={timeout_sec}），未生成切割产物"
        )

    events = generate_raw_events()

    # 按会话键分组
    keyed_events: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for e in events:
        key = e.get(session_key, e["user_id"])
        keyed_events[key].append(e)

    sessions: list[dict[str, Any]] = []
    total_events = len(events)
    event_min = events[0]["timestamp"] if events else 0
    event_max = events[-1]["timestamp"] if events else 0
    session_seq = 0

    for key, evts in keyed_events.items():
        evts.sort(key=lambda x: x["timestamp"])
        current_session: list[dict[str, Any]] = []
        session_start = evts[0]["timestamp"] if evts else 0

        def _flush(closed_events: list[dict[str, Any]], start_ts: int) -> None:
            nonlocal session_seq
            if not closed_events:
                return
            session_seq += 1
            sid = f"{strategy_id}-{key}-{session_seq:04d}"
            # 给产物中的每个事件打上 session_id，供路径窗口切分追溯
            tagged = [dict(e, session_id=sid) for e in closed_events]
            sessions.append({
                "session_id": sid,
                "session_key": key,
                "events": tagged,
                "event_count": len(tagged),
                "start_time": start_ts,
                "end_time": closed_events[-1]["timestamp"],
                "duration": closed_events[-1]["timestamp"] - start_ts,
            })

        for i, evt in enumerate(evts):
            if not current_session:
                current_session = [evt]
                session_start = evt["timestamp"]
                continue

            prev = current_session[-1]
            time_diff = evt["timestamp"] - prev["timestamp"]

            # 超时检查
            should_split = time_diff > timeout_sec

            # 跨天检查
            if not should_split and cross_day_policy == "split":
                prev_day = prev["timestamp"] // 86400
                curr_day = evt["timestamp"] // 86400
                if prev_day != curr_day:
                    should_split = True

            # 跨渠道检查
            if not should_split and cross_channel_policy == "split":
                if prev.get("channel") != evt.get("channel"):
                    should_split = True

            if should_split:
                _flush(current_session, session_start)
                current_session = [evt]
                session_start = evt["timestamp"]
            else:
                current_session.append(evt)

        _flush(current_session, session_start)

    sessions.sort(key=lambda s: (s["session_key"], s["start_time"]))

    # 计算统计
    avg_duration = sum(s["duration"] for s in sessions) / len(sessions) if sessions else 0

    result = SessionResult(
        strategy_id=strategy_id,
        total_events=total_events,
        session_count=len(sessions),
        avg_session_duration=round(avg_duration, 2),
        sessions=sessions,
        generated_at=int(time.time()),
        config_fingerprint=config_fingerprint(strategy),
        event_time_range={"start": event_min, "end": event_max},
    )

    # 切割成功才落产物；失败路径在上方已抛异常，不会覆盖旧产物
    store.set_artifact("session_split", strategy_id, result.model_dump())

    # 更新状态
    store.update("session_split", strategy["id"], {
        "status": "已完成",
        "pending": False,
        "abnormal": False,
    })

    return result


def get_session_result(strategy_id: str) -> dict[str, Any] | None:
    """获取会话切割结果。"""
    artifact = store.get_artifact("session_split", strategy_id)
    if artifact:
        return artifact
    try:
        result = split_sessions(strategy_id)
        return result.model_dump()
    except (ValueError, SessionSplitError):
        return None


def get_confirmed_session_artifact(strategy_id: str) -> dict[str, Any]:
    """供路径分析读取的会话切割产物，要求是与当前策略配置一致的已确认产物。

    - 无产物或配置指纹过期：按当前配置重新切割；
    - 切割失败：抛 SessionSplitError，不返回任何带旧时间边界的数据。
    """
    strategy = store.find_by_field("session_split", "strategy_id", strategy_id)
    if strategy is None:
        raise ValueError(f"Strategy not found: {strategy_id}")

    artifact = store.get_artifact("session_split", strategy_id)
    fingerprint = config_fingerprint(strategy)
    if artifact is not None and artifact.get("config_fingerprint") == fingerprint:
        return artifact
    return split_sessions(strategy_id).model_dump()


def resegment_with_strategy(strategy_id: str, new_timeout: int | None = None) -> SessionResult:
    """使用新参数重新切割会话。

    新参数先做合法性校验；切割失败时回滚已写入的参数，保证非法时间边界
    不会残留在策略配置里，后续重试只能基于合法的当前配置执行。
    """
    strategy = store.find_by_field("session_split", "strategy_id", strategy_id)
    if strategy is None:
        raise ValueError(f"Strategy not found: {strategy_id}")

    previous_timeout = strategy.get("timeout_sec")
    if new_timeout is not None:
        if not isinstance(new_timeout, int) or new_timeout <= 0:
            raise SessionSplitError(
                f"会话切割失败：超时阈值非法（timeout_sec={new_timeout}），配置未变更"
            )
        store.update("session_split", strategy["id"], {"timeout_sec": new_timeout})
        store.invalidate_compute_cache(f"session_split:{strategy_id}")

    try:
        return split_sessions(strategy_id)
    except SessionSplitError:
        # 切割失败：回滚参数，禁止把非法/过期边界留在配置中
        if new_timeout is not None:
            store.update("session_split", strategy["id"], {"timeout_sec": previous_timeout})
        raise
