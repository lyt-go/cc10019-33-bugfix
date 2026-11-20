"""会话事件流：会话切割与行为路径分析共用的同一份原始事件数据。

两个模块必须消费同一批事件，才能保证「会话切割产物 → 路径窗口切分」的链路
可核对：路径服务不再自行造数，而是读取会话切割产物中带 session_id 的事件链。
"""
from __future__ import annotations

from typing import Any

# 固定基准时间，保证数据确定性，测试可精确断言窗口边界
BASE_TIME = 1_700_000_000
# 会话内常规间隔（5 分钟），小于 SS-001 的 10800 秒超时阈值
IN_SESSION_GAP = 300
# 晚到目标与上一事件的间隔（9000 秒 = 2.5 小时）：
# - 不触发 SS-001（10800 秒）会话超时、不跨天，晚到目标留在同一切割会话；
# - 大于缩短后的路径时间窗口（如 3600/600 秒），落在有效窗口外，
# 由路径服务按 time_window 显式判定为「到达太晚被排除」，且不计入转化。
LATE_GAP = 9000
DEFAULT_USER_COUNT = 50

# 需要构造「目标晚到」场景的用户（确定性、可断言）
LATE_TARGET_USERS = {3, 12, 27}
# 需要构造「窗口内未转化」场景的用户：窗口内只有中间事件，目标根本没出现
NON_CONVERTED_USERS = {7, 18}


class SessionSplitError(RuntimeError):
    """会话切割失败：此时不存在可确认的新切割产物。"""


def generate_raw_events(user_count: int = DEFAULT_USER_COUNT) -> list[dict[str, Any]]:
    """生成确定性的用户行为事件流。

    每个用户一条事件链，包含起始事件 EV-001、中间事件 EV-002/EV-004 和
    目标事件 EV-003，其中：

    - LATE_TARGET_USERS：目标事件晚到（与上一事件间隔 LATE_GAP），落在时间窗口外，
      此前仍有两条有效的窗口内事件；
    - NON_CONVERTED_USERS：窗口内只有起始和中间事件，不出现目标事件。
    """
    events: list[dict[str, Any]] = []
    for user_idx in range(1, user_count + 1):
        user_id = f"U{user_idx:05d}"
        device_id = f"D{user_idx % 10 + 1:03d}"
        # 同一会话内渠道保持稳定，避免跨渠道策略把一条事件链切碎；
        # 不同用户之间仍有 app/web 差异，保留渠道维度数据
        channel = "app" if user_idx % 2 == 1 else "web"
        chain = _build_user_chain(user_idx)
        cursor = BASE_TIME + user_idx * 10_000
        for step, event_id in enumerate(chain):
            if step > 0:
                # 晚到目标与其前一事件之间拉开 LATE_GAP 间隔
                if user_idx in LATE_TARGET_USERS and event_id == "EV-003":
                    cursor += LATE_GAP
                else:
                    cursor += IN_SESSION_GAP
            events.append({
                "user_id": user_id,
                "device_id": device_id,
                "event_id": event_id,
                "timestamp": cursor,
                "channel": channel,
                "page_url": f"/page{step % 5 + 1}",
            })
    return sorted(events, key=lambda x: x["timestamp"])


def _build_user_chain(user_idx: int) -> list[str]:
    """构造单个用户的事件序列（不含时间戳）。"""
    if user_idx in NON_CONVERTED_USERS:
        # 窗口内有效但不转化：起始事件后只有两条中间事件
        return ["EV-001", "EV-002", "EV-004"]
    chain = ["EV-001", "EV-002", "EV-004", "EV-003"]
    if user_idx not in LATE_TARGET_USERS and user_idx % 5 == 0:
        # 部分用户路径更深一些，验证路径深度截断
        chain = ["EV-001", "EV-002", "EV-001", "EV-004", "EV-003"]
    return chain
