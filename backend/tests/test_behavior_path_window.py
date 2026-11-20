"""行为路径时间窗口切分专项测试。

覆盖：
- 路径分析只读取关联会话切割产物（事件带 session_id）；
- 时间窗口缩短后转化结果与目标时点对齐，晚到目标不计入转化；
- 窗口内未转化片段进入常见路径但显式标注未转化；
- 每条目标边可追溯到窗口内一笔事件；
- 切割失败时没有可确认的新图，重试不得沿用过期时间边界；
- 缩短窗口后重新运行可核对因到达太晚被排除的次数。
"""
from __future__ import annotations

import os
import sys
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from app.store import store
from app.services.event_stream import (
    SessionSplitError,
    LATE_TARGET_USERS,
    NON_CONVERTED_USERS,
    generate_raw_events,
)
from app.services.session_split import (
    split_sessions,
    get_confirmed_session_artifact,
    config_fingerprint,
)
from app.services.behavior_path import (
    build_path_sankey,
    get_path_result,
    find_common_paths,
    update_time_window,
    model_fingerprint,
)

LATE_UIDS = {f"U{u:05d}" for u in LATE_TARGET_USERS}
NON_CONVERTED_UIDS = {f"U{u:05d}" for u in NON_CONVERTED_USERS}


class PathWindowTestBase(unittest.TestCase):
    def setUp(self) -> None:
        store._artifacts.clear()
        store._compute_cache.clear()
        store._snapshots.clear()
        # 每个用例从种子默认配置出发
        strategy = store.find_by_field("session_split", "strategy_id", "SS-001")
        store.update("session_split", strategy["id"], {"timeout_sec": 10800})
        model = store.find_by_field("behavior_path", "model_id", "BP-001")
        store.update("behavior_path", model["id"], {"time_window": 3600})


class TestSessionArtifactFeedsPath(PathWindowTestBase):
    """构图请求与常见路径接口都读取关联的会话切割产物。"""

    def test_events_come_from_session_artifact_with_session_id(self) -> None:
        split_sessions("SS-001")
        artifact = get_confirmed_session_artifact("SS-001")
        self.assertGreater(len(artifact["sessions"]), 0)
        for session in artifact["sessions"]:
            self.assertTrue(session["session_id"])
            for event in session["events"]:
                self.assertEqual(event["session_id"], session["session_id"])

        result = build_path_sankey("BP-001")
        # 明细中的会话编号全部来自切割产物
        artifact_sids = {s["session_id"] for s in artifact["sessions"]}
        detail_sids = {d["session_id"] for d in result.path_details}
        self.assertTrue(detail_sids)
        self.assertTrue(detail_sids <= artifact_sids)

    def test_common_paths_read_same_artifact(self) -> None:
        result = build_path_sankey("BP-001")
        common = find_common_paths("BP-001", min_count=1)
        # 常见路径与构图产物中的切片完全一致（同一数据源）
        self.assertEqual(
            [(p["path"], p["count"]) for p in common],
            [(p["path"], p["count"]) for p in result.common_paths if p["count"] >= 1],
        )


class TestWindowedConversion(PathWindowTestBase):
    """窗口规则与事件链归因冲突时，晚到目标不得入转化计数。"""

    def test_late_target_excluded_from_conversion(self) -> None:
        result = build_path_sankey("BP-001")
        self.assertEqual(result.late_target_excluded, len(LATE_TARGET_USERS))
        # 晚到目标的排除明细
        exclusions = [
            d for d in result.path_details
            if d["excluded_reason"] == "target_arrived_after_window"
        ]
        self.assertEqual({d["user_id"] for d in exclusions}, LATE_UIDS)
        for d in exclusions:
            self.assertFalse(d["in_window"])
            self.assertFalse(d["converted"])
            self.assertGreaterEqual(d["target_timestamp"], d["window_end"])

    def test_late_session_keeps_two_valid_in_window_events(self) -> None:
        result = build_path_sankey("BP-001")
        in_window = [d for d in result.path_details if d["in_window"]]
        for uid in LATE_UIDS:
            user_edges = [d for d in in_window if d["user_id"] == uid]
            # 此前两条有效窗口内事件形成两条边，且片段标注为未转化
            self.assertGreaterEqual(len(user_edges), 2)
            self.assertTrue(all(not d["converted"] for d in user_edges))

    def test_unconverted_segment_enters_common_paths(self) -> None:
        build_path_sankey("BP-001")
        common = find_common_paths("BP-001", min_count=1)
        unconverted = [p for p in common if not p["converted"]]
        converted = [p for p in common if p["converted"]]
        self.assertTrue(unconverted, "未转化片段必须进入常见路径入口")
        self.assertTrue(converted)
        # 未转化片段路径中不允许出现窗口外目标
        for p in unconverted:
            self.assertNotIn("EV-003", p["path"])
        # 确定性未转化用户被未转化条目覆盖
        flattened_users = set()
        result = get_path_result("BP-001")
        for d in result["path_details"]:
            if not d["converted"] and d["in_window"]:
                flattened_users.add(d["user_id"])
        self.assertTrue(NON_CONVERTED_UIDS <= flattened_users)

    def test_widening_window_recovers_late_conversion(self) -> None:
        narrow = build_path_sankey("BP-001")
        self.assertEqual(narrow.late_target_excluded, len(LATE_TARGET_USERS))
        update_time_window("BP-001", 86400)
        wide = get_path_result("BP-001")
        self.assertEqual(wide["late_target_excluded"], 0)
        self.assertEqual(
            wide["converted_sessions"],
            narrow.converted_sessions + len(LATE_TARGET_USERS),
        )

    def test_shrinking_window_increases_exclusions(self) -> None:
        base = build_path_sankey("BP-001")
        update_time_window("BP-001", 600)
        shrunk = get_path_result("BP-001")
        self.assertEqual(shrunk["time_window"], 600)
        self.assertGreaterEqual(shrunk["late_target_excluded"], base.late_target_excluded)
        self.assertGreater(shrunk["out_of_window_events"], base.out_of_window_events)
        # 确定性晚到用户始终出现在排除名单里，次数可核对
        excluded_uids = {
            d["user_id"] for d in shrunk["path_details"]
            if d["excluded_reason"] == "target_arrived_after_window"
        }
        self.assertTrue(LATE_UIDS <= excluded_uids)


class TestEdgeTraceability(PathWindowTestBase):
    """路径明细数据的每条目标边能追溯到窗口内一笔事件。"""

    def test_every_in_window_edge_traceable_to_event(self) -> None:
        result = build_path_sankey("BP-001")
        artifact = get_confirmed_session_artifact("SS-001")
        event_index = {
            (e["session_id"], e["timestamp"]): e
            for session in artifact["sessions"]
            for e in session["events"]
        }
        for d in result.path_details:
            if not d["in_window"]:
                continue
            key = (d["session_id"], d["target_timestamp"])
            self.assertIn(key, event_index)
            event = event_index[key]
            self.assertEqual(event["event_id"], d["target_event"])
            self.assertLess(d["target_timestamp"], d["window_end"])
            self.assertGreaterEqual(d["target_timestamp"], d["window_start"])

    def test_sankey_links_carry_evidence(self) -> None:
        result = build_path_sankey("BP-001")
        self.assertTrue(result.sankey_links)
        for link in result.sankey_links:
            self.assertEqual(len(link["evidence"]), link["value"])
            for ev in link["evidence"]:
                self.assertTrue(ev["session_id"] and ev["target_timestamp"])


class TestSplitFailureAndStaleBoundary(PathWindowTestBase):
    """切割失败时没有可确认的新图，重试不得沿用过期时间边界。"""

    def test_split_failure_writes_no_artifact(self) -> None:
        split_sessions("SS-001")
        good = store.get_artifact("session_split", "SS-001")
        strategy = store.find_by_field("session_split", "strategy_id", "SS-001")
        store.update("session_split", strategy["id"], {"timeout_sec": 0})
        with self.assertRaises(SessionSplitError):
            split_sessions("SS-001")
        # 旧产物未被失败的切割覆盖
        self.assertIs(store.get_artifact("session_split", "SS-001"), good)

    def test_build_blocked_when_split_fails_and_artifact_preserved(self) -> None:
        result = build_path_sankey("BP-001")
        old_generated = result.generated_at
        # 先让窗口配置变化（旧图指纹过期），再让切割失败
        update_time_window("BP-001", 1800)
        strategy = store.find_by_field("session_split", "strategy_id", "SS-001")
        store.update("session_split", strategy["id"], {"timeout_sec": -5})

        with self.assertRaises(SessionSplitError):
            build_path_sankey("BP-001")
        preserved = store.get_artifact("behavior_path", "BP-001")
        self.assertEqual(preserved["generated_at"], old_generated)

        # get_path_result 只返回带 stale 标记的留痕，不允许当作新确认图
        stale_view = get_path_result("BP-001")
        self.assertTrue(stale_view["stale"])
        self.assertIn("timeout_sec", stale_view["stale_reason"])

    def test_retry_uses_current_boundary_after_recovery(self) -> None:
        build_path_sankey("BP-001")
        update_time_window("BP-001", 1800)
        strategy = store.find_by_field("session_split", "strategy_id", "SS-001")
        store.update("session_split", strategy["id"], {"timeout_sec": 0})
        with self.assertRaises(SessionSplitError):
            build_path_sankey("BP-001")

        # 恢复后重试：按当前 1800 秒边界重新切割、重新构图
        store.update("session_split", strategy["id"], {"timeout_sec": 10800})
        rebuilt = build_path_sankey("BP-001")
        self.assertEqual(rebuilt.time_window, 1800)
        self.assertFalse(rebuilt.model_dump()["stale"])
        self.assertEqual(rebuilt.config_fingerprint, model_fingerprint(
            store.find_by_field("behavior_path", "model_id", "BP-001")
        ))
        self.assertFalse(get_path_result("BP-001")["stale"])

    def test_strategy_change_invalidates_artifact_fingerprint(self) -> None:
        split_sessions("SS-001")
        strategy = store.find_by_field("session_split", "strategy_id", "SS-001")
        old_fp = config_fingerprint(strategy)
        store.update("session_split", strategy["id"], {"timeout_sec": 7200})
        new_fp = config_fingerprint(store.find_by_field("session_split", "strategy_id", "SS-001"))
        self.assertNotEqual(old_fp, new_fp)
        # get_confirmed_session_artifact 自动按新配置重切
        refreshed = get_confirmed_session_artifact("SS-001")
        self.assertEqual(refreshed["config_fingerprint"], new_fp)

    def test_resegment_rolls_back_invalid_timeout(self) -> None:
        from app.services.session_split import resegment_with_strategy
        split_sessions("SS-001")
        strategy = store.find_by_field("session_split", "strategy_id", "SS-001")
        original_timeout = strategy["timeout_sec"]
        with self.assertRaises(SessionSplitError):
            resegment_with_strategy("SS-001", 0)
        after = store.find_by_field("session_split", "strategy_id", "SS-001")
        # 非法参数不得残留在配置里
        self.assertEqual(after["timeout_sec"], original_timeout)


class TestRawEventDeterminism(unittest.TestCase):
    def test_raw_events_deterministic(self) -> None:
        a = generate_raw_events()
        b = generate_raw_events()
        self.assertEqual(a, b)
        self.assertTrue(a == sorted(a, key=lambda x: x["timestamp"]))


if __name__ == "__main__":
    unittest.main(verbosity=2)
