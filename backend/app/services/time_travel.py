"""时间旅行领域服务：按快照/时间点回溯查询历史湖表数据。"""
from __future__ import annotations

import time
from typing import Any

from app.services.base import BaseModuleService
from app.store import store
from app.lake_store import LakeStore, lake_store


class TimeTravelService(BaseModuleService):
    MODULE = "time_travel"
    ENTITY = "回溯查询"
    OPERATION_TERM = "读取"
    LIST_FIELDS = ["查询编号", "所属湖表", "目标快照", "回溯时间点", "查询列", "返回行数", "命中分区数", "查询耗时"]
    REQUIRED_FIELDS = ["查询编号", "所属湖表", "目标快照"]
    STATUS_ORDER = ["待执行", "执行中", "已返回", "执行失败"]
    ACTION_RULES = {"发起查询": "执行中", "取消查询": "已返回", "标记失败": "执行失败"}
    NEGATIVE_ACTIONS = []

    def __init__(self, engine: LakeStore | None = None) -> None:
        # 灾备端用回导后的分区文件清单按原快照号读取；默认全局单例
        self._engine = engine or lake_store

    def read_at_snapshot(self, lake_table: str, snapshot_id: str,
                         columns: list[str] | None = None) -> dict[str, Any]:
        """按指定快照 ID 读取历史数据并返回命中分区数和行数。"""
        snap = self._engine.get_snapshot(lake_table, snapshot_id)
        if snap is None:
            return {"success": False, "error": "snapshot not found", "rows": 0, "partitions": 0}
        total_rows = 0
        partitions = set()
        file_ids: list[str] = []
        for fid in snap.file_ids:
            pf = self._engine.get_file(lake_table, fid)
            if pf:
                total_rows += pf.file_rows
                partitions.add(pf.partition_key)
                file_ids.append(pf.file_id)
        return {
            "success": True,
            "snapshot_id": snapshot_id,
            "rows": total_rows,
            "partition_count": len(partitions),
            "partitions_hit": sorted(partitions),
            "file_ids": file_ids,
            "query_time": time.time(),
        }

    def read_at_timepoint(self, lake_table: str, timestamp: float) -> dict[str, Any]:
        """按时间点回溯查找最近可用快照并读取。"""
        snap = self._engine.get_snapshot_at_time(lake_table, timestamp)
        if snap is None:
            return {"success": False, "error": "no available snapshot at time", "rows": 0, "partitions": 0}
        return self.read_at_snapshot(lake_table, snap.snapshot_id)

    def compare_two_snapshots(self, lake_table: str, snap_a: str, snap_b: str) -> dict[str, Any]:
        """比较两个快照间的数据差异。"""
        s_a = self._engine.get_snapshot(lake_table, snap_a)
        s_b = self._engine.get_snapshot(lake_table, snap_b)
        if not s_a or not s_b:
            return {"success": False, "error": "snapshot not found"}
        return {
            "success": True,
            "snap_a": snap_a,
            "snap_b": snap_b,
            "diff_added_rows": max(0, s_b.added_rows - s_a.added_rows),
            "diff_deleted_rows": max(0, s_b.deleted_rows - s_a.deleted_rows),
        }
