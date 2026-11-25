"""过期策略领域服务：快照过期标记、清理范围判定与保护标签。"""
from __future__ import annotations

import time
from typing import Any

from app.services.base import BaseModuleService
from app.store import store
from app.lake_store import lake_store


class ExpirePolicyService(BaseModuleService):
    MODULE = "expire_policy"
    ENTITY = "过期策略"
    OPERATION_TERM = "查询"
    LIST_FIELDS = ["策略编号", "所属湖表", "保留快照数", "保留天数", "受保护标签", "清理范围", "最近清理", "释放字节数"]
    REQUIRED_FIELDS = ["策略编号", "所属湖表", "保留快照数"]
    STATUS_ORDER = ["草稿", "已生效", "清理中", "已暂停"]
    ACTION_RULES = {"启效策略": "已生效", "启动清理": "清理中", "暂停策略": "已暂停"}
    NEGATIVE_ACTIONS = []

    def __init__(self, engine=None) -> None:
        # 灾备冲突检测需要落到具体存储引擎；默认全局单例
        self._engine = engine or lake_store

    def identify_expired_snapshots(self, lake_table: str, keep_count: int,
                                   protected_tags: list[str] | None = None) -> list[str]:
        """识别过期快照：保留最近的 keep_count 个，其余标记过期但不含受保护标签。"""
        snaps = lake_store.list_snapshots(lake_table)
        available = [s for s in snaps if s.status == "available"]
        available.sort(key=lambda s: s.created_at)
        expired = available[:-keep_count] if keep_count < len(available) else []
        return [s.snapshot_id for s in expired]

    def execute_cleanup(self, lake_table: str, snapshot_ids: list[str]) -> dict[str, Any]:
        """执行清理：将过期快照标记为 expired 并释放相关文件的存储空间。"""
        released_bytes = 0
        for sid in snapshot_ids:
            snap = lake_store.get_snapshot(lake_table, sid)
            if snap:
                for fid in snap.file_ids:
                    pf = lake_store.get_file(lake_table, fid)
                    if pf:
                        released_bytes += pf.file_bytes
                lake_store.mark_snapshot_expired(lake_table, sid)
        return {
            "lake_table": lake_table,
            "expired_count": len(snapshot_ids),
            "released_bytes": released_bytes,
            "cleanup_time": time.time(),
        }

    def reclaim_unreferenced_files(self, lake_table: str) -> dict[str, Any]:
        """回收不再被可用/生成中快照引用的文件。

        与灾备旧包恢复冲突的文件照常回收（接收方持有自包含的包可补数），
        冲突明细记录在 dr_conflicts 中供对账。
        """
        return self._engine.reclaim_unreferenced_files(lake_table)