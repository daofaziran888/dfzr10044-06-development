"""快照管理领域服务：生成、标记、过期与回退湖表快照，灾备分区清单导出与回导。"""
from __future__ import annotations

import time
from typing import Any

from app.services.base import BaseModuleService
from app.store import store
from app.lake_store import LakeStore, lake_store


class SnapshotMgrService(BaseModuleService):
    MODULE = "snapshot_mgr"
    ENTITY = "湖表快照"
    OPERATION_TERM = "校验"
    LIST_FIELDS = ["快照编号", "所属湖表", "快照标识", "父快照", "新增行数", "删除行数", "文件清单摘要", "生成时间"]
    REQUIRED_FIELDS = ["快照编号", "所属湖表", "快照标识"]
    STATUS_ORDER = ["生成中", "可用", "已过期", "已回退"]
    ACTION_RULES = {"标记可用": "可用", "标记过期": "已过期", "执行回退": "已回退"}
    NEGATIVE_ACTIONS = []

    def __init__(self, engine: LakeStore | None = None) -> None:
        # 灾备场景下接收方是独立存储引擎；默认使用全局单例
        self._engine = engine or lake_store

    def generate_snapshot_for_table(self, lake_table: str, file_ids: list[str],
                                    parent_snap: str | None = None) -> dict[str, Any]:
        """为指定湖表生成新快照，标记可用。"""
        snap = self._engine.create_snapshot(lake_table, file_ids, parent_snap,
                                            added_rows=0, deleted_rows=0)
        self._engine.mark_snapshot_available(lake_table, snap.snapshot_id)
        return {
            "snapshot_id": snap.snapshot_id,
            "lake_table": lake_table,
            "file_count": len(file_ids),
            "parent": snap.parent_snapshot,
            "created_at": snap.created_at,
        }

    def get_ancestor_chain(self, lake_table: str) -> list[str]:
        """获取快照祖先链（按时间顺序）。"""
        snaps = self._engine.list_snapshots(lake_table)
        chain: list[str] = []
        snap_map = {s.snapshot_id: s for s in snaps}
        if not snaps:
            return chain
        current = snaps[-1]
        while current is not None:
            chain.append(current.snapshot_id)
            current = snap_map.get(current.parent_snapshot or "")
        return list(reversed(chain))

    def diff_snapshots(self, lake_table: str, snap_a: str, snap_b: str) -> dict[str, Any]:
        """比较两个快照间的文件差异。"""
        s_a = self._engine.get_snapshot(lake_table, snap_a)
        s_b = self._engine.get_snapshot(lake_table, snap_b)
        if not s_a or not s_b:
            return {"error": "snapshot not found"}
        set_a = set(s_a.file_ids)
        set_b = set(s_b.file_ids)
        return {
            "added": list(set_b - set_a),
            "removed": list(set_a - set_b),
            "unchanged": list(set_a & set_b),
        }

    # ---- 灾备分区清单：导出 / 回导 / 进度核对 ----

    def export_partition_manifest(self, lake_table: str, snapshot_id: str) -> dict[str, Any]:
        """为已可用快照导出分区清单包；非可用快照不可导出。"""
        package = self._engine.export_snapshot_package(lake_table, snapshot_id)
        if package is None:
            return {
                "success": False,
                "error": "snapshot_not_exportable",
                "message": f"快照 {snapshot_id} 不存在或不是可用状态，无法导出分区清单",
                "snapshot_id": snapshot_id,
            }
        return {
            "success": True,
            "package": package,
            "snapshot_id": snapshot_id,
            "file_count": len(package["files"]),
        }

    def stage_partition_manifest(self, package: dict[str, Any]) -> dict[str, Any]:
        """回导整包校验：逐项校核湖表、快照、父快照和文件身份，登记期望文件集。"""
        return self._engine.stage_snapshot_package(package)

    def import_partition_files(
        self, lake_table: str, snapshot_id: str, files: list[dict[str, Any]]
    ) -> dict[str, Any]:
        """逐文件回导：幂等注册，整包到齐才放开时间旅行。"""
        return self._engine.import_snapshot_files(lake_table, snapshot_id, files)

    def import_partition_manifest(self, package: dict[str, Any]) -> dict[str, Any]:
        """一次性回导整份分区清单包。"""
        return self._engine.import_snapshot_package(package)

    def dr_import_status(self, lake_table: str, snapshot_id: str) -> dict[str, Any]:
        """核对某快照号在本地仍有效的历史与回导进度。"""
        return self._engine.dr_import_status(lake_table, snapshot_id)
