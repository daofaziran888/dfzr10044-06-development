"""湖表存储引擎：文件注册表、分区清单、快照链、水位存储与批次数据。

本模块提供跨服务共享的不可变数据层：
- 文件注册：每个分区文件有唯一 file_id、所属分区、所属湖表、行数、字节数
- 快照链：每个湖表按时间线记录快照，含 file_ids、manifest、父快照指针
- 水位：每张湖表的已提交/待提交水位记录
- 批次数据：入湖批次的行级数据存储，含校验值
- 隔离区：被质量规则拦截的数据行，按批次隔离
- 写锁：并发提交冲突检测
- 灾备快照包：已可用快照的分区清单导出 / 回导 / 断点补数

设计原则：
- 所有写操作在提交前必须拿到全局锁
- 快照只在事务完整落湖后标记为可用
- 归并时先写入新文件清单，再原子地替换旧文件列表
- 灾备回导逐项校核（湖表、快照、父快照、文件身份），整包通过才放开时间旅行
"""
from __future__ import annotations

import copy
import hashlib
import threading
import time
import uuid
from dataclasses import dataclass, field
from typing import Any


# ----------------------------- 快照清单摘要 -----------------------------

# 灾备快照包类型标识：分区清单导出 / 回导的统一信封
DR_PACKAGE_TYPE = "snapshot_partition_manifest"


def snapshot_manifest_checksum(file_ids: list[str]) -> str:
    """快照清单摘要：与快照创建使用同一算法，供灾备回导逐项校核。"""
    return hashlib.md5(",".join(sorted(file_ids)).encode()).hexdigest()


def dr_package_checksum(package: dict[str, Any]) -> str:
    """整包摘要：绑定湖表、快照号、父快照、清单摘要与每个文件的完整身份。"""
    files = sorted(package.get("files", []), key=lambda f: f.get("file_id", ""))
    parts = [
        str(package.get("lake_table", "")),
        str(package.get("snapshot_id", "")),
        str(package.get("parent_snapshot") or "-"),
        str(package.get("manifest_checksum", "")),
        *[
            ":".join([
                str(f.get("file_id", "")),
                str(f.get("partition_key", "")),
                str(f.get("file_rows", "")),
                str(f.get("file_bytes", "")),
                str(f.get("file_manifest", "")),
            ])
            for f in files
        ],
    ]
    return hashlib.md5("|".join(parts).encode()).hexdigest()


# ----------------------------- 文件注册 -----------------------------

@dataclass
class PartitionFile:
    """分区文件：每个文件属于一个分区。"""
    file_id: str
    lake_table: str
    partition_key: str
    file_rows: int
    file_bytes: int
    file_manifest: str  # 文件内容摘要（MD5）
    created_at: float


# ----------------------------- 快照 -----------------------------

@dataclass
class LakeSnapshot:
    """湖表快照：记录某个时刻整个湖表的分区文件清单。"""
    snapshot_id: str
    lake_table: str
    parent_snapshot: str | None
    file_ids: list[str]
    added_rows: int
    deleted_rows: int
    manifest_checksum: str
    status: str  # generating | available | expired | rolled_back
    created_at: float


# ----------------------------- 水位 -----------------------------

@dataclass
class WatermarkRecord:
    """入湖水位追踪：记录已提交和待提交水位。"""
    lake_table: str
    field_name: str
    committed_value: str
    pending_value: str | None
    strategy: str  # append | merge | overwrite
    last_advanced_at: float


# ----------------------------- 批次数据 -----------------------------

@dataclass
class BatchRecord:
    """入湖批次：一次入湖操作导出的一组数据行。"""
    batch_id: str
    lake_table: str
    source_system: str
    rows: list[dict[str, Any]]  # 实际数据行
    row_count: int
    window_start: str
    window_end: str
    checksum: str
    snapshot_id: str | None  # 落湖后分配的 snap
    status: str  # pending | ingesting | ingested | failed
    created_at: float


# ----------------------------- 写锁 -----------------------------

class WriteLock:
    """全局写入锁：同一湖表同时只能有一个写入者提交。"""

    def __init__(self) -> None:
        self._locks: dict[str, threading.Lock] = {}
        self._lock = threading.Lock()

    def acquire(self, lake_table: str) -> threading.Lock:
        with self._lock:
            if lake_table not in self._locks:
                self._locks[lake_table] = threading.Lock()
            table_lock = self._locks[lake_table]
        table_lock.acquire()
        return table_lock

    def release(self, lake_table: str) -> None:
        with self._lock:
            lock = self._locks.get(lake_table)
        if lock is not None:
            try:
                lock.release()
            except RuntimeError:
                pass


# ----------------------------- 存储引擎 -----------------------------

class LakeStore:
    """湖表存储引擎主类。

    管理三大维度：
    - files: 分区文件注册表
    - snapshots: 快照链
    - watermarks: 水位
    - batches: 批次数据
    - quarantine: 隔离区数据
    - write_lock: 并发提交的控制锁
    """

    def __init__(self) -> None:
        # 文件注册表：lake_table -> {file_id -> PartitionFile}
        self._files: dict[str, dict[str, PartitionFile]] = {}

        # 快照链：lake_table -> list[LakeSnapshot]（时间序）
        self._snapshots: dict[str, list[LakeSnapshot]] = {}

        # 水位：lake_table -> WatermarkRecord
        self._watermarks: dict[str, WatermarkRecord] = {}

        # 批次数据：batch_id -> BatchRecord
        self._batches: dict[str, BatchRecord] = {}

        # 隔离区：batch_id -> list[dict]（被拦截的数据行）
        self._quarantine: dict[str, list[dict[str, Any]]] = {}

        # 写锁
        self._write_lock = WriteLock()

        # 归并中断恢复检查点：merge_id -> {partial_file_ids: [...], ...}
        self._merge_checkpoints: dict[str, dict[str, Any]] = {}

        # 灾备回导暂存区：(lake_table, snapshot_id) -> 暂存记录
        # 整包校验通过后登记期望文件集，全部文件校验到齐才落快照
        self._dr_staging: dict[tuple[str, str], dict[str, Any]] = {}

        # 灾备钉住：lake_table -> snapshot_id -> set(file_id)
        # 导出即登记，接收方确认完成才解除；源端回收被钉住文件时报告冲突
        self._dr_pins: dict[str, dict[str, set[str]]] = {}

    # ---- 文件操作 ----

    def register_file(
        self, lake_table: str, partition_key: str, rows: int, bytes_: int, manifest: str | None = None
    ) -> PartitionFile:
        file_id = f"{lake_table}-p{partition_key}-{uuid.uuid4().hex[:8]}"
        checksum = manifest or hashlib.md5(f"{lake_table}:{partition_key}:{time.time()}".encode()).hexdigest()
        pf = PartitionFile(
            file_id=file_id,
            lake_table=lake_table,
            partition_key=partition_key,
            file_rows=rows,
            file_bytes=bytes_,
            file_manifest=checksum,
            created_at=time.time(),
        )
        self._files.setdefault(lake_table, {})[file_id] = pf
        return pf

    def list_files(self, lake_table: str, partition_key: str | None = None) -> list[PartitionFile]:
        files = list(self._files.get(lake_table, {}).values())
        if partition_key is not None:
            files = [f for f in files if f.partition_key == partition_key]
        return files

    def get_file(self, lake_table: str, file_id: str) -> PartitionFile | None:
        return self._files.get(lake_table, {}).get(file_id)

    def remove_files(self, lake_table: str, file_ids: list[str]) -> int:
        table_files = self._files.get(lake_table, {})
        removed = 0
        for fid in file_ids:
            if fid in table_files:
                del table_files[fid]
                removed += 1
        return removed

    def replace_file_list(
        self, lake_table: str, old_file_ids: list[str], new_files: list[PartitionFile]
    ) -> None:
        """原子替换：先注册新文件，再删除旧文件。保证归并不留半份清单。"""
        table_files = self._files.setdefault(lake_table, {})
        for nf in new_files:
            table_files[nf.file_id] = nf
        for fid in old_file_ids:
            table_files.pop(fid, None)

    # ---- 快照操作 ----

    def create_snapshot(
        self,
        lake_table: str,
        file_ids: list[str],
        parent_snapshot: str | None = None,
        added_rows: int = 0,
        deleted_rows: int = 0,
    ) -> LakeSnapshot:
        snap_id = f"snap-{lake_table}-{uuid.uuid4().hex[:8]}"
        manifest = snapshot_manifest_checksum(file_ids)
        snap = LakeSnapshot(
            snapshot_id=snap_id,
            lake_table=lake_table,
            parent_snapshot=parent_snapshot,
            file_ids=list(file_ids),
            added_rows=added_rows,
            deleted_rows=deleted_rows,
            manifest_checksum=manifest,
            status="generating",
            created_at=time.time(),
        )
        chain = self._snapshots.setdefault(lake_table, [])
        chain.append(snap)
        return snap

    def mark_snapshot_available(self, lake_table: str, snapshot_id: str) -> LakeSnapshot | None:
        for snap in self._snapshots.get(lake_table, []):
            if snap.snapshot_id == snapshot_id:
                snap.status = "available"
                return snap
        return None

    def mark_snapshot_expired(self, lake_table: str, snapshot_id: str) -> LakeSnapshot | None:
        for snap in self._snapshots.get(lake_table, []):
            if snap.snapshot_id == snapshot_id:
                snap.status = "expired"
                return snap
        return None

    def get_snapshot(self, lake_table: str, snapshot_id: str) -> LakeSnapshot | None:
        for snap in self._snapshots.get(lake_table, []):
            if snap.snapshot_id == snapshot_id:
                return snap
        return None

    def list_snapshots(self, lake_table: str) -> list[LakeSnapshot]:
        return list(self._snapshots.get(lake_table, []))

    def get_snapshot_at_time(self, lake_table: str, timestamp: float) -> LakeSnapshot | None:
        """时间旅行：返回 <=timestamp 的最近可用快照。"""
        candidates = [
            s for s in self._snapshots.get(lake_table, [])
            if s.status == "available" and s.created_at <= timestamp
        ]
        if not candidates:
            return None
        return max(candidates, key=lambda s: s.created_at)

    # ---- 灾备快照包：导出 / 回导 / 断点补数 ----

    def export_snapshot_package(self, lake_table: str, snapshot_id: str) -> dict[str, Any] | None:
        """为已可用快照导出分区清单包，供灾备端按原快照号回导。

        只有 available 快照可导出；快照引用的文件缺失时无法拼出完整包，返回 None。
        导出即登记灾备钉住：接收方确认完成前，源端回收这些文件会报告冲突。
        """
        snap = self.get_snapshot(lake_table, snapshot_id)
        if snap is None or snap.status != "available":
            return None
        files: list[dict[str, Any]] = []
        for fid in snap.file_ids:
            pf = self.get_file(lake_table, fid)
            if pf is None:
                return None
            files.append({
                "file_id": pf.file_id,
                "lake_table": pf.lake_table,
                "partition_key": pf.partition_key,
                "file_rows": pf.file_rows,
                "file_bytes": pf.file_bytes,
                "file_manifest": pf.file_manifest,
            })
        package: dict[str, Any] = {
            "package_type": DR_PACKAGE_TYPE,
            "lake_table": lake_table,
            "snapshot_id": snap.snapshot_id,
            "parent_snapshot": snap.parent_snapshot,
            "manifest_checksum": snap.manifest_checksum,
            "added_rows": snap.added_rows,
            "deleted_rows": snap.deleted_rows,
            "created_at": snap.created_at,
            "files": files,
        }
        package["package_checksum"] = dr_package_checksum(package)
        self._dr_pins.setdefault(lake_table, {})[snapshot_id] = set(snap.file_ids)
        return package

    def stage_snapshot_package(self, package: dict[str, Any]) -> dict[str, Any]:
        """回导第一步：整包逐项校核（湖表、快照、父快照、文件身份）并登记期望文件集。

        校验失败不影响已暂存或已生效的有效历史；同号不同清单视为历史冲突。
        """
        err = self._validate_dr_package(package)
        if err is not None:
            return err
        lake_table = package["lake_table"]
        snapshot_id = package["snapshot_id"]
        parent = package.get("parent_snapshot") or None

        # 快照校核：同号快照已在链上时，清单摘要与父快照必须一致
        existing = self.get_snapshot(lake_table, snapshot_id)
        if existing is not None:
            if existing.manifest_checksum != package["manifest_checksum"]:
                return self._dr_error(
                    "snapshot_history_conflict",
                    f"快照 {snapshot_id} 已绑定有效历史，拒绝内容不同的清单",
                    lake_table, snapshot_id)
            if (existing.parent_snapshot or None) != parent:
                return self._dr_error(
                    "parent_snapshot_mismatch",
                    f"快照 {snapshot_id} 的父快照与有效历史不一致",
                    lake_table, snapshot_id)
            if existing.status == "available":
                # 同一份包重复回导：幂等，不再重复暂存
                self._dr_staging.pop((lake_table, snapshot_id), None)
                return {
                    "success": True, "staged": False, "already_available": True,
                    "snapshot_id": snapshot_id,
                    "expected_files": len(existing.file_ids),
                    "verified_files": len(existing.file_ids),
                    "remaining_files": [],
                }
            return self._dr_error(
                "snapshot_exists_inactive",
                f"快照 {snapshot_id} 已存在且状态为 {existing.status}，回导不覆盖",
                lake_table, snapshot_id)

        # 快照校核：已暂存的期望与本次包不一致 → 历史冲突
        staged = self._dr_staging.get((lake_table, snapshot_id))
        if staged is not None:
            if staged["manifest_checksum"] != package["manifest_checksum"]:
                return self._dr_error(
                    "snapshot_history_conflict",
                    f"快照 {snapshot_id} 已暂存有效清单，拒绝内容不同的清单",
                    lake_table, snapshot_id)
            if staged["parent_snapshot"] != parent:
                return self._dr_error(
                    "parent_snapshot_mismatch",
                    f"快照 {snapshot_id} 的父快照与已暂存历史不一致",
                    lake_table, snapshot_id)

        # 文件身份校核：注册表已有同号文件但内容摘要不同 → 冒充冲突
        for entry in package["files"]:
            pf = self.get_file(lake_table, entry["file_id"])
            if pf is not None and pf.file_manifest != entry["file_manifest"]:
                return self._dr_error(
                    "file_identity_conflict",
                    f"文件 {entry['file_id']} 的内容摘要与已校验身份不一致",
                    lake_table, snapshot_id)

        if staged is None:
            staged = {
                "lake_table": lake_table,
                "snapshot_id": snapshot_id,
                "parent_snapshot": parent,
                "manifest_checksum": package["manifest_checksum"],
                "added_rows": package.get("added_rows", 0),
                "deleted_rows": package.get("deleted_rows", 0),
                "created_at": package.get("created_at", time.time()),
                "file_ids": [f["file_id"] for f in package["files"]],
                "expected_files": {f["file_id"]: dict(f) for f in package["files"]},
                "verified_files": set(),
                "staged_at": time.time(),
            }
            self._dr_staging[(lake_table, snapshot_id)] = staged
        return {
            "success": True, "staged": True, "already_available": False,
            "snapshot_id": snapshot_id,
            "expected_files": len(staged["expected_files"]),
            "verified_files": len(staged["verified_files"]),
            "remaining_files": sorted(set(staged["expected_files"]) - staged["verified_files"]),
        }

    def import_snapshot_files(
        self, lake_table: str, snapshot_id: str, files: list[dict[str, Any]]
    ) -> dict[str, Any]:
        """回导第二步：逐文件校验身份并幂等注册，整包到齐才按原快照号放开时间旅行。

        已校验过的文件重复投递只算一次；本批任何一个文件不合格则整批拒绝，
        此前已校验的文件保持有效，可断点续传补数。
        """
        existing = self.get_snapshot(lake_table, snapshot_id)
        staged = self._dr_staging.get((lake_table, snapshot_id))
        already_available = False
        if existing is not None and existing.status == "available":
            # 已生效快照：以链上清单与注册表身份为期准，重复导入幂等
            expected: dict[str, dict[str, Any]] = {}
            for fid in existing.file_ids:
                pf = self.get_file(lake_table, fid)
                if pf is not None:
                    expected[fid] = {
                        "file_id": pf.file_id,
                        "partition_key": pf.partition_key,
                        "file_rows": pf.file_rows,
                        "file_bytes": pf.file_bytes,
                        "file_manifest": pf.file_manifest,
                    }
            already_available = True
        elif staged is not None:
            expected = staged["expected_files"]
        else:
            return self._dr_error(
                "no_staged_import",
                f"快照 {snapshot_id} 尚未回导分区清单包，请先整包校验",
                lake_table, snapshot_id)

        # 先全量校核本批投递：任何一个不合格都整批拒绝
        for entry in files:
            if not isinstance(entry, dict) or not entry.get("file_id") or "file_manifest" not in entry:
                return self._dr_error(
                    "malformed_file_entry", "投递的文件条目缺少身份字段", lake_table, snapshot_id)
            fid = entry["file_id"]
            if fid not in expected:
                return self._dr_error(
                    "unexpected_file",
                    f"文件 {fid} 不在快照 {snapshot_id} 的有效清单内",
                    lake_table, snapshot_id)
            exp = expected[fid]
            if entry["file_manifest"] != exp["file_manifest"]:
                return self._dr_error(
                    "file_identity_conflict",
                    f"文件 {fid} 的内容摘要与已校验身份不一致",
                    lake_table, snapshot_id)
            for key in ("partition_key", "file_rows", "file_bytes"):
                if key in entry and entry[key] != exp[key]:
                    return self._dr_error(
                        "file_identity_conflict",
                        f"文件 {fid} 的 {key} 与已校验身份不一致",
                        lake_table, snapshot_id)

        # 全部通过才落注册：新文件注册，已校验文件只算一次
        accepted: list[str] = []
        duplicated: list[str] = []
        table_files = self._files.setdefault(lake_table, {})
        for entry in files:
            fid = entry["file_id"]
            exp = expected[fid]
            if fid in table_files:
                duplicated.append(fid)
            else:
                table_files[fid] = PartitionFile(
                    file_id=fid,
                    lake_table=lake_table,
                    partition_key=exp["partition_key"],
                    file_rows=int(exp["file_rows"]),
                    file_bytes=int(exp["file_bytes"]),
                    file_manifest=exp["file_manifest"],
                    created_at=time.time(),
                )
                accepted.append(fid)
            if staged is not None:
                staged["verified_files"].add(fid)

        # 整包文件到齐且全部通过校验 → 按原快照号落 available 快照
        completed = already_available
        if staged is not None and set(staged["expected_files"]) <= staged["verified_files"]:
            self._register_imported_snapshot(lake_table, staged)
            del self._dr_staging[(lake_table, snapshot_id)]
            completed = True
        status = self.dr_import_status(lake_table, snapshot_id)
        return {
            "success": True,
            "snapshot_id": snapshot_id,
            "files_accepted": accepted,
            "files_duplicated": duplicated,
            "completed": completed,
            "snapshot_status": status["status"],
            "expected_files": status["expected_files"],
            "verified_files": status["verified_files"],
            "remaining_files": status["remaining_files"],
        }

    def import_snapshot_package(self, package: dict[str, Any]) -> dict[str, Any]:
        """一次性回导整包：整包校验 + 包内文件全量投递。"""
        staged = self.stage_snapshot_package(package)
        if not staged["success"]:
            return staged
        result = self.import_snapshot_files(
            package["lake_table"], package["snapshot_id"], package["files"])
        if not result["success"]:
            return result
        result["staged"] = staged.get("staged", False)
        result["already_available"] = staged.get("already_available", False)
        return result

    def dr_import_status(self, lake_table: str, snapshot_id: str) -> dict[str, Any]:
        """回导进度核对：暂存中 / 已生效 / 未知，以及还差哪些文件。"""
        snap = self.get_snapshot(lake_table, snapshot_id)
        if snap is not None:
            return {
                "snapshot_id": snapshot_id,
                "status": snap.status,
                "staged": False,
                "manifest_checksum": snap.manifest_checksum,
                "parent_snapshot": snap.parent_snapshot,
                "file_ids": list(snap.file_ids),
                "expected_files": len(snap.file_ids),
                "verified_files": len(snap.file_ids),
                "remaining_files": [],
            }
        staged = self._dr_staging.get((lake_table, snapshot_id))
        if staged is not None:
            remaining = sorted(set(staged["expected_files"]) - staged["verified_files"])
            return {
                "snapshot_id": snapshot_id,
                "status": "restoring",
                "staged": True,
                "manifest_checksum": staged["manifest_checksum"],
                "parent_snapshot": staged["parent_snapshot"],
                "file_ids": list(staged["file_ids"]),
                "expected_files": len(staged["expected_files"]),
                "verified_files": len(staged["verified_files"]),
                "remaining_files": remaining,
            }
        return {
            "snapshot_id": snapshot_id,
            "status": "unknown",
            "staged": False,
            "expected_files": 0,
            "verified_files": 0,
            "remaining_files": [],
        }

    def ack_dr_restore(self, lake_table: str, snapshot_id: str) -> bool:
        """接收方确认恢复完成，解除源端对应快照的灾备钉住。"""
        pins = self._dr_pins.get(lake_table, {})
        return pins.pop(snapshot_id, None) is not None

    def dr_restore_pins(self, lake_table: str) -> dict[str, list[str]]:
        """查看湖表当前被灾备恢复钉住的文件。"""
        return {
            sid: sorted(fids)
            for sid, fids in self._dr_pins.get(lake_table, {}).items()
        }

    def reclaim_unreferenced_files(self, lake_table: str) -> dict[str, Any]:
        """回收不再被任何可用/生成中快照引用的文件。

        被灾备恢复钉住的文件照常回收（接收方持有自包含的旧包可补数），
        但在 dr_conflicts 中记录这次旧包恢复与回收的冲突。
        """
        referenced: set[str] = set()
        for snap in self._snapshots.get(lake_table, []):
            if snap.status in ("available", "generating"):
                referenced.update(snap.file_ids)
        pinned_by: dict[str, set[str]] = {}
        for sid, fids in self._dr_pins.get(lake_table, {}).items():
            for fid in fids:
                pinned_by.setdefault(fid, set()).add(sid)
        table_files = self._files.get(lake_table, {})
        removed: list[str] = []
        conflicts: list[dict[str, Any]] = []
        released_bytes = 0
        for fid in list(table_files):
            if fid in referenced:
                continue
            pf = table_files.pop(fid)
            removed.append(fid)
            released_bytes += pf.file_bytes
            if fid in pinned_by:
                conflicts.append({"file_id": fid, "pinned_by": sorted(pinned_by[fid])})
        return {
            "lake_table": lake_table,
            "removed_files": removed,
            "removed_count": len(removed),
            "released_bytes": released_bytes,
            "dr_conflicts": conflicts,
        }

    def _register_imported_snapshot(self, lake_table: str, staged: dict[str, Any]) -> LakeSnapshot:
        """整包校验通过后，按原快照号落 available 快照（幂等）。"""
        existing = self.get_snapshot(lake_table, staged["snapshot_id"])
        if existing is not None:
            return existing
        snap = LakeSnapshot(
            snapshot_id=staged["snapshot_id"],
            lake_table=lake_table,
            parent_snapshot=staged["parent_snapshot"],
            file_ids=list(staged["file_ids"]),
            added_rows=staged["added_rows"],
            deleted_rows=staged["deleted_rows"],
            manifest_checksum=staged["manifest_checksum"],
            status="available",
            created_at=staged["created_at"],
        )
        self._snapshots.setdefault(lake_table, []).append(snap)
        return snap

    def _validate_dr_package(self, package: dict[str, Any]) -> dict[str, Any] | None:
        """整包结构校核：类型、必要字段、整包摘要、湖表、快照清单、文件身份字段。"""
        if not isinstance(package, dict) or package.get("package_type") != DR_PACKAGE_TYPE:
            return self._dr_error("malformed_package", "不是有效的快照分区清单包", "", None)
        lake_table = package.get("lake_table") or ""
        snapshot_id = package.get("snapshot_id")
        required = ["lake_table", "snapshot_id", "manifest_checksum", "files", "package_checksum"]
        if any(package.get(key) in (None, "") for key in required):
            return self._dr_error("malformed_package", "包缺少必要字段", lake_table, snapshot_id)
        files = package["files"]
        if not isinstance(files, list):
            return self._dr_error("malformed_package", "包内文件清单不是列表", lake_table, snapshot_id)
        for entry in files:
            if not isinstance(entry, dict) or any(
                key not in entry
                for key in ("file_id", "partition_key", "file_rows", "file_bytes", "file_manifest")
            ):
                return self._dr_error(
                    "malformed_package", "包内文件条目缺少身份字段", lake_table, snapshot_id)
        if dr_package_checksum(package) != package["package_checksum"]:
            return self._dr_error(
                "package_checksum_mismatch", "整包摘要校验失败，包内容被篡改", lake_table, snapshot_id)
        for entry in files:
            if entry.get("lake_table") not in (None, lake_table):
                return self._dr_error(
                    "lake_table_mismatch",
                    f"文件 {entry['file_id']} 所属湖表与包不一致",
                    lake_table, snapshot_id)
        file_ids = [entry["file_id"] for entry in files]
        if len(set(file_ids)) != len(file_ids):
            return self._dr_error(
                "duplicate_file_in_package", "包内文件编号重复", lake_table, snapshot_id)
        if snapshot_manifest_checksum(file_ids) != package["manifest_checksum"]:
            return self._dr_error(
                "snapshot_manifest_mismatch",
                "快照清单摘要与包内文件范围不一致",
                lake_table, snapshot_id)
        return None

    def _valid_history(self, lake_table: str, snapshot_id: str | None) -> dict[str, Any] | None:
        """报错时附上当前仍有效的历史，便于核对哪份清单算数。"""
        if not snapshot_id:
            return None
        snap = self.get_snapshot(lake_table, snapshot_id)
        if snap is not None:
            return {
                "snapshot_id": snap.snapshot_id,
                "status": snap.status,
                "manifest_checksum": snap.manifest_checksum,
                "parent_snapshot": snap.parent_snapshot,
                "file_ids": list(snap.file_ids),
            }
        staged = self._dr_staging.get((lake_table, snapshot_id))
        if staged is not None:
            return {
                "snapshot_id": snapshot_id,
                "status": "restoring",
                "manifest_checksum": staged["manifest_checksum"],
                "parent_snapshot": staged["parent_snapshot"],
                "file_ids": list(staged["file_ids"]),
                "verified_files": sorted(staged["verified_files"]),
            }
        return None

    def _dr_error(
        self, code: str, message: str, lake_table: str, snapshot_id: str | None
    ) -> dict[str, Any]:
        return {
            "success": False,
            "error": code,
            "message": message,
            "lake_table": lake_table,
            "snapshot_id": snapshot_id,
            "valid_history": self._valid_history(lake_table, snapshot_id),
        }


    # ---- 水位操作 ----

    def set_watermark(
        self, lake_table: str, field_name: str, committed: str,
        strategy: str = "append"
    ) -> WatermarkRecord:
        wm = WatermarkRecord(
            lake_table=lake_table,
            field_name=field_name,
            committed_value=committed,
            pending_value=None,
            strategy=strategy,
            last_advanced_at=time.time(),
        )
        self._watermarks[lake_table] = wm
        return wm

    def advance_watermark(self, lake_table: str, new_value: str) -> WatermarkRecord | None:
        wm = self._watermarks.get(lake_table)
        if wm is None:
            return None
        wm.pending_value = new_value
        return wm

    def commit_watermark(self, lake_table: str) -> WatermarkRecord | None:
        wm = self._watermarks.get(lake_table)
        if wm is None or wm.pending_value is None:
            return None
        wm.committed_value = wm.pending_value
        wm.pending_value = None
        wm.last_advanced_at = time.time()
        return wm

    def get_watermark(self, lake_table: str) -> WatermarkRecord | None:
        return self._watermarks.get(lake_table)

    def list_watermarks(self) -> list[WatermarkRecord]:
        return list(self._watermarks.values())

    # ---- 批次数据 ----

    def store_batch(
        self, batch_id: str, lake_table: str, source_system: str,
        rows: list[dict[str, Any]], window_start: str, window_end: str,
    ) -> BatchRecord:
        checksum = hashlib.md5(
            str(sorted([str(r) for r in rows])).encode()
        ).hexdigest()
        br = BatchRecord(
            batch_id=batch_id,
            lake_table=lake_table,
            source_system=source_system,
            rows=list(rows),
            row_count=len(rows),
            window_start=window_start,
            window_end=window_end,
            checksum=checksum,
            snapshot_id=None,
            status="pending",
            created_at=time.time(),
        )
        self._batches[batch_id] = br
        return br

    def get_batch(self, batch_id: str) -> BatchRecord | None:
        return self._batches.get(batch_id)

    def list_batches(self, lake_table: str) -> list[BatchRecord]:
        return [b for b in self._batches.values() if b.lake_table == lake_table]

    def update_batch_status(self, batch_id: str, status: str, snapshot_id: str | None = None) -> BatchRecord | None:
        br = self._batches.get(batch_id)
        if br is None:
            return None
        br.status = status
        if snapshot_id is not None:
            br.snapshot_id = snapshot_id
        return br

    # ---- 隔离区 ----

    def quarantine_rows(
        self, batch_id: str, rule_id: str, rule_name: str,
        rows: list[dict[str, Any]]
    ) -> list[dict[str, Any]]:
        """将被质量规则拦截的行放入隔离区。"""
        quarantined = []
        for row in rows:
            qr = dict(row)
            qr["_quarantine_batch"] = batch_id
            qr["_quarantine_rule"] = rule_id
            qr["_quarantine_rule_name"] = rule_name
            qr["_quarantine_time"] = time.time()
            quarantined.append(qr)
        self._quarantine.setdefault(batch_id, []).extend(quarantined)
        return quarantined

    def list_quarantine(self, batch_id: str | None = None) -> list[dict[str, Any]]:
        if batch_id is not None:
            return list(self._quarantine.get(batch_id, []))
        result: list[dict[str, Any]] = []
        for rows in self._quarantine.values():
            result.extend(rows)
        return result

    def release_quarantine(self, batch_id: str) -> list[dict[str, Any]]:
        """回炉重入：从隔离区取出数据行。"""
        rows = self._quarantine.pop(batch_id, [])
        cleaned: list[dict[str, Any]] = []
        for r in rows:
            cleaned_row = {k: v for k, v in r.items() if not k.startswith("_quarantine_")}
            cleaned.append(cleaned_row)
        return cleaned

    # ---- 归并检查点 ----

    def save_merge_checkpoint(self, merge_id: str, partial_file_ids: list[str]) -> None:
        self._merge_checkpoints[merge_id] = {
            "partial_file_ids": list(partial_file_ids),
            "saved_at": time.time(),
        }

    def get_merge_checkpoint(self, merge_id: str) -> dict[str, Any] | None:
        return self._merge_checkpoints.get(merge_id)

    def clear_merge_checkpoint(self, merge_id: str) -> None:
        self._merge_checkpoints.pop(merge_id, None)

    # ---- 并发写锁 ----

    def acquire_write_lock(self, lake_table: str) -> threading.Lock:
        return self._write_lock.acquire(lake_table)

    def release_write_lock(self, lake_table: str) -> None:
        self._write_lock.release(lake_table)

    # ---- 统计快照 ----

    def stats_snapshot(self, lake_table: str) -> dict[str, Any]:
        """生成湖表统计画像快照。"""
        files = self.list_files(lake_table)
        snapshots = self.list_snapshots(lake_table)
        wm = self.get_watermark(lake_table)
        total_rows = sum(f.file_rows for f in files)
        total_bytes = sum(f.file_bytes for f in files)
        partition_count = len(set(f.partition_key for f in files))
        return {
            "lake_table": lake_table,
            "file_count": len(files),
            "total_rows": total_rows,
            "total_bytes": total_bytes,
            "partition_count": partition_count,
            "available_snapshots": sum(1 for s in snapshots if s.status == "available"),
            "watermark": wm.committed_value if wm else None,
        }


# 全局单例
lake_store = LakeStore()