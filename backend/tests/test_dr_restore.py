"""灾备快照包：分区清单导出、回导逐项校核、断点补数与冲突核对。

场景主线：
- S1 = A(3行) + B(4行)，接收方取完 A 时导入中断
- 源表发布 S2 用 C 替换 B，并回收 B（与旧包恢复冲突）
- 接收方从旧包补到 B，回溯 S1 读到 A、B 共 7 行；源表回放 S2 看到 A、C
- 拿 C 冒充 B、内容不同的清单重复导入 S1，都要报错并能核对有效历史
"""
from __future__ import annotations

import copy

from app.lake_store import (
    LakeStore,
    dr_package_checksum,
    snapshot_manifest_checksum,
)
from app.services.expire_policy import ExpirePolicyService
from app.services.snapshot_mgr import SnapshotMgrService
from app.services.time_travel import TimeTravelService

TBL = "tbl_dr"


def _build_source_with_s1():
    """源表：S1 = A(3行) + B(4行)，已可用。"""
    source = LakeStore()
    a = source.register_file(TBL, "20261101", rows=3, bytes_=768, manifest="md5-A")
    b = source.register_file(TBL, "20261101", rows=4, bytes_=1024, manifest="md5-B")
    s1 = source.create_snapshot(TBL, [a.file_id, b.file_id], added_rows=7, deleted_rows=0)
    source.mark_snapshot_available(TBL, s1.snapshot_id)
    return source, a, b, s1


def _publish_s2_replacing_b_with_c(source, s1, a):
    """源表发布 S2：C 替换 B，父快照为 S1。"""
    c = source.register_file(TBL, "20261101", rows=5, bytes_=1280, manifest="md5-C")
    s2 = source.create_snapshot(
        TBL, [a.file_id, c.file_id],
        parent_snapshot=s1.snapshot_id, added_rows=5, deleted_rows=4)
    source.mark_snapshot_available(TBL, s2.snapshot_id)
    return c, s2


class TestDrRestoreScenario:
    """主线：中断 → 源端换代回收 → 旧包补数 → 双端各自回放。"""

    def test_interrupt_backfill_and_replay(self):
        source, a, b, s1 = _build_source_with_s1()
        receiver = LakeStore()
        tt_src = TimeTravelService(engine=source)
        tt_dst = TimeTravelService(engine=receiver)

        # 接收方只拿到快照编号，没有那一代的分区文件范围 → 无法照原编号回放
        assert receiver.get_snapshot(TBL, s1.snapshot_id) is None
        assert not tt_dst.read_at_snapshot(TBL, s1.snapshot_id)["success"]

        # 为已可用的 S1 导出分区清单包，接收方整包校验后暂存
        pkg = source.export_snapshot_package(TBL, s1.snapshot_id)
        assert pkg is not None
        assert {f["file_id"] for f in pkg["files"]} == {a.file_id, b.file_id}
        staged = receiver.stage_snapshot_package(pkg)
        assert staged["success"] and staged["expected_files"] == 2
        assert sorted(staged["remaining_files"]) == sorted([a.file_id, b.file_id])

        # 取完 A 时导入中断：A 已校验落册，整包未齐 → 时间旅行仍不可用
        r1 = receiver.import_snapshot_files(TBL, s1.snapshot_id, [pkg["files"][0]])
        assert r1["success"] and not r1["completed"]
        assert r1["files_accepted"] == [a.file_id]
        assert receiver.get_snapshot(TBL, s1.snapshot_id) is None
        assert not tt_dst.read_at_snapshot(TBL, s1.snapshot_id)["success"]
        progress = receiver.dr_import_status(TBL, s1.snapshot_id)
        assert progress["status"] == "restoring"
        assert progress["remaining_files"] == [b.file_id]

        # 源表发布 S2：C 替换 B
        c, s2 = _publish_s2_replacing_b_with_c(source, s1, a)

        # 源表过期 S1 并回收不再被引用的文件：B 与旧包恢复发生冲突
        source.mark_snapshot_expired(TBL, s1.snapshot_id)
        gc = source.reclaim_unreferenced_files(TBL)
        assert gc["removed_files"] == [b.file_id]
        assert [d["file_id"] for d in gc["dr_conflicts"]] == [b.file_id]
        assert gc["dr_conflicts"][0]["pinned_by"] == [s1.snapshot_id]
        assert source.get_file(TBL, b.file_id) is None
        assert source.get_file(TBL, a.file_id) is not None  # S2 仍引用 A

        # 接收方从旧包补数：重复投 A 只算一次，补到 B 后整包生效
        dup = receiver.import_snapshot_files(TBL, s1.snapshot_id, [pkg["files"][0]])
        assert dup["success"] and not dup["completed"]
        assert dup["files_accepted"] == [] and dup["files_duplicated"] == [a.file_id]
        r2 = receiver.import_snapshot_files(TBL, s1.snapshot_id, [pkg["files"][1]])
        assert r2["success"] and r2["completed"]
        assert r2["files_accepted"] == [b.file_id]
        assert r2["snapshot_status"] == "available"
        assert len(receiver.list_files(TBL)) == 2  # A、B 各一份，没有重复计数

        # 回溯 S1：按原快照号读到 A、B 共 7 行
        got = tt_dst.read_at_snapshot(TBL, s1.snapshot_id)
        assert got["success"] and got["rows"] == 7
        assert set(got["file_ids"]) == {a.file_id, b.file_id}

        # 从源表回放 S2：看到 A、C
        got2 = tt_src.read_at_snapshot(TBL, s2.snapshot_id)
        assert got2["success"] and got2["rows"] == 8
        assert set(got2["file_ids"]) == {a.file_id, c.file_id}

        # 接收方确认完成后，源端解除钉住，再回收不再报冲突
        assert source.ack_dr_restore(TBL, s1.snapshot_id)
        assert source.dr_restore_pins(TBL) == {}


class TestDrForgeryAndConflicts:
    """负向：冒充文件、篡改包、内容不同的清单重复导入。"""

    def test_forged_file_rejected_and_history_intact(self):
        source, a, b, s1 = _build_source_with_s1()
        receiver = LakeStore()
        pkg = source.export_snapshot_package(TBL, s1.snapshot_id)
        assert receiver.stage_snapshot_package(pkg)["success"]
        receiver.import_snapshot_files(TBL, s1.snapshot_id, [pkg["files"][0]])

        # 拿 C 的内容冒充 B 的编号投递
        forged = dict(pkg["files"][1])
        forged["file_manifest"] = "md5-C"
        forged["file_rows"] = 5
        res = receiver.import_snapshot_files(TBL, s1.snapshot_id, [forged])
        assert not res["success"] and res["error"] == "file_identity_conflict"
        assert res["valid_history"]["manifest_checksum"] == pkg["manifest_checksum"]
        assert b.file_id in res["valid_history"]["file_ids"]

        # 直接投递清单外文件 C
        stranger = {
            "file_id": f"{TBL}-p20261101-c0000000",
            "lake_table": TBL,
            "partition_key": "20261101",
            "file_rows": 5,
            "file_bytes": 1280,
            "file_manifest": "md5-C",
        }
        res2 = receiver.import_snapshot_files(TBL, s1.snapshot_id, [stranger])
        assert not res2["success"] and res2["error"] == "unexpected_file"

        # 冒充被拒不影响已校验的 A：补到正版 B 后整包照常生效
        ok = receiver.import_snapshot_files(TBL, s1.snapshot_id, [pkg["files"][1]])
        assert ok["success"] and ok["completed"]
        got = TimeTravelService(engine=receiver).read_at_snapshot(TBL, s1.snapshot_id)
        assert got["success"] and got["rows"] == 7

    def test_conflicting_manifest_rejected_and_valid_history_kept(self):
        source, a, b, s1 = _build_source_with_s1()
        receiver = LakeStore()
        pkg = source.export_snapshot_package(TBL, s1.snapshot_id)

        # 一次性整包回导成功；同一份包重复导入幂等，文件只算一次
        first = receiver.import_snapshot_package(pkg)
        assert first["success"] and first["completed"]
        again = receiver.import_snapshot_package(pkg)
        assert again["success"] and again["already_available"]
        assert again["files_accepted"] == []
        assert sorted(again["files_duplicated"]) == sorted([a.file_id, b.file_id])
        assert len(receiver.list_files(TBL)) == 2

        # 接收方另有一个已生效版本（本地快照 SX），用于验证不受冲突波及
        d = receiver.register_file(TBL, "20261102", rows=11, bytes_=2816, manifest="md5-D")
        sx = receiver.create_snapshot(TBL, [d.file_id], added_rows=11)
        receiver.mark_snapshot_available(TBL, sx.snapshot_id)

        # 拿一份内容不同的清单（A、C）冒充 S1 重复导入：摘要重算也过不了绑定校核
        forged_pkg = copy.deepcopy(pkg)
        forged_pkg["files"] = [
            pkg["files"][0],
            {
                "file_id": f"{TBL}-p20261101-c0000000",
                "lake_table": TBL,
                "partition_key": "20261101",
                "file_rows": 5,
                "file_bytes": 1280,
                "file_manifest": "md5-C",
            },
        ]
        forged_pkg["manifest_checksum"] = snapshot_manifest_checksum(
            [f["file_id"] for f in forged_pkg["files"]])
        forged_pkg["package_checksum"] = dr_package_checksum(forged_pkg)
        res = receiver.stage_snapshot_package(forged_pkg)
        assert not res["success"] and res["error"] == "snapshot_history_conflict"

        # 通过报错核对哪份历史仍然有效：原来的 S1（A、B）
        valid = res["valid_history"]
        assert valid["status"] == "available"
        assert valid["manifest_checksum"] == pkg["manifest_checksum"]
        assert set(valid["file_ids"]) == {a.file_id, b.file_id}

        # 已生效的其他版本仍可回放
        tt = TimeTravelService(engine=receiver)
        got_s1 = tt.read_at_snapshot(TBL, s1.snapshot_id)
        assert got_s1["success"] and got_s1["rows"] == 7
        got_sx = tt.read_at_snapshot(TBL, sx.snapshot_id)
        assert got_sx["success"] and got_sx["rows"] == 11

    def test_tampered_package_rejected(self):
        source, a, b, s1 = _build_source_with_s1()
        receiver = LakeStore()
        pkg = source.export_snapshot_package(TBL, s1.snapshot_id)

        # 篡改包内文件内容但不动摘要 → 整包摘要校验失败
        bad = copy.deepcopy(pkg)
        bad["files"][1]["file_rows"] = 999
        res = receiver.stage_snapshot_package(bad)
        assert not res["success"] and res["error"] == "package_checksum_mismatch"

        # 改掉快照清单摘要再重算整包摘要 → 清单与文件范围对不上
        bad2 = copy.deepcopy(pkg)
        bad2["manifest_checksum"] = "0" * 32
        bad2["package_checksum"] = dr_package_checksum(bad2)
        res2 = receiver.stage_snapshot_package(bad2)
        assert not res2["success"] and res2["error"] == "snapshot_manifest_mismatch"

        # 篡改湖表归属 → 湖表校核失败
        bad3 = copy.deepcopy(pkg)
        bad3["files"][0]["lake_table"] = "tbl_other"
        bad3["package_checksum"] = dr_package_checksum(bad3)
        res3 = receiver.stage_snapshot_package(bad3)
        assert not res3["success"] and res3["error"] == "lake_table_mismatch"

        # 篡改父快照 → 父快照校核失败（先暂存正版，再投篡改版）
        assert receiver.stage_snapshot_package(pkg)["success"]
        bad4 = copy.deepcopy(pkg)
        bad4["parent_snapshot"] = "snap-evil-parent"
        bad4["package_checksum"] = dr_package_checksum(bad4)
        res4 = receiver.stage_snapshot_package(bad4)
        assert not res4["success"] and res4["error"] == "parent_snapshot_mismatch"
        assert res4["valid_history"]["parent_snapshot"] is None

    def test_import_without_stage_rejected(self):
        receiver = LakeStore()
        res = receiver.import_snapshot_files(TBL, "snap-never-staged", [])
        assert not res["success"] and res["error"] == "no_staged_import"


class TestDrServiceApi:
    """服务层：导出门槛、跨引擎往返、回收冲突。"""

    def test_export_requires_available_snapshot(self):
        engine = LakeStore()
        svc = SnapshotMgrService(engine=engine)
        pf = engine.register_file("t_svc", "20261101", rows=1, bytes_=256)
        snap = engine.create_snapshot("t_svc", [pf.file_id])

        # 生成中不可导出
        assert not svc.export_partition_manifest("t_svc", snap.snapshot_id)["success"]
        # 可用后可导出
        engine.mark_snapshot_available("t_svc", snap.snapshot_id)
        ok = svc.export_partition_manifest("t_svc", snap.snapshot_id)
        assert ok["success"] and ok["file_count"] == 1
        # 已过期不可导出
        engine.mark_snapshot_expired("t_svc", snap.snapshot_id)
        assert not svc.export_partition_manifest("t_svc", snap.snapshot_id)["success"]
        # 不存在不可导出
        assert not svc.export_partition_manifest("t_svc", "snap-none")["success"]

    def test_service_roundtrip_between_stores(self):
        source = LakeStore()
        receiver = LakeStore()
        src_svc = SnapshotMgrService(engine=source)
        dst_svc = SnapshotMgrService(engine=receiver)
        tt = TimeTravelService(engine=receiver)

        pf = source.register_file("t_rt", "20261101", rows=9, bytes_=2304, manifest="md5-X")
        snap = source.create_snapshot("t_rt", [pf.file_id], added_rows=9)
        source.mark_snapshot_available("t_rt", snap.snapshot_id)

        exported = src_svc.export_partition_manifest("t_rt", snap.snapshot_id)
        assert exported["success"]
        imported = dst_svc.import_partition_manifest(exported["package"])
        assert imported["success"] and imported["completed"]

        got = tt.read_at_snapshot("t_rt", snap.snapshot_id)
        assert got["success"] and got["rows"] == 9
        status = dst_svc.dr_import_status("t_rt", snap.snapshot_id)
        assert status["status"] == "available"

    def test_reclaim_reports_dr_conflict_and_ack_clears(self):
        source, a, b, s1 = _build_source_with_s1()
        svc = ExpirePolicyService(engine=source)

        # 导出即钉住；过期 S1 后回收，A、B 都与旧包恢复冲突
        pkg = source.export_snapshot_package(TBL, s1.snapshot_id)
        assert source.dr_restore_pins(TBL) == {
            s1.snapshot_id: sorted([a.file_id, b.file_id])
        }
        source.mark_snapshot_expired(TBL, s1.snapshot_id)
        gc = svc.reclaim_unreferenced_files(TBL)
        assert sorted(gc["removed_files"]) == sorted([a.file_id, b.file_id])
        assert {d["file_id"] for d in gc["dr_conflicts"]} == {a.file_id, b.file_id}

        # 接收方确认完成后解除钉住，再回收无冲突
        assert source.ack_dr_restore(TBL, s1.snapshot_id)
        gc2 = svc.reclaim_unreferenced_files(TBL)
        assert gc2["dr_conflicts"] == [] and gc2["removed_files"] == []
