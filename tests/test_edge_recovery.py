from __future__ import annotations

import json
from pathlib import Path

import pytest

from manufacturing_data_platform import edge_recovery as er
from manufacturing_data_platform.kafka_ingestion.contracts import (
    sample_machine_event,
    serialize_machine_event,
)
from manufacturing_data_platform.kafka_ingestion.landing import KafkaRecord, land_records
from manufacturing_data_platform.pipeline.lakehouse import DATASET_ID, state_dir


TOPIC = "manufacturing.machine-events.v1"
BUSINESS_DATE = "2026-06-29"
OTHER_DATE = "2026-06-30"
EDGE = "edge-plant-a"
SESSION = "boot-0001"


# --------------------------------------------------------------------------- #
# Helpers
# --------------------------------------------------------------------------- #
def _spool_three(spool: Path) -> None:
    for seq in (1, 2, 3):
        er.append_edge_event(
            spool_root=spool,
            edge_source_id=EDGE,
            boot_session_id=SESSION,
            sequence_no=seq,
            event=sample_machine_event(seq),
        )


def _seal(spool: Path, last: int = 3) -> dict:
    return er.seal_edge_session(
        spool_root=spool,
        edge_source_id=EDGE,
        boot_session_id=SESSION,
        expected_last_sequence=last,
    )


def _session(spool: Path) -> er.SealedSession:
    return er.load_sealed_session(
        spool_root=spool, edge_source_id=EDGE, boot_session_id=SESSION
    )


def _land(landing: Path, indexes: list[int], start_offset: int) -> None:
    """Replay the given edge sequences into the central landing at NEW Kafka offsets."""
    records = []
    for position, index in enumerate(indexes):
        event = sample_machine_event(index)
        records.append(
            KafkaRecord(
                topic=TOPIC,
                partition=0,
                offset=start_offset + position,
                key=event["machine_id"],
                value=serialize_machine_event(event),
                timestamp_ms=1_783_000_000_000 + start_offset + position,
            )
        )
    land_records(records, landing)


def _accepted_total(landing: Path) -> int:
    from manufacturing_data_platform.kafka_ingestion.landing import load_landing_index

    return len(load_landing_index(landing)["accepted_events"])


def _promote(spool: Path, landing: Path, tmp_path: Path, name: str = "run") -> dict:
    return er.promote_recovered_session(
        spool_root=spool,
        edge_source_id=EDGE,
        boot_session_id=SESSION,
        landing_dir=landing,
        business_date=BUSINESS_DATE,
        adapter_output_dir=tmp_path / "adapter",
        lakehouse_output_dir=tmp_path / "lakehouse",
        evidence_file=tmp_path / f"{name}_evidence.json",
    )


# --------------------------------------------------------------------------- #
# 1. Canonical spool entry + sealed manifest
# --------------------------------------------------------------------------- #
def test_canonical_entry_and_seal_are_deterministic(tmp_path):
    spool = tmp_path / "spool"
    _spool_three(spool)
    seal = _seal(spool)

    session_path = er.session_dir(spool, EDGE, SESSION)
    entry_file = er.entry_dir(session_path, 1) / er.ENTRY_NAME
    assert entry_file.exists()

    # Entry content IS the canonical envelope: no wall-clock, byte-stable.
    envelope = json.loads(entry_file.read_text(encoding="utf-8"))
    assert envelope["format_version"] == er.FORMAT_VERSION
    assert envelope["edge_source_id"] == EDGE
    assert envelope["sequence_no"] == 1
    assert envelope["event"]["event_id"] == sample_machine_event(1)["event_id"]
    assert entry_file.read_bytes() == er.canonical_envelope_bytes(envelope)
    assert "timestamp" not in envelope and "sealed_at" not in envelope

    assert seal["expected_last_sequence"] == 3
    assert seal["sealed_event_count"] == 3
    assert [e["sequence_no"] for e in seal["entries"]] == [1, 2, 3]

    session = _session(spool)
    assert [e.sequence_no for e in session.entries] == [1, 2, 3]
    assert len(set(session.event_ids)) == 3


# --------------------------------------------------------------------------- #
# 2-4. Idempotent reuse, conflicts, and seal rules
# --------------------------------------------------------------------------- #
def test_same_coordinate_same_bytes_is_reused(tmp_path):
    spool = tmp_path / "spool"
    first = er.append_edge_event(
        spool_root=spool, edge_source_id=EDGE, boot_session_id=SESSION,
        sequence_no=1, event=sample_machine_event(1),
    )
    second = er.append_edge_event(
        spool_root=spool, edge_source_id=EDGE, boot_session_id=SESSION,
        sequence_no=1, event=sample_machine_event(1),
    )
    assert first.status == "appended"
    assert second.status == "reused"
    assert second.fingerprint == first.fingerprint
    assert len(list(er.session_dir(spool, EDGE, SESSION).glob("seq=*"))) == 1


def test_same_coordinate_different_bytes_conflicts(tmp_path):
    spool = tmp_path / "spool"
    er.append_edge_event(
        spool_root=spool, edge_source_id=EDGE, boot_session_id=SESSION,
        sequence_no=1, event=sample_machine_event(1),
    )
    with pytest.raises(er.EdgeSpoolConflictError, match="different canonical bytes"):
        er.append_edge_event(
            spool_root=spool, edge_source_id=EDGE, boot_session_id=SESSION,
            sequence_no=1, event=sample_machine_event(2),
        )


def test_unsafe_identifier_is_rejected(tmp_path):
    for bad in ("../escape", "edge id", ""):
        with pytest.raises(er.EdgeIdentifierError):
            er.append_edge_event(
                spool_root=tmp_path / "spool", edge_source_id=bad,
                boot_session_id=SESSION, sequence_no=1, event=sample_machine_event(1),
            )


def test_duplicate_event_id_at_another_sequence_is_rejected(tmp_path):
    spool = tmp_path / "spool"
    er.append_edge_event(
        spool_root=spool, edge_source_id=EDGE, boot_session_id=SESSION,
        sequence_no=1, event=sample_machine_event(1),
    )
    with pytest.raises(er.EdgeSpoolConflictError, match="already spooled at sequence 1"):
        er.append_edge_event(
            spool_root=spool, edge_source_id=EDGE, boot_session_id=SESSION,
            sequence_no=2, event=sample_machine_event(1),
        )


def test_seal_rejects_missing_sequence(tmp_path):
    spool = tmp_path / "spool"
    for seq in (1, 3):  # 2 is absent
        er.append_edge_event(
            spool_root=spool, edge_source_id=EDGE, boot_session_id=SESSION,
            sequence_no=seq, event=sample_machine_event(seq),
        )
    with pytest.raises(er.EdgeSealError, match=r"\[2\]"):
        _seal(spool, last=3)
    assert not er.is_sealed(spool, EDGE, SESSION)


def test_append_after_seal_is_rejected_and_reseal_must_match(tmp_path):
    spool = tmp_path / "spool"
    _spool_three(spool)
    _seal(spool)

    with pytest.raises(er.EdgeSealError, match="is sealed"):
        er.append_edge_event(
            spool_root=spool, edge_source_id=EDGE, boot_session_id=SESSION,
            sequence_no=4, event=sample_machine_event(4),
        )
    # Re-sealing with the same expectation is idempotent; changing it is refused.
    assert _seal(spool)["expected_last_sequence"] == 3
    with pytest.raises(er.EdgeSealError, match="already sealed"):
        _seal(spool, last=2)


# --------------------------------------------------------------------------- #
# 5. Partial coverage reports the exact missing sequence
# --------------------------------------------------------------------------- #
def test_partial_coverage_reports_exact_missing_sequence(tmp_path):
    spool, landing = tmp_path / "spool", tmp_path / "raw"
    _spool_three(spool)
    _seal(spool)

    empty = er.compute_recovery_coverage(session=_session(spool), landing_dir=landing)
    assert empty["central_accepted_sequence_count"] == 0
    assert empty["missing_sequences"] == [1, 2, 3]
    assert empty["recovery_complete"] is False

    _land(landing, [1, 2], start_offset=0)
    partial = er.compute_recovery_coverage(session=_session(spool), landing_dir=landing)
    assert partial["central_accepted_sequence_count"] == 2
    assert partial["missing_sequences"] == [3]
    assert partial["recovery_complete"] is False
    assert _accepted_total(landing) == 2


# --------------------------------------------------------------------------- #
# 6. Incomplete recovery blocks K1.5 and creates no downstream output
# --------------------------------------------------------------------------- #
def test_incomplete_recovery_blocks_promotion_without_side_effects(tmp_path):
    spool, landing = tmp_path / "spool", tmp_path / "raw"
    _spool_three(spool)
    _seal(spool)
    _land(landing, [1, 2], start_offset=0)

    with pytest.raises(er.RecoveryIncompleteError, match=r"missing edge sequences \[3\]"):
        _promote(spool, landing, tmp_path)

    # No adapter version, no lakehouse run, no trusted-state pointer.
    assert not (tmp_path / "adapter").exists()
    assert not (tmp_path / "lakehouse").exists()
    assert not (state_dir(tmp_path / "lakehouse", DATASET_ID) / f"business_date={BUSINESS_DATE}.json").exists()


# --------------------------------------------------------------------------- #
# 7-10. Complete recovery, replay idempotency, identity separation
# --------------------------------------------------------------------------- #
def test_complete_recovery_permits_promotion_and_replay_is_idempotent(tmp_path):
    spool, landing = tmp_path / "spool", tmp_path / "raw"
    _spool_three(spool)
    _seal(spool)

    # partial -> complete: sequence 3 arrives at NEW Kafka offsets alongside replays of 1-2.
    _land(landing, [1, 2], start_offset=0)
    assert _accepted_total(landing) == 2
    _land(landing, [1, 2, 3], start_offset=2)
    assert _accepted_total(landing) == 3  # 1,2 duplicate evidence; 3 newly accepted

    coverage = er.compute_recovery_coverage(session=_session(spool), landing_dir=landing)
    assert coverage["missing_sequences"] == []
    assert coverage["recovery_complete"] is True

    first = _promote(spool, landing, tmp_path, name="first")
    assert first["bridge"]["lakehouse"]["status"] == "processed"
    assert first["bridge"]["lakehouse"]["quality_passed"] is True
    assert first["bridge"]["lakehouse"]["row_counts"]["silver"] == 3

    # 8. Repeated producer replay at new coordinates does not grow the accepted set.
    _land(landing, [1, 2, 3], start_offset=5)
    assert _accepted_total(landing) == 3

    # 9. Duplicate-only replay: canonical source_hash unchanged, bridge rerun skipped.
    second = _promote(spool, landing, tmp_path, name="second")
    assert second["bridge"]["adapter"]["source_hash"] == first["bridge"]["adapter"]["source_hash"]
    assert second["bridge"]["lakehouse"]["status"] == "skipped"
    assert second["bridge"]["lakehouse"]["run_id"] == first["bridge"]["lakehouse"]["run_id"]
    assert second["bridge"]["lakehouse"]["gold_rows"] == first["bridge"]["lakehouse"]["gold_rows"]

    # 10. The five identity spaces stay distinguishable in the persisted evidence.
    persisted = json.loads((tmp_path / "second_evidence.json").read_text(encoding="utf-8"))
    ident = persisted["identities"]
    assert ident["edge_sequence"] == [1, 2, 3]
    assert len(ident["event_id"]) == 3
    offsets = [c["kafka_offset"] for c in ident["kafka_coordinate"]]
    assert offsets == [0, 1, 4]  # transport offsets differ from edge sequence 1,2,3
    assert ident["adapter_source_hash"] == first["bridge"]["adapter"]["source_hash"]
    assert ident["lakehouse_run_id"] == first["bridge"]["lakehouse"]["run_id"]
    assert ident["edge_sequence"] != offsets


# --------------------------------------------------------------------------- #
# H1. Bounded session scope: one machine_id, one business_date
# --------------------------------------------------------------------------- #
def _event_with(index: int, **over) -> dict:
    event = sample_machine_event(index)
    event.update(over)
    return event


def test_seal_rejects_mixed_machine_id(tmp_path):
    spool = tmp_path / "spool"
    er.append_edge_event(
        spool_root=spool, edge_source_id=EDGE, boot_session_id=SESSION,
        sequence_no=1, event=_event_with(1, machine_id="mc-101"),
    )
    er.append_edge_event(
        spool_root=spool, edge_source_id=EDGE, boot_session_id=SESSION,
        sequence_no=2, event=_event_with(2, machine_id="mc-202"),
    )
    with pytest.raises(er.EdgeSessionScopeError, match="exactly one machine_id"):
        _seal(spool, last=2)
    assert not er.is_sealed(spool, EDGE, SESSION)


def test_seal_rejects_mixed_business_date(tmp_path):
    spool = tmp_path / "spool"
    er.append_edge_event(
        spool_root=spool, edge_source_id=EDGE, boot_session_id=SESSION,
        sequence_no=1, event=_event_with(1, business_date=BUSINESS_DATE),
    )
    er.append_edge_event(
        spool_root=spool, edge_source_id=EDGE, boot_session_id=SESSION,
        sequence_no=2, event=_event_with(2, business_date=OTHER_DATE),
    )
    with pytest.raises(er.EdgeSessionScopeError, match="exactly one business_date"):
        _seal(spool, last=2)
    assert not er.is_sealed(spool, EDGE, SESSION)


def test_seal_persists_session_scope(tmp_path):
    spool = tmp_path / "spool"
    _spool_three(spool)
    seal = _seal(spool)
    assert seal["machine_id"] == sample_machine_event(1)["machine_id"]
    assert seal["business_date"] == BUSINESS_DATE

    session = _session(spool)
    assert session.machine_id == seal["machine_id"]
    assert session.business_date == BUSINESS_DATE


def test_promotion_rejects_business_date_mismatch_without_side_effects(tmp_path):
    spool, landing = tmp_path / "spool", tmp_path / "raw"
    _spool_three(spool)
    _seal(spool)
    _land(landing, [1, 2, 3], start_offset=0)  # fully recovered

    with pytest.raises(er.EdgeSessionScopeError, match="does not match the sealed session date"):
        er.promote_recovered_session(
            spool_root=spool, edge_source_id=EDGE, boot_session_id=SESSION,
            landing_dir=landing, business_date=OTHER_DATE,
            adapter_output_dir=tmp_path / "adapter",
            lakehouse_output_dir=tmp_path / "lakehouse",
            evidence_file=tmp_path / "evidence.json",
        )

    # No adapter version, no lakehouse run, no trusted pointer, no evidence document.
    assert not (tmp_path / "adapter").exists()
    assert not (tmp_path / "lakehouse").exists()
    assert not (tmp_path / "evidence.json").exists()


# --------------------------------------------------------------------------- #
# M1. The sealed contract is fully re-validated on load and on reuse
# --------------------------------------------------------------------------- #
def test_entry_added_after_sealing_is_rejected_on_load(tmp_path):
    spool = tmp_path / "spool"
    _spool_three(spool)
    _seal(spool)

    # Bypass the append guard the way a stray writer would: drop a 4th entry directly.
    session_path = er.session_dir(spool, EDGE, SESSION)
    extra_dir = er.entry_dir(session_path, 4)
    extra_dir.mkdir(parents=True)
    envelope = er.canonical_envelope(
        edge_source_id=EDGE, boot_session_id=SESSION, sequence_no=4,
        event=sample_machine_event(4),
    )
    (extra_dir / er.ENTRY_NAME).write_bytes(er.canonical_envelope_bytes(envelope))

    with pytest.raises(er.EdgeSealError, match=r"extra spool entries \[4\]"):
        _session(spool)
    # Re-sealing at the same expectation must also refuse the tampered spool.
    with pytest.raises(er.EdgeSealError, match=r"extra spool entries \[4\]"):
        _seal(spool)


def test_tampered_seal_manifest_is_rejected(tmp_path):
    spool = tmp_path / "spool"
    _spool_three(spool)
    _seal(spool)
    seal_file = er.seal_path(er.session_dir(spool, EDGE, SESSION))

    def rewrite(mutate) -> None:
        doc = json.loads(seal_file.read_text(encoding="utf-8"))
        mutate(doc)
        seal_file.write_text(json.dumps(doc, indent=2, sort_keys=True), encoding="utf-8")

    def _set(key, value):
        return lambda doc: doc.__setitem__(key, value)

    for mutate, pattern in [
        (_set("sealed_event_count", 99), "sealed_event_count"),
        (_set("edge_source_id", "other-edge"), "edge_source_id does not match"),
        (_set("boot_session_id", "other-session"), "boot_session_id does not match"),
        (_set("format_version", 99), "format_version"),
        (_set("machine_id", "mc-999"), "machine_id"),
        (_set("business_date", OTHER_DATE), "business_date"),
        # Drop a declared entry AND fix the count, so the exact-range check is what rejects it.
        (
            lambda doc: (
                doc["entries"].pop(),
                doc.__setitem__("sealed_event_count", len(doc["entries"])),
            ),
            "declare exactly sequences",
        ),
        (lambda doc: doc["entries"][0].__setitem__("fingerprint", "deadbeef"), "fingerprint"),
        (lambda doc: doc["entries"][0].__setitem__("event_id", "evt-tampered"), "event_id"),
    ]:
        seal_file.unlink()   # restore a valid seal each round
        _seal(spool)
        rewrite(mutate)
        with pytest.raises(er.EdgeSealError, match=pattern):
            _session(spool)


def test_completeness_is_not_inferred_from_kafka_offset_continuity(tmp_path):
    """A contiguous-looking landing that misses one edge event is still incomplete."""
    spool, landing = tmp_path / "spool", tmp_path / "raw"
    _spool_three(spool)
    _seal(spool)
    _land(landing, [1, 2], start_offset=0)  # offsets 0,1 are contiguous

    coverage = er.compute_recovery_coverage(session=_session(spool), landing_dir=landing)
    assert coverage["recovery_complete"] is False
    assert coverage["missing_sequences"] == [3]
