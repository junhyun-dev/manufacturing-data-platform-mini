from __future__ import annotations

import importlib.util
import json
from pathlib import Path

import pytest

from manufacturing_data_platform import edge_recovery as er
from manufacturing_data_platform.kafka_ingestion.contracts import (
    sample_machine_event,
    serialize_machine_event,
)
from manufacturing_data_platform.kafka_ingestion.landing import KafkaRecord, land_records
from manufacturing_data_platform.pipeline import recovered_telemetry_publish as s9


TOPIC = "manufacturing.machine-events.v1"
BUSINESS_DATE = "2026-06-29"
OTHER_DATE = "2026-06-30"
EDGE = "edge-plant-a"
SESSION = "boot-0001"


# --------------------------------------------------------------------------- #
# Helpers — real S8 spool + real K1 landing writer, no mocks for the Core path
# --------------------------------------------------------------------------- #
def _event(index: int, **over) -> dict:
    event = sample_machine_event(index)
    event.update(over)
    return event


def _spool_and_seal(spool: Path, indexes=(1, 2, 3)) -> dict:
    for seq, index in enumerate(indexes, start=1):
        er.append_edge_event(
            spool_root=spool, edge_source_id=EDGE, boot_session_id=SESSION,
            sequence_no=seq, event=_event(index),
        )
    return er.seal_edge_session(
        spool_root=spool, edge_source_id=EDGE, boot_session_id=SESSION,
        expected_last_sequence=len(indexes),
    )


def _land(landing: Path, indexes: list[int], start_offset: int, **over) -> None:
    records = []
    for position, index in enumerate(indexes):
        event = _event(index, **over)
        records.append(
            KafkaRecord(
                topic=TOPIC, partition=0, offset=start_offset + position,
                key=event["machine_id"], value=serialize_machine_event(event),
                timestamp_ms=1_783_000_000_000 + start_offset + position,
            )
        )
    land_records(records, landing)


def _paths(tmp_path: Path) -> dict:
    return {
        "spool_root": tmp_path / "spool",
        "landing_dir": tmp_path / "raw",
        "adapter_output_dir": tmp_path / "adapter",
        "warehouse_path": tmp_path / "warehouse",
        "evidence_output_dir": tmp_path / "evidence",
    }


def _run(tmp_path: Path, business_date: str = BUSINESS_DATE, **over):
    p = _paths(tmp_path)
    p.update(over)
    return s9.run_recovered_telemetry_publish(
        edge_source_id=EDGE, boot_session_id=SESSION, business_date=business_date, **p
    )


def _no_downstream_output(tmp_path: Path) -> bool:
    """Iceberg warehouse and S9 evidence must not exist when a gate refuses."""
    return not (tmp_path / "warehouse").exists() and not (
        tmp_path / "evidence" / "s9_recovered_telemetry_publish.json"
    ).exists()


class _SparkSpy:
    """Records whether the S7 callable was invoked at all."""

    def __init__(self):
        self.calls: list[dict] = []

    def __call__(self, **kwargs):
        self.calls.append(kwargs)
        raise AssertionError("S7 run_spark_machine_event_batch must not be called")


def _patch_spark(monkeypatch, replacement):
    import manufacturing_data_platform.pipeline.spark_machine_event_batch as s7

    monkeypatch.setattr(s7, "run_spark_machine_event_batch", replacement)
    return replacement


# --------------------------------------------------------------------------- #
# 1. The shared S8 gate is reused, not duplicated
# --------------------------------------------------------------------------- #
def test_s8_promotion_uses_the_shared_gate(tmp_path, monkeypatch):
    calls: list[dict] = []
    real_gate = er.require_recovery_ready

    def spy(**kwargs):
        calls.append(kwargs)
        return real_gate(**kwargs)

    monkeypatch.setattr(er, "require_recovery_ready", spy)

    spool, landing = tmp_path / "spool", tmp_path / "raw"
    _spool_and_seal(spool)
    _land(landing, [1, 2, 3], start_offset=0)

    evidence = er.promote_recovered_session(
        spool_root=spool, edge_source_id=EDGE, boot_session_id=SESSION,
        landing_dir=landing, business_date=BUSINESS_DATE,
        adapter_output_dir=tmp_path / "adapter",
        lakehouse_output_dir=tmp_path / "lakehouse",
    )
    assert len(calls) == 1  # S8 delegates to the one shared gate
    assert evidence["coverage"]["recovery_complete"] is True
    assert evidence["bridge"]["lakehouse"]["status"] == "processed"


def test_s9_uses_the_shared_gate(tmp_path, monkeypatch):
    calls: list[dict] = []
    real_gate = er.require_recovery_ready

    def spy(**kwargs):
        calls.append(kwargs)
        return real_gate(**kwargs)

    monkeypatch.setattr(s9, "require_recovery_ready", spy)
    spark = _patch_spark(monkeypatch, _SparkSpy())

    spool, landing = tmp_path / "spool", tmp_path / "raw"
    _spool_and_seal(spool)
    _land(landing, [1, 2], start_offset=0)  # incomplete

    with pytest.raises(er.RecoveryIncompleteError):
        _run(tmp_path)
    assert len(calls) == 1
    assert spark.calls == []


# --------------------------------------------------------------------------- #
# 2-3. Incomplete recovery and date mismatch stop before any downstream output
# --------------------------------------------------------------------------- #
def test_incomplete_recovery_blocks_before_adapter_spark_and_iceberg(tmp_path, monkeypatch):
    spark = _patch_spark(monkeypatch, _SparkSpy())
    spool, landing = tmp_path / "spool", tmp_path / "raw"
    _spool_and_seal(spool)
    _land(landing, [1, 2], start_offset=0)

    with pytest.raises(er.RecoveryIncompleteError, match=r"missing edge sequences \[3\]"):
        _run(tmp_path)

    assert spark.calls == []
    assert not (tmp_path / "adapter").exists()   # gate runs before the adapter
    assert _no_downstream_output(tmp_path)


def test_requested_date_mismatch_blocks_before_downstream_output(tmp_path, monkeypatch):
    spark = _patch_spark(monkeypatch, _SparkSpy())
    spool, landing = tmp_path / "spool", tmp_path / "raw"
    _spool_and_seal(spool)
    _land(landing, [1, 2, 3], start_offset=0)

    with pytest.raises(er.EdgeSessionScopeError, match="does not match the sealed session date"):
        _run(tmp_path, business_date=OTHER_DATE)

    assert spark.calls == []
    assert not (tmp_path / "adapter").exists()
    assert _no_downstream_output(tmp_path)


# --------------------------------------------------------------------------- #
# 4. Complete recovery calls S7 with the adapter's own csv_path and source_hash
# --------------------------------------------------------------------------- #
def test_complete_recovery_calls_s7_with_adapter_identity(tmp_path, monkeypatch):
    captured: list[dict] = []

    def fake_spark(**kwargs):
        captured.append(kwargs)
        return {
            "business_date": kwargs["business_date"], "table": "local.db.gold_daily_metrics",
            "run_id": "run-s9-1", "source_hash": kwargs["source_hash"], "status": "published",
            "quality": {"passed": True, "checks": [{"name": "q", "status": "pass"}]},
            "row_counts": {"input": 3, "silver": 3, "gold": 1},
            "gold_snapshot_id": 4242, "snapshot_count": 1, "target_partition_row_count": 1,
        }

    _patch_spark(monkeypatch, fake_spark)

    spool, landing = tmp_path / "spool", tmp_path / "raw"
    _spool_and_seal(spool)
    _land(landing, [1, 2, 3], start_offset=0)

    evidence = _run(tmp_path)

    assert len(captured) == 1
    call = captured[0]
    adapter_csv = Path(evidence["adapter"]["csv_path"])
    assert Path(call["csv_path"]) == adapter_csv          # existing adapter CSV, not a copy
    assert call["source_hash"] == evidence["adapter"]["source_hash"]
    assert call["business_date"] == BUSINESS_DATE
    assert evidence["status"] == "published"
    assert evidence["spark"]["source_hash"] == evidence["adapter"]["source_hash"]


# --------------------------------------------------------------------------- #
# 5-6. Exact-session input contract
# --------------------------------------------------------------------------- #
def test_extra_same_date_accepted_event_blocks_publication(tmp_path, monkeypatch):
    spark = _patch_spark(monkeypatch, _SparkSpy())
    spool, landing = tmp_path / "spool", tmp_path / "raw"
    _spool_and_seal(spool)                       # sealed session = events 1..3
    _land(landing, [1, 2, 3], start_offset=0)
    _land(landing, [4], start_offset=3)          # extra accepted event, same business_date

    with pytest.raises(s9.SessionInputMismatchError, match="extra_event_ids"):
        _run(tmp_path)

    assert spark.calls == []                     # S7 never started
    assert _no_downstream_output(tmp_path)       # no Iceberg/table/snapshot/evidence


def test_other_date_accepted_events_do_not_poison_the_session(tmp_path, monkeypatch):
    def fake_spark(**kwargs):
        return {
            "business_date": kwargs["business_date"], "table": "local.db.gold_daily_metrics",
            "run_id": "run-s9-2", "source_hash": kwargs["source_hash"], "status": "published",
            "quality": {"passed": True, "checks": []},
            "row_counts": {"input": 3, "silver": 3, "gold": 1},
            "gold_snapshot_id": 77, "snapshot_count": 1, "target_partition_row_count": 1,
        }

    _patch_spark(monkeypatch, fake_spark)

    spool, landing = tmp_path / "spool", tmp_path / "raw"
    _spool_and_seal(spool)
    _land(landing, [1, 2, 3], start_offset=0)
    _land(landing, [7], start_offset=3, business_date=OTHER_DATE)  # different date

    evidence = _run(tmp_path)
    assert evidence["status"] == "published"
    assert evidence["adapter"]["selected_event_count"] == 3
    assert set(evidence["adapter"]["selected_event_ids"]) == set(evidence["edge"]["event_ids"])


def test_assert_exact_session_input_is_pure_and_symmetric():
    s9.assert_exact_session_input(
        sealed_event_ids=["a", "b"], adapter_event_ids=["b", "a"], sealed_count=2
    )
    with pytest.raises(s9.SessionInputMismatchError, match="extra_event_ids"):
        s9.assert_exact_session_input(
            sealed_event_ids=["a"], adapter_event_ids=["a", "b"], sealed_count=1
        )
    with pytest.raises(s9.SessionInputMismatchError, match="missing_event_ids"):
        s9.assert_exact_session_input(
            sealed_event_ids=["a", "b"], adapter_event_ids=["a"], sealed_count=2
        )


# --------------------------------------------------------------------------- #
# 7. Returned and persisted evidence carry the same identity chain
# --------------------------------------------------------------------------- #
def test_returned_and_persisted_evidence_share_the_identity_chain(tmp_path, monkeypatch):
    def fake_spark(**kwargs):
        return {
            "business_date": kwargs["business_date"], "table": "local.db.gold_daily_metrics",
            "run_id": "run-s9-3", "source_hash": kwargs["source_hash"], "status": "published",
            "quality": {"passed": True, "checks": []},
            "row_counts": {"input": 3, "silver": 3, "gold": 1},
            "gold_snapshot_id": 999, "snapshot_count": 1, "target_partition_row_count": 1,
        }

    _patch_spark(monkeypatch, fake_spark)

    spool, landing = tmp_path / "spool", tmp_path / "raw"
    _spool_and_seal(spool)
    _land(landing, [1, 2, 3], start_offset=0)

    evidence = _run(tmp_path)
    persisted = json.loads(Path(evidence["evidence_path"]).read_text(encoding="utf-8"))

    chain = evidence["identity_chain"]
    assert persisted["identity_chain"] == chain
    # Each identity space is recorded in its own field. This asserts that the fields exist and
    # carry the value from the space they name; it is NOT a proof that the spaces can never
    # coincide numerically.
    assert chain["edge_sequence"] == [1, 2, 3]
    assert len(chain["event_id"]) == 3
    assert [c["kafka_offset"] for c in chain["kafka_coordinate"]] == [0, 1, 2]
    assert chain["adapter_source_hash"] == evidence["adapter"]["source_hash"]
    assert chain["spark_attempt_run_id"] == "run-s9-3"
    assert chain["iceberg_snapshot_id"] == 999
    # A published attempt created the snapshot it reports.
    assert chain["snapshot_created_by_current_attempt"] is True
    assert evidence["iceberg"]["snapshot_relation"] == "created_by_current_attempt"
    assert evidence["iceberg"]["producer_attempt_run_id"] == "run-s9-3"
    assert evidence["edge"]["edge_source_id"] == EDGE
    assert evidence["edge"]["boot_session_id"] == SESSION
    assert evidence["edge"]["sequence_range"] == [1, 3]


def test_skipped_attempt_does_not_claim_it_created_the_snapshot(tmp_path, monkeypatch):
    """S7 mints a fresh run_id even when it skips, and never reveals the producer's run_id.

    So a skipped attempt must record the snapshot as reused and the producer as unknown; pairing
    this attempt's id with that snapshot would read as a false `run -> snapshot` causal claim.
    """

    def fake_skipped_spark(**kwargs):
        return {
            "business_date": kwargs["business_date"], "table": "local.db.gold_daily_metrics",
            "run_id": "run-s9-retry-attempt", "source_hash": kwargs["source_hash"],
            "status": "skipped",
            "quality": {"passed": True, "checks": []},
            "row_counts": {"input": 3, "silver": 3, "gold": 1},
            "gold_snapshot_id": 999, "snapshot_count": 1, "target_partition_row_count": 1,
        }

    _patch_spark(monkeypatch, fake_skipped_spark)

    spool, landing = tmp_path / "spool", tmp_path / "raw"
    _spool_and_seal(spool)
    _land(landing, [1, 2, 3], start_offset=0)

    evidence = _run(tmp_path)

    assert evidence["status"] == "skipped"
    assert evidence["spark"]["attempt_run_id"] == "run-s9-retry-attempt"
    assert evidence["iceberg"]["gold_snapshot_id"] == 999
    assert evidence["iceberg"]["snapshot_relation"] == "reused_from_prior_attempt"
    assert evidence["iceberg"]["snapshot_created_by_current_attempt"] is False
    # The producer run_id is unknown, not guessed from the current attempt.
    assert evidence["iceberg"]["producer_attempt_run_id"] is None
    assert evidence["identity_chain"]["snapshot_created_by_current_attempt"] is False
    assert evidence["identity_chain"]["spark_attempt_run_id"] == "run-s9-retry-attempt"
    assert evidence["identity_chain"]["snapshot_relation"] == "reused_from_prior_attempt"


def test_quality_failure_evidence_says_no_snapshot_not_reuse(tmp_path, monkeypatch):
    """A quality failure commits nothing, so there is no snapshot to have created OR reused.

    Classifying it as `reused_from_prior_attempt` asserts the reuse of a snapshot that does not
    exist - a false evidence statement even though the CLI exit code is already correct.
    """

    def fake_quality_failed_spark(**kwargs):
        return {
            "business_date": kwargs["business_date"], "table": "local.db.gold_daily_metrics",
            "run_id": "run-s9-quality-failed", "source_hash": kwargs["source_hash"],
            "status": "quality_failed",
            "quality": {
                "passed": False,
                "checks": [{"name": "numeric_range_within_bounds", "status": "fail"}],
            },
            "row_counts": {"input": 3, "silver": 3, "gold": 1},
            "gold_snapshot_id": None, "snapshot_count": None,
            "target_partition_row_count": None,
        }

    _patch_spark(monkeypatch, fake_quality_failed_spark)

    spool, landing = tmp_path / "spool", tmp_path / "raw"
    _spool_and_seal(spool)
    _land(landing, [1, 2, 3], start_offset=0)

    evidence = _run(tmp_path)

    assert evidence["status"] == "quality_failed"
    assert evidence["spark"]["quality_passed"] is False
    assert evidence["iceberg"]["snapshot_relation"] == "no_snapshot"
    assert evidence["iceberg"]["snapshot_created_by_current_attempt"] is False
    assert evidence["iceberg"]["producer_attempt_run_id"] is None
    assert evidence["iceberg"]["gold_snapshot_id"] is None
    assert evidence["identity_chain"]["snapshot_relation"] == "no_snapshot"
    assert evidence["identity_chain"]["iceberg_snapshot_id"] is None
    assert evidence["identity_chain"]["snapshot_created_by_current_attempt"] is False
    # The attempt itself is still identified; only the snapshot claim is absent.
    assert evidence["spark"]["attempt_run_id"] == "run-s9-quality-failed"

    # Returned and persisted evidence must agree for this status too.
    persisted = json.loads(Path(evidence["evidence_path"]).read_text(encoding="utf-8"))
    assert persisted["iceberg"] == evidence["iceberg"]
    assert persisted["identity_chain"] == evidence["identity_chain"]


def test_build_evidence_maps_every_status_and_rejects_unknown_ones():
    """The status -> snapshot relation mapping is exhaustive, with no silent default."""
    coverage = {
        "edge_source_id": EDGE, "boot_session_id": SESSION, "expected_last_sequence": 1,
        "recovered_sequences": [1], "sealed_event_ids": ["evt-1"], "recovery_complete": True,
        "recovered_coordinates": [
            {"kafka_topic": TOPIC, "kafka_partition": 0, "kafka_offset": 0, "event_id": "evt-1"}
        ],
    }

    class _Adapter:
        status = "created"
        source_hash = "hash-1"
        selected_event_count = 1
        csv_path = Path("/tmp/none.csv")
        provenance = {"selected_event_ids": ["evt-1"]}

    def spark(status, snapshot_id):
        return {
            "business_date": BUSINESS_DATE, "table": "local.db.gold_daily_metrics",
            "run_id": "run-x", "source_hash": "hash-1", "status": status,
            "quality": {"passed": status != "quality_failed", "checks": []},
            "row_counts": {"input": 1, "silver": 1, "gold": 1},
            "gold_snapshot_id": snapshot_id, "snapshot_count": 1 if snapshot_id else None,
            "target_partition_row_count": 1 if snapshot_id else None,
        }

    expected = {
        ("published", 7): ("created_by_current_attempt", True, "run-x"),
        ("skipped", 7): ("reused_from_prior_attempt", False, None),
        ("quality_failed", None): ("no_snapshot", False, None),
    }
    assert set(s9.SNAPSHOT_RELATION_BY_STATUS) == {"published", "skipped", "quality_failed"}

    for (status, snapshot_id), (relation, created, producer) in expected.items():
        evidence = s9.build_evidence(
            coverage=coverage, adapter=_Adapter(), spark=spark(status, snapshot_id)
        )
        iceberg = evidence["iceberg"]
        assert iceberg["snapshot_relation"] == relation, status
        assert iceberg["snapshot_created_by_current_attempt"] is created, status
        assert iceberg["producer_attempt_run_id"] == producer, status
        assert iceberg["gold_snapshot_id"] == snapshot_id, status
        assert evidence["identity_chain"]["snapshot_relation"] == relation, status

    # An unrecognized S7 status must fail loudly instead of defaulting to "reused".
    with pytest.raises(s9.UnexpectedSparkStatusError, match="no S9 evidence statement"):
        s9.build_evidence(
            coverage=coverage, adapter=_Adapter(), spark=spark("committed_unpublished", 7)
        )


# --------------------------------------------------------------------------- #
# 8-9. CLI exit contract
# --------------------------------------------------------------------------- #
def _argv(tmp_path: Path) -> list[str]:
    return [
        "--spool-root", str(tmp_path / "spool"),
        "--edge-source-id", EDGE, "--boot-session-id", SESSION,
        "--landing-dir", str(tmp_path / "raw"),
        "--business-date", BUSINESS_DATE,
        "--adapter-output-dir", str(tmp_path / "adapter"),
        "--warehouse", str(tmp_path / "warehouse"),
        "--output-dir", str(tmp_path / "evidence"),
    ]


def test_cli_exits_nonzero_on_quality_failure(tmp_path, monkeypatch):
    monkeypatch.setattr(
        s9, "run_recovered_telemetry_publish",
        lambda **kwargs: {"status": "quality_failed", "spark": {"run_id": "r"}},
    )
    with pytest.raises(SystemExit) as excinfo:
        s9.main(_argv(tmp_path))
    assert excinfo.value.code == 1


def test_cli_exits_zero_on_skipped_and_published(tmp_path, monkeypatch):
    for status in ("skipped", "published"):
        monkeypatch.setattr(
            s9, "run_recovered_telemetry_publish",
            lambda status=status, **kwargs: {"status": status, "spark": {"run_id": "r"}},
        )
        s9.main(_argv(tmp_path))  # must not raise


def test_cli_exits_nonzero_when_a_gate_blocks(tmp_path, monkeypatch):
    def blocked(**kwargs):
        raise er.RecoveryIncompleteError("recovery incomplete: missing edge sequences [3]")

    monkeypatch.setattr(s9, "run_recovered_telemetry_publish", blocked)
    with pytest.raises(SystemExit) as excinfo:
        s9.main(_argv(tmp_path))
    assert excinfo.value.code == 1


# --------------------------------------------------------------------------- #
# Spark integration (optional)
# --------------------------------------------------------------------------- #
spark_only = pytest.mark.skipif(
    importlib.util.find_spec("pyspark") is None,
    reason="optional Spark/Iceberg dependency not installed; run `pip install -r requirements-spark.txt`",
)


@spark_only
def test_spark_publish_then_retry_creates_no_new_snapshot(tmp_path):
    """The retry is not a whole-pipeline no-op: S7 still runs Spark and quality before skipping.

    What is invariant is the published table state - no new snapshot and no partition overwrite -
    plus the input identity. The attempt id is expected to differ.
    """
    spool, landing = tmp_path / "spool", tmp_path / "raw"
    _spool_and_seal(spool)
    _land(landing, [1, 2, 3], start_offset=0)

    first = _run(tmp_path)
    assert first["status"] == "published"
    assert first["spark"]["quality_passed"] is True
    assert isinstance(first["iceberg"]["gold_snapshot_id"], int)
    assert first["iceberg"]["snapshot_created_by_current_attempt"] is True
    assert first["iceberg"]["producer_attempt_run_id"] == first["spark"]["attempt_run_id"]

    second = _run(tmp_path)
    assert second["status"] == "skipped"
    assert second["iceberg"]["gold_snapshot_id"] == first["iceberg"]["gold_snapshot_id"]
    assert second["adapter"]["source_hash"] == first["adapter"]["source_hash"]
    assert second["iceberg"]["snapshot_count"] == first["iceberg"]["snapshot_count"]
    # A fresh attempt id paired with a snapshot it did not create - recorded, never implied.
    assert second["spark"]["attempt_run_id"] != first["spark"]["attempt_run_id"]
    assert second["iceberg"]["snapshot_relation"] == "reused_from_prior_attempt"
    assert second["iceberg"]["snapshot_created_by_current_attempt"] is False
    assert second["iceberg"]["producer_attempt_run_id"] is None


@spark_only
def test_spark_incomplete_session_creates_no_iceberg_state(tmp_path):
    spool, landing = tmp_path / "spool", tmp_path / "raw"
    _spool_and_seal(spool)
    _land(landing, [1, 2], start_offset=0)

    with pytest.raises(er.RecoveryIncompleteError):
        _run(tmp_path)
    assert _no_downstream_output(tmp_path)
