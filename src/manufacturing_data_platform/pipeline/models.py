from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class PipelinePaths:
    raw_path: Path
    bronze_path: Path
    silver_path: Path
    gold_path: Path
    quality_path: Path
    manifest_path: Path


@dataclass(frozen=True)
class PipelineResult:
    run_id: str
    dataset_id: str
    business_date: str
    source_hash: str
    schema_hash: str
    paths: PipelinePaths
    quality_passed: bool
    quality_checks: list[dict]
    catalog_backend: str
    status: str = "processed"  # "processed" | "skipped" (idempotency reuse)

