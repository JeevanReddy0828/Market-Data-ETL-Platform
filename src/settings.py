from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml


@dataclass(frozen=True)
class StorageSettings:
    raw_dir: Path
    staged_dir: Path


@dataclass(frozen=True)
class WarehouseSettings:
    url: str


@dataclass(frozen=True)
class PipelineSettings:
    symbols: list[str]
    start_date: str
    end_date: str


@dataclass(frozen=True)
class Settings:
    pipeline: PipelineSettings
    storage: StorageSettings
    warehouse: WarehouseSettings


def load_settings(config_path: str = "config/pipeline.yml") -> Settings:
    with open(config_path, "r", encoding="utf-8") as f:
        cfg: dict[str, Any] = yaml.safe_load(f)

    pipeline_cfg = cfg["pipeline"]
    storage_cfg = cfg["storage"]
    warehouse_cfg = cfg["warehouse"]

    raw_dir = Path(os.getenv("RAW_DIR", storage_cfg["raw_dir"]))
    staged_dir = Path(os.getenv("STAGED_DIR", storage_cfg["staged_dir"]))
    warehouse_url = os.getenv("WAREHOUSE_URL", warehouse_cfg["url"])

    return Settings(
        pipeline=PipelineSettings(
            symbols=list(pipeline_cfg["symbols"]),
            start_date=str(pipeline_cfg["start_date"]),
            end_date=str(pipeline_cfg["end_date"]),
        ),
        storage=StorageSettings(raw_dir=raw_dir, staged_dir=staged_dir),
        warehouse=WarehouseSettings(url=warehouse_url),
    )
