from __future__ import annotations

import os
from copy import deepcopy
from pathlib import Path
from typing import Any, Dict

import yaml


def _deep_merge(base: Dict[str, Any], override: Dict[str, Any]) -> Dict[str, Any]:
    merged = deepcopy(base)
    for key, value in override.items():
        if isinstance(value, dict) and isinstance(merged.get(key), dict):
            merged[key] = _deep_merge(merged[key], value)
        else:
            merged[key] = value
    return merged


def load_yaml_config(path: str) -> Dict[str, Any]:
    p = Path(path)
    if not p.exists():
        raise FileNotFoundError(f"Config file not found: {path}")
    with p.open("r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


def _load_config_bundle(config_dir: str) -> Dict[str, Any]:
    cfg: Dict[str, Any] = {}
    for fname in ("data.yaml", "features.yaml", "baseline.yaml", "experiment.yaml"):
        fp = Path(config_dir) / fname
        if not fp.exists():
            continue
        layer = load_yaml_config(str(fp))
        cfg = _deep_merge(cfg, layer)
    return cfg


def load_pipeline_config(
    config_dir: str = "configs",
    overrides: Dict[str, Any] | None = None,
) -> Dict[str, Any]:
    cfg = _load_config_bundle(config_dir)
    cfg = _deep_merge(cfg, overrides or {})

    project_root = Path.cwd().resolve()
    cfg.setdefault("project_root", str(project_root))
    cfg.setdefault("environment", {})
    cfg["environment"]["pwd"] = str(project_root)

    # optional env-variable driven overrides (non-destructive)
    run_synthetic = os.getenv("NEWS_TO_SIGNAL_SYNTHETIC")
    if run_synthetic is not None:
        cfg.setdefault("experiment", {})["synthetic"] = run_synthetic.lower() in {"1", "true", "yes"}

    return cfg
