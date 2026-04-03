from datetime import datetime
import re
from pathlib import Path


def slugify(name: str) -> str:
    if not name:
        return "baseline"

    normalized = re.sub(r"[^a-zA-Z0-9_.-]+", "-", name.strip().lower())
    normalized = normalized.strip("-_.")
    return normalized or "baseline"


def make_run_name(experiment_name: str, suffix: str = "") -> str:
    ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    slug = slugify(experiment_name)
    if suffix:
        return f"exp_{ts}_{slug}_{slugify(suffix)}"
    return f"exp_{ts}_{slug}"


def make_run_dir(output_root: str, run_name: str) -> Path:
    root = Path(output_root).expanduser()
    run_path = root / run_name
    (run_path / "figures").mkdir(parents=True, exist_ok=True)
    (run_path / "tables").mkdir(parents=True, exist_ok=True)
    (run_path / "reports").mkdir(parents=True, exist_ok=True)
    return run_path
