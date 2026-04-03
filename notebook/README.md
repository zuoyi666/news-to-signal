# Legacy notebook folder

`notebook/` is kept for backward compatibility with the original Claude Code project layout.

For the standardized workflow, use:

- `notebooks/` for analysis notebooks
- `artifacts/runs/exp_YYYYMMDD_name/` for all persisted outputs

When saving outputs from notebooks, use the helper convention:

```python
import os
from pathlib import Path

artifact_root = Path(
    os.getenv("NEWS_TO_SIGNAL_ARTIFACT_RUN_DIR", "artifacts/runs")
)
run_dirs = sorted(artifact_root.glob("exp_*"))
run_dir = run_dirs[-1] if run_dirs else artifact_root

fig_dir = run_dir / "figures"
table_dir = run_dir / "tables"
report_dir = run_dir / "reports"
fig_dir.mkdir(parents=True, exist_ok=True)
table_dir.mkdir(parents=True, exist_ok=True)
report_dir.mkdir(parents=True, exist_ok=True)
```

Then save files under `run_dir` instead of `results/`:

- `.../figures`
- `.../tables`
- `.../reports`
