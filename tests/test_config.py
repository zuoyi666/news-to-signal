from src.utils.configuration import load_pipeline_config


def test_load_pipeline_config():
    cfg = load_pipeline_config(config_dir="configs")
    assert "data" in cfg
    assert "features" in cfg
    assert "baseline" in cfg
    assert "experiment" in cfg
