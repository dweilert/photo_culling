from pathlib import Path

from photo_culling.config_loader import PipelineConfig, load_pipeline_config


def test_load_config_smoke() -> None:
    config_path = Path("config/pipeline.yaml")
    config = load_pipeline_config(config_path)

    assert isinstance(config, PipelineConfig)


def test_load_config_has_expected_top_level_keys() -> None:
    config_path = Path("config/pipeline.yaml")
    config = load_pipeline_config(config_path)

    assert hasattr(config, "tools")
    assert hasattr(config, "files")
    assert hasattr(config, "metadata")
    assert hasattr(config, "derivatives")
    assert hasattr(config, "logging")
