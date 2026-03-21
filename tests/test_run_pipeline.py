from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

import pytest

import photo_culling.run_pipeline as run_pipeline
from photo_culling.pairing import PairedImage


# ============================================================
# Helpers
# ============================================================


def _make_pair(group_key: str, status: str = "paired") -> PairedImage:
    return PairedImage(
        group_key=group_key,
        rel_dir=Path("album1"),
        base_name="img001",
        status=status,
        raw_path=Path("/photos/album1/img001.cr2"),
        jpeg_path=Path("/photos/album1/img001.jpg"),
        raw_count=1,
        jpeg_count=1,
        notes=(),
    )


def _make_config(db_path: str | None = "/tmp/test.db"):
    paths = SimpleNamespace(
        source_root="/photos",
        derivative_root="/derivatives",
        db_path=db_path,
    )
    return SimpleNamespace(paths=paths)


def _make_summary(total=2, succeeded=2, failed=0, skipped=0):
    return SimpleNamespace(
        total=total,
        succeeded=succeeded,
        failed=failed,
        skipped=skipped,
    )


# ============================================================
# main()
# ============================================================


def test_main_calls_discover_and_pair(monkeypatch: pytest.MonkeyPatch) -> None:
    discover_calls = []
    pair_calls = []

    monkeypatch.setattr(
        run_pipeline, "load_pipeline_config", lambda _: _make_config()
    )
    monkeypatch.setattr(
        run_pipeline,
        "discover_assets",
        lambda root, config: discover_calls.append(root) or [],
    )
    monkeypatch.setattr(
        run_pipeline,
        "pair_assets",
        lambda assets: pair_calls.append(assets) or [],
    )
    monkeypatch.setattr(
        run_pipeline, "process_all_pairs", lambda pairs, **kw: _make_summary(total=0)
    )

    run_pipeline.main()

    assert len(discover_calls) == 1
    assert len(pair_calls) == 1


def test_main_passes_db_path_to_process(monkeypatch: pytest.MonkeyPatch) -> None:
    captured_kwargs = {}

    monkeypatch.setattr(
        run_pipeline, "load_pipeline_config", lambda _: _make_config(db_path="/my/db.db")
    )
    monkeypatch.setattr(run_pipeline, "discover_assets", lambda root, config: [])
    monkeypatch.setattr(run_pipeline, "pair_assets", lambda assets: [])

    def fake_process(pairs, **kwargs):
        captured_kwargs.update(kwargs)
        return _make_summary(total=0)

    monkeypatch.setattr(run_pipeline, "process_all_pairs", fake_process)

    run_pipeline.main()

    assert captured_kwargs.get("db_path") == Path("/my/db.db")


def test_main_passes_none_db_path_when_not_configured(monkeypatch: pytest.MonkeyPatch) -> None:
    captured_kwargs = {}

    monkeypatch.setattr(
        run_pipeline, "load_pipeline_config", lambda _: _make_config(db_path=None)
    )
    monkeypatch.setattr(run_pipeline, "discover_assets", lambda root, config: [])
    monkeypatch.setattr(run_pipeline, "pair_assets", lambda assets: [])

    def fake_process(pairs, **kwargs):
        captured_kwargs.update(kwargs)
        return _make_summary(total=0)

    monkeypatch.setattr(run_pipeline, "process_all_pairs", fake_process)

    run_pipeline.main()

    assert captured_kwargs.get("db_path") is None


def test_main_prints_summary(monkeypatch: pytest.MonkeyPatch, capsys) -> None:
    monkeypatch.setattr(
        run_pipeline, "load_pipeline_config", lambda _: _make_config()
    )
    monkeypatch.setattr(run_pipeline, "discover_assets", lambda root, config: [])
    monkeypatch.setattr(run_pipeline, "pair_assets", lambda assets: [])
    monkeypatch.setattr(
        run_pipeline,
        "process_all_pairs",
        lambda pairs, **kw: _make_summary(total=5, succeeded=3, failed=1, skipped=1),
    )

    run_pipeline.main()

    captured = capsys.readouterr()
    assert "5" in captured.out
    assert "3" in captured.out
    assert "1" in captured.out


def test_main_prints_config_paths(monkeypatch: pytest.MonkeyPatch, capsys) -> None:
    monkeypatch.setattr(
        run_pipeline,
        "load_pipeline_config",
        lambda _: _make_config(db_path="/my/special.db"),
    )
    monkeypatch.setattr(run_pipeline, "discover_assets", lambda root, config: [])
    monkeypatch.setattr(run_pipeline, "pair_assets", lambda assets: [])
    monkeypatch.setattr(
        run_pipeline, "process_all_pairs", lambda pairs, **kw: _make_summary(total=0)
    )

    run_pipeline.main()

    captured = capsys.readouterr()
    assert "/photos" in captured.out
    assert "/derivatives" in captured.out


def test_main_passes_source_and_derivative_roots(monkeypatch: pytest.MonkeyPatch) -> None:
    captured_kwargs = {}

    monkeypatch.setattr(
        run_pipeline, "load_pipeline_config", lambda _: _make_config()
    )
    monkeypatch.setattr(run_pipeline, "discover_assets", lambda root, config: [])
    monkeypatch.setattr(run_pipeline, "pair_assets", lambda assets: [])

    def fake_process(pairs, **kwargs):
        captured_kwargs.update(kwargs)
        return _make_summary(total=0)

    monkeypatch.setattr(run_pipeline, "process_all_pairs", fake_process)

    run_pipeline.main()

    assert captured_kwargs["source_root"] == Path("/photos")
    assert captured_kwargs["derivative_root"] == Path("/derivatives")
