from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

from photo_culling.raw_render import (
    build_darktable_command,
    render_raw_to_jpeg,
)

# ============================================================
# Helpers
# ============================================================


def _make_file(path: Path, content: bytes = b"test") -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(content)


def _config() -> dict:
    return {
        "tools": {
            "darktable_cli": "darktable-cli",
        },
        "render": {
            "overwrite_existing": False,
        },
    }


# ============================================================
# Tests
# ============================================================


def test_build_darktable_command() -> None:
    config = _config()

    cmd = build_darktable_command(
        source_raw=Path("/photos/IMG_0001.ARW"),
        output_jpeg=Path("/derivatives/IMG_0001.jpg"),
        config=config,
    )

    assert cmd == [
        "darktable-cli",
        "/photos/IMG_0001.ARW",
        "/derivatives/IMG_0001.jpg",
    ]


def test_render_missing_source_file(tmp_path: Path) -> None:
    config = _config()

    source_raw = tmp_path / "missing.ARW"
    output_jpeg = tmp_path / "out" / "missing.jpg"

    result = render_raw_to_jpeg(
        source_raw=source_raw,
        output_jpeg=output_jpeg,
        config=config,
    )

    assert result.success is False
    assert result.error is not None
    assert "does not exist" in result.error


def test_render_skips_existing_output(tmp_path: Path) -> None:
    config = _config()

    source_raw = tmp_path / "photos" / "IMG_0001.ARW"
    output_jpeg = tmp_path / "derivatives" / "IMG_0001.jpg"

    _make_file(source_raw)
    _make_file(output_jpeg)

    result = render_raw_to_jpeg(
        source_raw=source_raw,
        output_jpeg=output_jpeg,
        config=config,
    )

    assert result.success is True
    assert result.skipped_existing is True
    assert result.created is False


def test_render_success_with_mocked_subprocess(tmp_path: Path, monkeypatch) -> None:
    config = _config()

    source_raw = tmp_path / "photos" / "IMG_0001.ARW"
    output_jpeg = tmp_path / "derivatives" / "IMG_0001.jpg"

    _make_file(source_raw)

    def fake_run(cmd, capture_output, text, check):
        output_jpeg.parent.mkdir(parents=True, exist_ok=True)
        output_jpeg.write_bytes(b"jpeg-data")
        return SimpleNamespace(returncode=0, stdout="ok", stderr="")

    monkeypatch.setattr("photo_culling.raw_render.subprocess.run", fake_run)

    result = render_raw_to_jpeg(
        source_raw=source_raw,
        output_jpeg=output_jpeg,
        config=config,
    )

    assert result.success is True
    assert result.created is True
    assert result.skipped_existing is False
    assert result.return_code == 0
    assert result.stdout == "ok"
    assert output_jpeg.exists()


def test_render_failure_with_mocked_subprocess(tmp_path: Path, monkeypatch) -> None:
    config = _config()

    source_raw = tmp_path / "photos" / "IMG_0002.ARW"
    output_jpeg = tmp_path / "derivatives" / "IMG_0002.jpg"

    _make_file(source_raw)

    def fake_run(cmd, capture_output, text, check):
        return SimpleNamespace(returncode=1, stdout="", stderr="render failed")

    monkeypatch.setattr("photo_culling.raw_render.subprocess.run", fake_run)

    result = render_raw_to_jpeg(
        source_raw=source_raw,
        output_jpeg=output_jpeg,
        config=config,
    )

    assert result.success is False
    assert result.created is False
    assert result.skipped_existing is False
    assert result.return_code == 1
    assert result.stderr == "render failed"
    assert result.error == "darktable-cli render failed"


def test_render_force_overwrites_existing_output(tmp_path: Path, monkeypatch) -> None:
    config = _config()

    source_raw = tmp_path / "photos" / "IMG_0003.ARW"
    output_jpeg = tmp_path / "derivatives" / "IMG_0003.jpg"

    _make_file(source_raw)
    _make_file(output_jpeg, content=b"old-jpeg")

    def fake_run(cmd, capture_output, text, check):
        output_jpeg.write_bytes(b"new-jpeg")
        return SimpleNamespace(returncode=0, stdout="forced", stderr="")

    monkeypatch.setattr("photo_culling.raw_render.subprocess.run", fake_run)

    result = render_raw_to_jpeg(
        source_raw=source_raw,
        output_jpeg=output_jpeg,
        config=config,
        force=True,
    )

    assert result.success is True
    assert result.created is True
    assert result.skipped_existing is False
    assert result.stdout == "forced"
    assert output_jpeg.read_bytes() == b"new-jpeg"
