from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import pytest
from fastapi.testclient import TestClient

from app import app, APP_VERSION, WORKER_NAME

client = TestClient(app)


# ============================================================
# Helpers
# ============================================================


def _make_file(path: Path, content: bytes = b"raw-data") -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(content)


def _render_payload(
    source_raw: str,
    output_jpeg: str,
    overwrite: bool = False,
    job_id: str = "job-001",
    jpeg_quality: int | None = None,
) -> dict:
    payload: dict = {
        "job_id": job_id,
        "source_raw": source_raw,
        "output_jpeg": output_jpeg,
        "overwrite": overwrite,
    }
    if jpeg_quality is not None:
        payload["jpeg_quality"] = jpeg_quality
    return payload


# ============================================================
# /health
# ============================================================


def test_health_returns_200() -> None:
    response = client.get("/health")
    assert response.status_code == 200


def test_health_ok_is_true() -> None:
    response = client.get("/health")
    assert response.json()["ok"] is True


def test_health_contains_expected_fields() -> None:
    data = client.get("/health").json()
    assert data["service"] == "photo-culling-render-worker"
    assert data["worker_name"] == WORKER_NAME
    assert data["version"] == APP_VERSION
    assert data["renderer"] is not None
    assert data["status"] == "ready"


# ============================================================
# /render — input validation
# ============================================================


def test_render_source_not_found(tmp_path: Path) -> None:
    source_raw = tmp_path / "missing.ARW"
    output_jpeg = tmp_path / "out" / "missing.jpg"

    response = client.post("/render", json=_render_payload(str(source_raw), str(output_jpeg)))
    data = response.json()

    assert response.status_code == 200
    assert data["success"] is False
    assert data["created"] is False
    assert data["skipped_existing"] is False
    assert data["error"]["code"] == "SOURCE_NOT_FOUND"
    assert data["error"]["retryable"] is False


def test_render_source_is_directory(tmp_path: Path) -> None:
    source_dir = tmp_path / "not_a_file"
    source_dir.mkdir()
    output_jpeg = tmp_path / "out.jpg"

    response = client.post("/render", json=_render_payload(str(source_dir), str(output_jpeg)))
    data = response.json()

    assert data["success"] is False
    assert data["error"]["code"] == "SOURCE_NOT_FILE"
    assert data["error"]["retryable"] is False


# ============================================================
# /render — skip existing
# ============================================================


def test_render_skips_existing_when_overwrite_false(tmp_path: Path) -> None:
    source_raw = tmp_path / "IMG_0001.ARW"
    output_jpeg = tmp_path / "out" / "IMG_0001.jpg"

    _make_file(source_raw)
    _make_file(output_jpeg)

    response = client.post(
        "/render",
        json=_render_payload(str(source_raw), str(output_jpeg), overwrite=False),
    )
    data = response.json()

    assert data["success"] is True
    assert data["skipped_existing"] is True
    assert data["created"] is False
    assert data["error"] is None


def test_render_does_not_skip_when_overwrite_true(tmp_path: Path) -> None:
    source_raw = tmp_path / "IMG_0002.ARW"
    output_jpeg = tmp_path / "out" / "IMG_0002.jpg"

    _make_file(source_raw)
    _make_file(output_jpeg, content=b"old")

    def fake_run(cmd, capture_output, text):
        output_jpeg.write_bytes(b"new")
        return SimpleNamespace(returncode=0, stdout="ok", stderr="")

    with patch("app.subprocess.run", side_effect=fake_run):
        response = client.post(
            "/render",
            json=_render_payload(str(source_raw), str(output_jpeg), overwrite=True),
        )

    data = response.json()
    assert data["success"] is True
    assert data["created"] is True
    assert data["skipped_existing"] is False


# ============================================================
# /render — output directory creation failure
# ============================================================


def test_render_output_parent_create_failed(tmp_path: Path) -> None:
    source_raw = tmp_path / "IMG_0003.ARW"
    output_jpeg = tmp_path / "out" / "IMG_0003.jpg"

    _make_file(source_raw)

    with patch("app.Path.mkdir", side_effect=PermissionError("no permission")):
        response = client.post("/render", json=_render_payload(str(source_raw), str(output_jpeg)))

    data = response.json()
    assert data["success"] is False
    assert data["error"]["code"] == "OUTPUT_PARENT_CREATE_FAILED"
    assert data["error"]["retryable"] is False


# ============================================================
# /render — subprocess failures
# ============================================================


def test_render_renderer_not_found(tmp_path: Path) -> None:
    source_raw = tmp_path / "IMG_0004.ARW"
    output_jpeg = tmp_path / "out" / "IMG_0004.jpg"

    _make_file(source_raw)

    with patch("app.subprocess.run", side_effect=FileNotFoundError("no such file")):
        response = client.post("/render", json=_render_payload(str(source_raw), str(output_jpeg)))

    data = response.json()
    assert data["success"] is False
    assert data["error"]["code"] == "RENDERER_NOT_FOUND"
    assert data["error"]["retryable"] is False


def test_render_internal_subprocess_error(tmp_path: Path) -> None:
    source_raw = tmp_path / "IMG_0005.ARW"
    output_jpeg = tmp_path / "out" / "IMG_0005.jpg"

    _make_file(source_raw)

    with patch("app.subprocess.run", side_effect=RuntimeError("unexpected boom")):
        response = client.post("/render", json=_render_payload(str(source_raw), str(output_jpeg)))

    data = response.json()
    assert data["success"] is False
    assert data["error"]["code"] == "INTERNAL_ERROR"


def test_render_nonzero_return_code(tmp_path: Path) -> None:
    source_raw = tmp_path / "IMG_0006.ARW"
    output_jpeg = tmp_path / "out" / "IMG_0006.jpg"

    _make_file(source_raw)

    def fake_run(cmd, capture_output, text):
        return SimpleNamespace(returncode=1, stdout="", stderr="darktable error")

    with patch("app.subprocess.run", side_effect=fake_run):
        response = client.post("/render", json=_render_payload(str(source_raw), str(output_jpeg)))

    data = response.json()
    assert data["success"] is False
    assert data["error"]["code"] == "RENDER_FAILED"
    assert data["return_code"] == 1
    assert data["stderr"] == "darktable error"


def test_render_output_not_created_after_zero_exit(tmp_path: Path) -> None:
    source_raw = tmp_path / "IMG_0007.ARW"
    output_jpeg = tmp_path / "out" / "IMG_0007.jpg"

    _make_file(source_raw)

    # subprocess reports success but does NOT write the output file
    def fake_run(cmd, capture_output, text):
        return SimpleNamespace(returncode=0, stdout="done", stderr="")

    with patch("app.subprocess.run", side_effect=fake_run):
        response = client.post("/render", json=_render_payload(str(source_raw), str(output_jpeg)))

    data = response.json()
    assert data["success"] is False
    assert data["created"] is False
    assert data["error"]["code"] == "OUTPUT_NOT_CREATED"


# ============================================================
# /render — happy path
# ============================================================


def test_render_success(tmp_path: Path) -> None:
    source_raw = tmp_path / "IMG_0008.ARW"
    output_jpeg = tmp_path / "out" / "IMG_0008.jpg"

    _make_file(source_raw)

    def fake_run(cmd, capture_output, text):
        output_jpeg.parent.mkdir(parents=True, exist_ok=True)
        output_jpeg.write_bytes(b"jpeg-data")
        return SimpleNamespace(returncode=0, stdout="rendered", stderr="")

    with patch("app.subprocess.run", side_effect=fake_run):
        response = client.post("/render", json=_render_payload(str(source_raw), str(output_jpeg)))

    data = response.json()
    assert data["success"] is True
    assert data["created"] is True
    assert data["skipped_existing"] is False
    assert data["return_code"] == 0
    assert data["stdout"] == "rendered"
    assert data["error"] is None
    assert data["job_id"] == "job-001"
    assert data["worker_name"] == WORKER_NAME


def test_render_success_propagates_job_id(tmp_path: Path) -> None:
    source_raw = tmp_path / "IMG_0009.ARW"
    output_jpeg = tmp_path / "out" / "IMG_0009.jpg"

    _make_file(source_raw)

    def fake_run(cmd, capture_output, text):
        output_jpeg.parent.mkdir(parents=True, exist_ok=True)
        output_jpeg.write_bytes(b"jpeg-data")
        return SimpleNamespace(returncode=0, stdout="", stderr="")

    with patch("app.subprocess.run", side_effect=fake_run):
        response = client.post(
            "/render",
            json=_render_payload(str(source_raw), str(output_jpeg), job_id="my-unique-job"),
        )

    assert response.json()["job_id"] == "my-unique-job"
