from __future__ import annotations

from pathlib import Path
import os
import subprocess
import time

from fastapi import FastAPI
from pydantic import BaseModel


APP_VERSION = "0.1.0"
WORKER_NAME = os.getenv("WORKER_NAME", "render-worker")
RENDER_COMMAND = os.getenv("RENDER_COMMAND", "darktable-cli")


app = FastAPI(title="photo-culling-render-worker", version=APP_VERSION)


class RenderRequest(BaseModel):
    job_id: str
    source_raw: str
    output_jpeg: str
    overwrite: bool
    jpeg_quality: int | None = None


class ErrorPayload(BaseModel):
    code: str
    message: str
    retryable: bool = False


class RenderResponse(BaseModel):
    success: bool
    job_id: str
    worker_name: str
    tool_used: str
    source_raw: str
    output_jpeg: str
    created: bool
    skipped_existing: bool
    return_code: int | None = None
    stdout: str = ""
    stderr: str = ""
    error: ErrorPayload | None = None


@app.get("/health")
def health() -> dict:
    return {
        "ok": True,
        "service": "photo-culling-render-worker",
        "worker_name": WORKER_NAME,
        "version": APP_VERSION,
        "renderer": RENDER_COMMAND,
        "status": "ready",
    }


@app.post("/render", response_model=RenderResponse)
def render(req: RenderRequest) -> RenderResponse:
    source_raw = Path(req.source_raw)
    output_jpeg = Path(req.output_jpeg)

    if not source_raw.exists():
        return RenderResponse(
            success=False,
            job_id=req.job_id,
            worker_name=WORKER_NAME,
            tool_used=RENDER_COMMAND,
            source_raw=str(source_raw),
            output_jpeg=str(output_jpeg),
            created=False,
            skipped_existing=False,
            error=ErrorPayload(
                code="SOURCE_NOT_FOUND",
                message=f"Source RAW does not exist: {source_raw}",
                retryable=False,
            ),
        )

    if not source_raw.is_file():
        return RenderResponse(
            success=False,
            job_id=req.job_id,
            worker_name=WORKER_NAME,
            tool_used=RENDER_COMMAND,
            source_raw=str(source_raw),
            output_jpeg=str(output_jpeg),
            created=False,
            skipped_existing=False,
            error=ErrorPayload(
                code="SOURCE_NOT_FILE",
                message=f"Source RAW is not a file: {source_raw}",
                retryable=False,
            ),
        )

    if output_jpeg.exists() and not req.overwrite:
        return RenderResponse(
            success=True,
            job_id=req.job_id,
            worker_name=WORKER_NAME,
            tool_used=RENDER_COMMAND,
            source_raw=str(source_raw),
            output_jpeg=str(output_jpeg),
            created=False,
            skipped_existing=True,
            return_code=0,
            stdout="",
            stderr="",
        )

    try:
        output_jpeg.parent.mkdir(parents=True, exist_ok=True)
    except Exception as exc:
        return RenderResponse(
            success=False,
            job_id=req.job_id,
            worker_name=WORKER_NAME,
            tool_used=RENDER_COMMAND,
            source_raw=str(source_raw),
            output_jpeg=str(output_jpeg),
            created=False,
            skipped_existing=False,
            error=ErrorPayload(
                code="OUTPUT_PARENT_CREATE_FAILED",
                message=str(exc),
                retryable=False,
            ),
        )

    try:
        result = subprocess.run(
            [RENDER_COMMAND, str(source_raw), str(output_jpeg)],
            capture_output=True,
            text=True,
        )
    except FileNotFoundError:
        return RenderResponse(
            success=False,
            job_id=req.job_id,
            worker_name=WORKER_NAME,
            tool_used=RENDER_COMMAND,
            source_raw=str(source_raw),
            output_jpeg=str(output_jpeg),
            created=False,
            skipped_existing=False,
            error=ErrorPayload(
                code="RENDERER_NOT_FOUND",
                message=f"Renderer not found: {RENDER_COMMAND}",
                retryable=False,
            ),
        )
    except Exception as exc:
        return RenderResponse(
            success=False,
            job_id=req.job_id,
            worker_name=WORKER_NAME,
            tool_used=RENDER_COMMAND,
            source_raw=str(source_raw),
            output_jpeg=str(output_jpeg),
            created=False,
            skipped_existing=False,
            error=ErrorPayload(
                code="INTERNAL_ERROR",
                message=str(exc),
                retryable=False,
            ),
        )

    if result.returncode != 0:
        return RenderResponse(
            success=False,
            job_id=req.job_id,
            worker_name=WORKER_NAME,
            tool_used=RENDER_COMMAND,
            source_raw=str(source_raw),
            output_jpeg=str(output_jpeg),
            created=False,
            skipped_existing=False,
            return_code=result.returncode,
            stdout=result.stdout,
            stderr=result.stderr,
            error=ErrorPayload(
                code="RENDER_FAILED",
                message="darktable-cli returned non-zero exit status",
                retryable=False,
            ),
        )

    if not output_jpeg.exists():
        return RenderResponse(
            success=False,
            job_id=req.job_id,
            worker_name=WORKER_NAME,
            tool_used=RENDER_COMMAND,
            source_raw=str(source_raw),
            output_jpeg=str(output_jpeg),
            created=False,
            skipped_existing=False,
            return_code=result.returncode,
            stdout=result.stdout,
            stderr=result.stderr,
            error=ErrorPayload(
                code="OUTPUT_NOT_CREATED",
                message=f"Render reported success but output missing: {output_jpeg}",
                retryable=False,
            ),
        )

    return RenderResponse(
        success=True,
        job_id=req.job_id,
        worker_name=WORKER_NAME,
        tool_used=RENDER_COMMAND,
        source_raw=str(source_raw),
        output_jpeg=str(output_jpeg),
        created=True,
        skipped_existing=False,
        return_code=result.returncode,
        stdout=result.stdout,
        stderr=result.stderr,
    )