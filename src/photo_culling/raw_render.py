from __future__ import annotations

import subprocess

# ============================================================
# Imports
# ============================================================
from dataclasses import dataclass
from pathlib import Path
from typing import Any

# ============================================================
# Data Models
# ============================================================


@dataclass(frozen=True)
class RenderResult:
    success: bool
    source_raw: Path
    output_jpeg: Path
    tool_used: str | None
    created: bool
    skipped_existing: bool
    return_code: int | None
    stdout: str
    stderr: str
    error: str | None = None


# ============================================================
# Config Helpers
# ============================================================


def _get_nested(config: dict[str, Any], *keys: str, default=None):
    cur: Any = config
    for key in keys:
        if not isinstance(cur, dict) or key not in cur:
            return default
        cur = cur[key]
    return cur


def get_darktable_cli_path(config: dict[str, Any]) -> str:
    """
    Return the configured darktable-cli executable name/path.
    """
    return str(_get_nested(config, "tools", "darktable_cli", default="darktable-cli"))


def should_overwrite_existing(config: dict[str, Any], force: bool) -> bool:
    """
    Force overrides config. Otherwise, use render.overwrite_existing.
    """
    if force:
        return True
    return bool(_get_nested(config, "render", "overwrite_existing", default=False))


# ============================================================
# Command Builder
# ============================================================


def build_darktable_command(
    source_raw: Path,
    output_jpeg: Path,
    config: dict[str, Any],
) -> list[str]:
    """
    Build the darktable-cli command.

    Current version uses the simplest stable invocation:
        darktable-cli input.raw output.jpg

    Additional rendering options can be added later if needed.
    """
    darktable_cli = get_darktable_cli_path(config)
    return [darktable_cli, str(source_raw), str(output_jpeg)]


# ============================================================
# Main Rendering Logic
# ============================================================


def render_raw_to_jpeg(
    source_raw: Path,
    output_jpeg: Path,
    config: dict[str, Any],
    force: bool = False,
) -> RenderResult:
    """
    Render a RAW image to JPEG using darktable-cli.

    Behavior:
    - validates that source_raw exists
    - creates output parent directories
    - skips existing output unless overwrite/force is enabled
    - captures stdout/stderr for debugging
    """
    source_raw = Path(source_raw)
    output_jpeg = Path(output_jpeg)

    if not source_raw.exists():
        return RenderResult(
            success=False,
            source_raw=source_raw,
            output_jpeg=output_jpeg,
            tool_used=None,
            created=False,
            skipped_existing=False,
            return_code=None,
            stdout="",
            stderr="",
            error=f"Source RAW does not exist: {source_raw}",
        )

    if not source_raw.is_file():
        return RenderResult(
            success=False,
            source_raw=source_raw,
            output_jpeg=output_jpeg,
            tool_used=None,
            created=False,
            skipped_existing=False,
            return_code=None,
            stdout="",
            stderr="",
            error=f"Source RAW is not a file: {source_raw}",
        )

    overwrite = should_overwrite_existing(config, force=force)

    if output_jpeg.exists() and not overwrite:
        return RenderResult(
            success=True,
            source_raw=source_raw,
            output_jpeg=output_jpeg,
            tool_used=None,
            created=False,
            skipped_existing=True,
            return_code=None,
            stdout="",
            stderr="",
            error=None,
        )

    output_jpeg.parent.mkdir(parents=True, exist_ok=True)

    cmd = build_darktable_command(source_raw=source_raw, output_jpeg=output_jpeg, config=config)
    tool_used = cmd[0]

    try:
        completed = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            check=False,
        )
    except FileNotFoundError as exc:
        return RenderResult(
            success=False,
            source_raw=source_raw,
            output_jpeg=output_jpeg,
            tool_used=tool_used,
            created=False,
            skipped_existing=False,
            return_code=None,
            stdout="",
            stderr="",
            error=f"Renderer executable not found: {exc}",
        )
    except Exception as exc:
        return RenderResult(
            success=False,
            source_raw=source_raw,
            output_jpeg=output_jpeg,
            tool_used=tool_used,
            created=False,
            skipped_existing=False,
            return_code=None,
            stdout="",
            stderr="",
            error=f"Unexpected render error: {exc}",
        )

    success = completed.returncode == 0 and output_jpeg.exists()

    return RenderResult(
        success=success,
        source_raw=source_raw,
        output_jpeg=output_jpeg,
        tool_used=tool_used,
        created=success,
        skipped_existing=False,
        return_code=completed.returncode,
        stdout=completed.stdout,
        stderr=completed.stderr,
        error=None if success else "darktable-cli render failed",
    )
