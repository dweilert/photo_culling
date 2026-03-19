from __future__ import annotations

from pathlib import Path

import pytest

import photo_culling.batch as batch
from photo_culling.analysis_pipeline import AnalysisResult
from photo_culling.batch import (
    STATUS_FAILED,
    STATUS_SKIPPED,
    STATUS_STARTED,
    STATUS_SUCCESS,
    ProgressEvent,
)
from photo_culling.pairing import PairedImage


def make_pair(
    *,
    group_key: str,
    status: str,
    raw: str | None,
    jpeg: str | None,
    rel_dir: str = "photos/a",
    base_name: str = "image",
    raw_count: int = 1,
    jpeg_count: int = 1,
) -> PairedImage:
    return PairedImage(
        group_key=group_key,
        rel_dir=Path(rel_dir),
        base_name=base_name,
        status=status,
        raw_path=Path(raw) if raw is not None else None,
        jpeg_path=Path(jpeg) if jpeg is not None else None,
        raw_count=raw_count,
        jpeg_count=jpeg_count,
        notes=(),
    )


def batch_kwargs() -> dict:
    return {
        "source_root": Path("/photos"),
        "derivative_root": Path("/derived"),
        "config": {},
    }


def test_make_pair_id_uses_group_key() -> None:
    pair = make_pair(
        group_key="photos/a/image1",
        status="paired",
        raw="/photos/a/image1.cr2",
        jpeg="/photos/a/image1.jpg",
        base_name="image1",
    )
    assert batch.make_pair_id(pair) == "photos/a/image1"


def test_process_all_pairs_empty_input() -> None:
    summary = batch.process_all_pairs([], **batch_kwargs())

    assert summary.total == 0
    assert summary.succeeded == 0
    assert summary.failed == 0
    assert summary.skipped == 0
    assert summary.results == []


def test_process_all_pairs_all_success(monkeypatch: pytest.MonkeyPatch) -> None:
    pairs = [
        make_pair(
            group_key="photos/a/one",
            status="paired",
            raw="/photos/a/one.cr2",
            jpeg="/photos/a/one.jpg",
            base_name="one",
        ),
        make_pair(
            group_key="photos/a/two",
            status="raw_only",
            raw="/photos/a/two.cr2",
            jpeg=None,
            base_name="two",
            jpeg_count=0,
        ),
    ]

    def fake_process_pair(pair: PairedImage, **kwargs: object) -> AnalysisResult:
        return AnalysisResult(
            pair=pair,
            final_image=Path(f"/derived/{pair.base_name}.jpg"),
            used_existing=False,
            render_result=None,
            metadata_result=None,
            error=None,
        )

    monkeypatch.setattr(batch, "process_pair", fake_process_pair)

    summary = batch.process_all_pairs(pairs, **batch_kwargs())

    assert summary.total == 2
    assert summary.succeeded == 2
    assert summary.failed == 0
    assert summary.skipped == 0
    assert len(summary.results) == 2
    assert all(item.status == STATUS_SUCCESS for item in summary.results)


def test_process_all_pairs_skips_ambiguous(monkeypatch: pytest.MonkeyPatch) -> None:
    pair = make_pair(
        group_key="photos/a/ambig",
        status="ambiguous",
        raw="/photos/a/ambig.cr2",
        jpeg="/photos/a/ambig.jpg",
        base_name="ambig",
    )

    called = False

    def fake_process_pair(pair: PairedImage, **kwargs: object) -> AnalysisResult:
        nonlocal called
        called = True
        return AnalysisResult(
            pair=pair,
            final_image=None,
            used_existing=False,
            render_result=None,
            metadata_result=None,
            error=None,
        )

    monkeypatch.setattr(batch, "process_pair", fake_process_pair)

    summary = batch.process_all_pairs([pair], **batch_kwargs())

    assert called is False
    assert summary.total == 1
    assert summary.succeeded == 0
    assert summary.failed == 0
    assert summary.skipped == 1
    assert summary.results[0].status == STATUS_SKIPPED


def test_process_all_pairs_marks_analysis_error_as_failed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    pair = make_pair(
        group_key="photos/a/bad",
        status="paired",
        raw="/photos/a/bad.cr2",
        jpeg="/photos/a/bad.jpg",
        base_name="bad",
    )

    def fake_process_pair(pair: PairedImage, **kwargs: object) -> AnalysisResult:
        return AnalysisResult(
            pair=pair,
            final_image=None,
            used_existing=False,
            render_result=None,
            metadata_result=None,
            error="render failed",
        )

    monkeypatch.setattr(batch, "process_pair", fake_process_pair)

    summary = batch.process_all_pairs([pair], **batch_kwargs())

    assert summary.total == 1
    assert summary.succeeded == 0
    assert summary.failed == 1
    assert summary.skipped == 0
    assert summary.results[0].status == STATUS_FAILED
    assert summary.results[0].error_message == "render failed"


def test_process_all_pairs_continues_after_exception(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    pairs = [
        make_pair(
            group_key="photos/a/one",
            status="paired",
            raw="/photos/a/one.cr2",
            jpeg="/photos/a/one.jpg",
            base_name="one",
        ),
        make_pair(
            group_key="photos/a/two",
            status="paired",
            raw="/photos/a/two.cr2",
            jpeg="/photos/a/two.jpg",
            base_name="two",
        ),
        make_pair(
            group_key="photos/a/three",
            status="jpeg_only",
            raw=None,
            jpeg="/photos/a/three.jpg",
            base_name="three",
            raw_count=0,
        ),
    ]

    def fake_process_pair(pair: PairedImage, **kwargs: object) -> AnalysisResult:
        if pair.base_name == "two":
            raise RuntimeError("simulated failure")

        return AnalysisResult(
            pair=pair,
            final_image=Path(f"/derived/{pair.base_name}.jpg"),
            used_existing=(pair.status == "jpeg_only"),
            render_result=None,
            metadata_result=None,
            error=None,
        )

    monkeypatch.setattr(batch, "process_pair", fake_process_pair)

    summary = batch.process_all_pairs(pairs, **batch_kwargs())

    assert summary.total == 3
    assert summary.succeeded == 2
    assert summary.failed == 1
    assert summary.skipped == 0

    assert summary.results[0].status == STATUS_SUCCESS
    assert summary.results[1].status == STATUS_FAILED
    assert summary.results[2].status == STATUS_SUCCESS
    assert "simulated failure" in (summary.results[1].error_message or "")


def test_process_all_pairs_emits_progress_events(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    pairs = [
        make_pair(
            group_key="photos/a/one",
            status="paired",
            raw="/photos/a/one.cr2",
            jpeg="/photos/a/one.jpg",
            base_name="one",
        ),
        make_pair(
            group_key="photos/a/two",
            status="ambiguous",
            raw="/photos/a/two.cr2",
            jpeg="/photos/a/two.jpg",
            base_name="two",
        ),
    ]
    events: list[ProgressEvent] = []

    def fake_process_pair(pair: PairedImage, **kwargs: object) -> AnalysisResult:
        return AnalysisResult(
            pair=pair,
            final_image=Path(f"/derived/{pair.base_name}.jpg"),
            used_existing=False,
            render_result=None,
            metadata_result=None,
            error=None,
        )

    def capture(event: ProgressEvent) -> None:
        events.append(event)

    monkeypatch.setattr(batch, "process_pair", fake_process_pair)

    summary = batch.process_all_pairs(
        pairs,
        progress=capture,
        **batch_kwargs(),
    )

    assert summary.total == 2
    assert summary.succeeded == 1
    assert summary.skipped == 1

    assert events[0].status == STATUS_STARTED
    assert events[1].status == STATUS_SUCCESS
    assert events[2].status == STATUS_SKIPPED


def test_process_all_pairs_emits_failed_progress_event(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    pair = make_pair(
        group_key="photos/a/bad",
        status="paired",
        raw="/photos/a/bad.cr2",
        jpeg="/photos/a/bad.jpg",
        base_name="bad",
    )
    events: list[ProgressEvent] = []

    def fake_process_pair(pair: PairedImage, **kwargs: object) -> AnalysisResult:
        raise RuntimeError("boom")

    def capture(event: ProgressEvent) -> None:
        events.append(event)

    monkeypatch.setattr(batch, "process_pair", fake_process_pair)

    summary = batch.process_all_pairs(
        [pair],
        progress=capture,
        **batch_kwargs(),
    )

    assert summary.total == 1
    assert summary.succeeded == 0
    assert summary.failed == 1

    assert len(events) == 2
    assert events[0].status == STATUS_STARTED
    assert events[1].status == STATUS_FAILED
    assert events[1].message is not None
    assert "boom" in events[1].message
