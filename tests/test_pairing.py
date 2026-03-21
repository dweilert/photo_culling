from pathlib import Path

from photo_culling.pairing import discover_assets, pair_assets


def _make_file(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.touch()


def _basic_config() -> dict:
    return {
        "files": {
            "raw_extensions": [".arw", ".nef", ".cr2", ".cr3", ".dng"],
            "jpeg_extensions": [".jpg", ".jpeg"],
        }
    }


def test_pairing_basic_cases(tmp_path: Path) -> None:
    source_root = tmp_path / "photos"

    _make_file(source_root / "set1" / "IMG_0001.ARW")
    _make_file(source_root / "set1" / "IMG_0001.JPG")
    _make_file(source_root / "set1" / "IMG_0002.ARW")
    _make_file(source_root / "set1" / "IMG_0003.JPG")
    _make_file(source_root / "set1" / "IMG_0004.ARW")
    _make_file(source_root / "set1" / "IMG_0004-edit.JPG")
    _make_file(source_root / "set2" / "IMG_0001.ARW")
    _make_file(source_root / "set2" / "IMG_0001.JPG")

    assets = discover_assets(source_root, _basic_config())
    pairs = pair_assets(assets)

    found = {(pair.rel_dir.as_posix(), pair.base_name): pair.status for pair in pairs}

    assert found[("set1", "IMG_0001")] == "paired"
    assert found[("set1", "IMG_0002")] == "raw_only"
    assert found[("set1", "IMG_0003")] == "jpeg_only"
    assert found[("set1", "IMG_0004")] == "raw_only"
    assert found[("set1", "IMG_0004-edit")] == "jpeg_only"
    assert found[("set2", "IMG_0001")] == "paired"


def test_pairing_is_same_directory_only(tmp_path: Path) -> None:
    source_root = tmp_path / "photos"

    _make_file(source_root / "a" / "IMG_1000.ARW")
    _make_file(source_root / "b" / "IMG_1000.JPG")

    assets = discover_assets(source_root, _basic_config())
    pairs = pair_assets(assets)

    found = {(pair.rel_dir.as_posix(), pair.base_name): pair.status for pair in pairs}

    assert found[("a", "IMG_1000")] == "raw_only"
    assert found[("b", "IMG_1000")] == "jpeg_only"


def test_pairing_flags_ambiguous_multiple_raws(tmp_path: Path) -> None:
    source_root = tmp_path / "photos"

    _make_file(source_root / "set1" / "IMG_2000.ARW")
    _make_file(source_root / "set1" / "IMG_2000.NEF")
    _make_file(source_root / "set1" / "IMG_2000.JPG")

    assets = discover_assets(source_root, _basic_config())
    pairs = pair_assets(assets)

    assert len(pairs) == 1
    pair = pairs[0]

    assert pair.status == "ambiguous"
    assert pair.raw_count == 2
    assert pair.jpeg_count == 1
    assert "multiple_raws:2" in pair.notes


def test_pairing_flags_ambiguous_multiple_jpegs(tmp_path: Path) -> None:
    source_root = tmp_path / "photos"

    _make_file(source_root / "set1" / "IMG_3000.ARW")
    _make_file(source_root / "set1" / "IMG_3000.JPG")
    _make_file(source_root / "set1" / "IMG_3000.jpeg")

    assets = discover_assets(source_root, _basic_config())
    pairs = pair_assets(assets)

    assert len(pairs) == 1
    pair = pairs[0]

    assert pair.status == "ambiguous"
    assert pair.raw_count == 1
    assert pair.jpeg_count == 2
    assert "multiple_jpegs:2" in pair.notes


def test_discover_assets_empty_directory(tmp_path: Path) -> None:
    source_root = tmp_path / "empty"
    source_root.mkdir()

    assets = discover_assets(source_root, _basic_config())
    assert assets == []


def test_discover_assets_nonexistent_root_raises(tmp_path: Path) -> None:
    missing = tmp_path / "does_not_exist"

    try:
        discover_assets(missing, _basic_config())
        assert False, "Expected FileNotFoundError"
    except FileNotFoundError:
        pass


def test_discover_assets_file_as_root_raises(tmp_path: Path) -> None:
    file_path = tmp_path / "not_a_dir.txt"
    file_path.write_text("hello")

    try:
        discover_assets(file_path, _basic_config())
        assert False, "Expected NotADirectoryError"
    except NotADirectoryError:
        pass


def test_discover_assets_classifies_non_raw_non_jpeg_as_other(tmp_path: Path) -> None:
    source_root = tmp_path / "photos"
    _make_file(source_root / "IMG_0001.ARW")
    _make_file(source_root / "IMG_0001.JPG")
    _make_file(source_root / "document.pdf")
    _make_file(source_root / "video.mp4")

    assets = discover_assets(source_root, _basic_config())

    # discover_assets returns all files — non-raw/jpeg get classified as "other"
    assert len(assets) == 4
    kinds = {a.kind for a in assets}
    assert "other" in kinds

    # pair_assets is what ignores "other" — only raw/jpeg become pairs
    pairs = pair_assets(assets)
    assert len(pairs) == 1
    assert pairs[0].status == "paired"


def test_pair_assets_empty_input_returns_empty() -> None:
    result = pair_assets([])
    assert result == []


def test_pairing_case_insensitive_stem_matching(tmp_path: Path) -> None:
    source_root = tmp_path / "photos"

    # RAW has uppercase stem, JPEG has lowercase stem — should still pair
    _make_file(source_root / "IMG_5000.ARW")
    _make_file(source_root / "img_5000.JPG")

    assets = discover_assets(source_root, _basic_config())
    pairs = pair_assets(assets)

    assert len(pairs) == 1
    assert pairs[0].status == "paired"
