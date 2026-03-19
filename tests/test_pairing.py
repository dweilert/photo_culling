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
