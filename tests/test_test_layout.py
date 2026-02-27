from pathlib import Path


def test_expected_test_directories_exist() -> None:
    assert Path("tests/db").is_dir()
    assert Path("tests/etl").is_dir()
    assert Path("tests/pipeline").is_dir()
