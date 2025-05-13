import os
import pytest
import yaml
import pandas as pd
from pathlib import Path
import sys
from datetime import datetime, timezone
import shutil
import pandas as pd
import time


from app import (
    load_yaml_file, validate_metadata, read_csv, append_to_csv,
    iso_utc, file_already_uploaded, compute_file_hash,
    zip_directory, discover_files
)

from unittest.mock import patch, MagicMock


def test_load_yaml_file_valid(tmp_path):
    path = tmp_path / "meta.yaml"
    content = {"key": "value"}
    path.write_text(yaml.dump(content))
    assert load_yaml_file(path) == content


def test_load_yaml_file_invalid(tmp_path):
    path = tmp_path / "bad.yaml"
    path.write_text("[invalid: yaml")
    assert load_yaml_file(path) is None



def test_validate_metadata_valid():
    meta = {
        "upload_name": "abc",
        "site": "xyz",
        "sensor": "s1",
        "creator": "you",
        "original_path": "/some/path"
    }
    validated = validate_metadata(meta)
    assert all(k in validated for k in meta)


def test_validate_metadata_missing(caplog):
    with pytest.raises(SystemExit):
        validate_metadata({"upload_name": "abc"})
    assert "Missing required metadata fields" in caplog.text



def test_read_csv_creates_new(tmp_path):
    path = tmp_path / "uploaded_files.csv"
    df = read_csv(path)
    assert path.exists()
    assert list(df.columns) == [
        "original_path",
        "filename",
        "last_modified_timestamp_source",
        "file_hash"
    ]



def test_append_to_csv_creates_and_appends(tmp_path):
    path = tmp_path / "file.csv"
    row = {"a": 1, "b": 2}
    append_to_csv(path, row)
    df = pd.read_csv(path)
    assert len(df) == 1
    assert df.iloc[0]["a"] == 1


def test_iso_utc_conversion():
    ts = 1700000000.0  # fixed timestamp
    dt = iso_utc(ts)
    assert dt.endswith("Z") or dt.endswith("+00:00")


def test_file_already_uploaded_match(tmp_path):
    test_file = tmp_path / "a.txt"
    test_file.write_text("hello")

    file_hash = compute_file_hash(test_file)

    df = pd.DataFrame([{
        "original_path": str(test_file),
        "filename": "a.txt",
        "last_modified_timestamp_source": test_file.stat().st_mtime,
        "file_hash": file_hash
    }])

    assert file_already_uploaded(str(test_file), df) is True

def test_zip_directory_creates_zip(tmp_path):
    d = tmp_path / "dir"
    d.mkdir()
    f = d / "f.txt"
    f.write_text("hello")
    zipf = zip_directory(d, level=1)
    assert zipf.endswith(".zip")
    assert os.path.exists(zipf)


def test_discover_files_filters_symlinks(tmp_path):
    real_file = tmp_path / "a.txt"
    real_file.write_text("x")
    symlink = tmp_path / "link.txt"
    symlink.symlink_to(real_file)

    df = pd.DataFrame(columns=[
        "original_path", "filename", "last_modified_timestamp_source", "file_hash"
    ])

    files = discover_files(
        str(tmp_path), None, recursive=False,
        uploaded_df=df,
        skip_last_n=0,
        sort_key="name",
        transfer_symlinks=False
    )

    # Only the real file should be included, not the symlink
    assert len(files) == 1
    assert files[0]["path"] == str(real_file)

def test_discover_files_skips_recent(tmp_path):
    f1 = tmp_path / "a.txt"
    f2 = tmp_path / "b.txt"

    f1.write_text("data1")
    time.sleep(1)  # ensure different mtimes
    f2.write_text("data2")

    df = pd.DataFrame(columns=[
        "original_path", "filename", "last_modified_timestamp_source", "file_hash"
    ])

    files = discover_files(
        str(tmp_path), None, recursive=False,
        uploaded_df=df,
        skip_last_n=1,
        sort_key="mtime",
        transfer_symlinks=True
    )

    # Only 1 file should be returned (the older one)
    assert len(files) == 1
    assert files[0]["path"] == str(f1)


    def test_discover_files_multiple_extensions(tmp_path):
        (tmp_path / "file.csv").write_text("csv")
        (tmp_path / "file.zip").write_text("zip")
        (tmp_path / "file.txt").write_text("txt")

        df = pd.DataFrame(columns=["original_path", "filename", "last_modified_timestamp_source", "file_hash"])
        files = discover_files(
            str(tmp_path),
            "*.{" + ",".join(["csv", "zip"]) + "}",
            recursive=False,
            uploaded_df=df,
            skip_last_n=0,
            sort_key="name",
            transfer_symlinks=True
        )
        assert len(files) == 2
