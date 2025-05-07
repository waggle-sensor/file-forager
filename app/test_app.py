import os
import pytest
import yaml
import pandas as pd
from pathlib import Path
import sys
from datetime import datetime, timezone
import shutil
import pandas as pd




from app import (
    load_yaml_file, validate_metadata, read_csv, append_to_csv,
    iso_utc, file_already_uploaded, apply_filename_modifiers,
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
        "original_path", "filename_at_upload", "size_bytes",
        "last_modified_timestamp_source", "upload_timestamp_utc",
        "metadata_sent_json", "upload_status"
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


def test_file_already_uploaded_match():
    data = {
        "original_path": ["/a.txt"],
        "size_bytes": [100],
        "last_modified_timestamp_source": [1234567890],
        "upload_status": ["success"]
    }
    df = pd.DataFrame(data)
    assert file_already_uploaded("/a.txt", 100, 1234567890, df) is True
    assert file_already_uploaded("/a.txt", 200, 1234567890, df) is False


def test_apply_filename_modifiers():
    assert apply_filename_modifiers("data.txt", "pre_", "_suf") == "pre_data_suf.txt"


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
    df = pd.DataFrame(columns=["original_path", "size_bytes", "last_modified_timestamp_source", "upload_status"])
    files = discover_files(str(tmp_path), None, False, df, 0, "name", transfer_symlinks=False)
    assert all(f["path"] != str(symlink) for f in files)


def test_discover_files_skips_recent(tmp_path):
    f1 = tmp_path / "a.txt"
    f2 = tmp_path / "b.txt"
    f1.write_text("data1")
    f2.write_text("data2")
    df = pd.DataFrame(columns=["original_path", "size_bytes", "last_modified_timestamp_source", "upload_status"])
    files = discover_files(str(tmp_path), None, False, df, skip_last_n=1, sort_key="name", transfer_symlinks=True)
    assert len(files) == 1