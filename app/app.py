#!/usr/bin/env python3

import os
import sys
import csv
import time
import argparse
import yaml
import json
import tempfile
import shutil
import re
import glob
import pandas as pd
import logging
from datetime import datetime, timezone
from pathlib import Path
from zipfile import ZipFile, ZIP_DEFLATED
import hashlib

from waggle.plugin import Plugin, get_timestamp

# @todo ADD: option to zip subdirectories and upload when asked by the user.

# ========== Constants ==========
UPLOADED_CSV = "uploaded_files.csv"
SKIPPED_CSV = "skipped_files.csv"
PROCESSING_LOG = "processing_errors.log"


# ========== Logging Setup ==========
def setup_logging(args):
    logging.basicConfig(
    level=logging.DEBUG if args.DEBUG else logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
    )


# ========== Utility Functions ==========
def load_yaml_file(path):
    try:
        with open(path, "r") as f:
            return yaml.safe_load(f)
    except Exception as e:
        logging.error(f"Error loading YAML file: {e}")
        return None


def validate_metadata(metadata):
    """
    Validates that all required fields are present in the metadata dict.
    Exits with an error if any are missing.
    """

    required_fields = ["upload_name", "site", "sensor", "creator", "original_path"]

    missing = [field for field in required_fields if field not in metadata]
    
    if missing:
        logging.error(f"Missing required metadata fields: {', '.join(missing)}")
        logging.info(f"Your metadata.yaml must contain these fields {required_fields}")
        sys.exit(1)
    
    # Make all values strings
    return {str(k): str(v) for k, v in metadata.items()}


def read_csv(path):
    """Reads a CSV into a DataFrame, or creates one with appropriate headers if file doesn't exist."""
    uploaded_columns = ["original_path", "filename", "last_modified_timestamp_source", "file_hash"]

    skipped_columns = [
        "file_path", "reason_skipped", "size_bytes",
        "last_modified_timestamp_source", "log_timestamp_utc"
    ]

    if not os.path.exists(path):
        if "uploaded" in str(path):
            df = pd.DataFrame(columns=uploaded_columns)
        elif "skipped" in str(path):
            df = pd.DataFrame(columns=skipped_columns)
        else:
            df = pd.DataFrame()
        df.to_csv(path, index=False)
        return df

    return pd.read_csv(path)


def append_to_csv(path, row_dict):
    df = pd.DataFrame([row_dict])
    if not os.path.exists(path):
        df.to_csv(path, index=False)
    else:
        df.to_csv(path, mode='a', header=False, index=False)


def iso_utc(ts):
    return datetime.fromtimestamp(ts, tz=timezone.utc).isoformat()


def compute_file_hash(file_path, hash_algo='sha256'):
    """Compute hash of a file."""
    hash_func = hashlib.new(hash_algo)
    with open(file_path, 'rb') as f:
        while chunk := f.read(8192):
            hash_func.update(chunk)
    return hash_func.hexdigest()


def file_already_uploaded(file_path, uploaded_df, hash_algo='sha256'):
    """Check if a file with the same hash has already been uploaded (regardless of name)."""
    file_hash = compute_file_hash(file_path, hash_algo)

    matches = uploaded_df[uploaded_df['file_hash'] == file_hash]

    return not matches.empty



def zip_directory(source_path, level):
    temp_file = tempfile.NamedTemporaryFile(delete=False, suffix=".zip")
    with ZipFile(temp_file.name, 'w', compression=ZIP_DEFLATED, compresslevel=level) as zipf:
        for root, dirs, files in os.walk(source_path):
            for file in files:
                full_path = os.path.join(root, file)
                arcname = os.path.relpath(full_path, start=source_path)
                zipf.write(full_path, arcname)
    return temp_file.name


# ========== File Discovery ==========
def discover_files(folder_path, glob_pattern, recursive, uploaded_df, skip_last_n, sort_key, transfer_symlinks):
    """Scan folder_path for eligible files to upload, applying filters and exclusions."""
    logging.info("Scanning for files...")
    all_files = []

    pattern = glob_pattern or ("**/*" if recursive else "*")
    base_path = Path(folder_path)
    paths = sorted(base_path.rglob(pattern) if recursive else base_path.glob(pattern))

    for path in paths:
        if should_skip_file(path, transfer_symlinks):
            continue

        file_info = get_file_stat_info(path)
        if not file_info:
            continue

        if is_already_uploaded(file_info["path"], uploaded_df):
            logging.info(f"Skipping already uploaded: {file_info['path']}")
            continue

        all_files.append(file_info)

    # Sort files
    if sort_key == "mtime":
        all_files.sort(key=lambda x: x["mtime"])
    else:
        all_files.sort(key=lambda x: x["name"])

    # Skip most recent N files
    if skip_last_n > 0:
        deferred = all_files[-skip_last_n:]
        all_files = all_files[:-skip_last_n]
        logging.info(f"Skipping {len(deferred)} recently modified files")

    return all_files

def is_hidden_path(path: Path) -> bool:
    """Check if any part of the path is hidden (starts with a dot)."""
    return any(part.startswith('.') for part in path.parts)


def should_skip_file(path: Path, transfer_symlinks: bool) -> bool:
    """Determine if the file should be skipped based on type, symlink, or hidden folder."""
    if path.is_dir():
        return True
    if path.is_symlink() and not transfer_symlinks:
        logging.debug(f"Skipping symlink: {path}")
        return True
    if is_hidden_path(path):
        logging.debug(f"Skipping hidden path: {path}")
        return True
    return False


def get_file_stat_info(path: Path):
    """Get file size, mtime, and name with safe error handling."""
    try:
        stat = path.stat()
        return {
            "path": str(path),
            "size": stat.st_size,
            "mtime": stat.st_mtime,
            "name": path.name
        }
    except Exception as e:
        logging.warning(f"Failed to stat file {path}: {e}")
        return None


def is_already_uploaded(path: str, uploaded_df) -> bool:
    """Return True if the file is already uploaded (based on hash)."""
    return file_already_uploaded(path, uploaded_df)



# ========== Upload Logic ==========
def prepare_and_upload_file(file_info, plugin, base_metadata, args, uploaded_df, skipped_df):
    path = file_info["path"]
    size = file_info["size"]
    mtime = file_info["mtime"]
    filename = os.path.basename(path)

    if args.max_file_size and size > args.max_file_size:
        reason = "max_size_exceeded"
        logging.warning(f"Skipping {path}: {reason}")
        append_to_csv(args.skipped_csv, {
            "file_path": path,
            "reason_skipped": reason,
            "size_bytes": size,
            "last_modified_timestamp_source": mtime,
            "log_timestamp_utc": iso_utc(time.time())
        })
        plugin.publish("error", f'''Skipped {path} reason: {reason} 
                       upload_name: {base_metadata.get("upload_name", "unknown")}''')
        return False, 0

    metadata = base_metadata.copy()
    metadata.update({
        "original_path": str(path),
        "filename": str(filename),
        "size_bytes": str(size),
        "last_modified_timestamp_source": iso_utc(mtime)
    })

    file_to_upload = path

    # Calculate file hash
    file_hash = compute_file_hash(file_to_upload)

    metadata["file_hash"] = file_hash

    try:
        if args.dry_run:
            logging.info(f"[Dry Run] Would not upload: {file_to_upload}")
            plugin.publish("status", f"[Dry Run] Would not upload: {file_to_upload}")
            return True, size

        plugin.publish("status", f'''Uploading {filename} 
                       upload_name: {metadata.get("upload_name", "unknown")}''')

        if args.timestamp == 'mtime':
            timestamp = int(mtime * 1e9)
        else:
            timestamp = get_timestamp()

        plugin.upload_file(file_to_upload, metadata, timestamp=timestamp, keep=True)

        append_to_csv(args.uploaded_csv, {
            "original_path": path,
            "filename": filename,
            "last_modified_timestamp_source": mtime,
            "file_hash": file_hash
        })


        if args.delete_files:
            os.remove(path)

        plugin.publish("status", f'''Uploaded {filename}
                       upload_name: {metadata.get("upload_name", "unknown")}''')

        return True, size

    except Exception as e:
        logging.error(f"Failed to upload {path}: {e}")
        append_to_csv(args.skipped_csv, {
            "file_path": path,
            "reason_skipped": str(e),
            "size_bytes": size,
            "last_modified_timestamp_source": mtime,
            "log_timestamp_utc": iso_utc(time.time())
        })
        plugin.publish("error", f'''Failed to upload {filename} error_details: {str(e)},
                       upload_name: {metadata.get("upload_name", "unknown")}''')
        return False, 0


# ========== Main ==========
def main():
    parser = argparse.ArgumentParser(
        description="FileForager - Sync files from local folders to Beehive via Waggle plugin.upload_file."
    )

    # Add arguments for the CLI
    parser.add_argument("--source", default="/data/", help="Source directory containing files to upload.")
    parser.add_argument("--glob", default=None, help="Optional glob pattern to filter files (e.g., '*.csv').")
    parser.add_argument("--timestamp", default='current', choices=['mtime', 'current'], help="Files timestamp in beehive")
    parser.add_argument("-r", "--recursive", action="store_true", help="Recursively scan subdirectories.")
    parser.add_argument("--skip-last-file", type=int, default=1, help="Skip the most recently modified N files.")
    parser.add_argument("--sort-key", choices=["mtime", "name"], default="mtime", help="Sort files by 'mtime' or 'name'.")
    parser.add_argument("--max-file-size", type=int, default=1 * 1024 * 1024 * 1024, help="Maximum file size to upload (in bytes).")
    parser.add_argument("--num-files", type=int, default=10, help="Number of files to upload per run.")
    parser.add_argument("--sleep", type=float, default=3, help="Sleep time (in seconds) between file uploads.")
    parser.add_argument("--dry-run", action="store_true", help="Preview actions without uploading files.")
    parser.add_argument("--delete-files", action="store_true", help="Delete source files after successful upload.")
    parser.add_argument("--transfer-symlinks", action="store_true", help="Follow and upload symlinks (default: skip).")
    parser.add_argument("--DEBUG", action="store_true", help="Enable detailed debug logging.")


    args = parser.parse_args()
    setup_logging(args)

    config_dir = f'{args.source}/.forager/'

    # Check that source directory and config directory exist
    if not (os.path.isdir(args.source) and os.path.isdir(config_dir)):
        logging.error("Missing source or config directory.")
        logging.info("Check documentation for creating config directory.")
        return -1

    args.uploaded_csv = os.path.join(config_dir, UPLOADED_CSV)
    args.skipped_csv = os.path.join(config_dir, SKIPPED_CSV)

    metadata_path = os.path.join(config_dir, "metadata.yaml")
    metadata = load_yaml_file(metadata_path)
    if metadata is None:
        logging.error("Missing or bad metadata.yaml")
        return

    metadata = validate_metadata(metadata)

    # If no CSV file found then create an empty dataframe with correct columns.
    uploaded_df = read_csv(args.uploaded_csv)
    skipped_df = read_csv(args.skipped_csv)

    with Plugin() as plugin:
        files = discover_files(args.source, args.glob,
                               args.recursive, uploaded_df, args.skip_last_file,
                               args.sort_key, args.transfer_symlinks)
        logging.info(f"Found {len(files)} files to process.")
        plugin.publish("status", f'''Found {len(files)} recent files. upload_name: {metadata.get("upload_name", "unknown")}''')

        count = 0
        total_bytes = 0
        for file_info in files:
            if count >= args.num_files:
                break
            success, size = prepare_and_upload_file(file_info, plugin, metadata, args, uploaded_df, skipped_df)
            if success:
                count += 1
                total_bytes += size
            time.sleep(args.sleep)

        plugin.publish("upload.stats", f'''transferred_count: {count} , 
                       total_bytes: {total_bytes},
                       upload_name: {metadata.get("upload_name", "unknown")}''')
        logging.info("Run complete.")


if __name__ == "__main__":
    main()
