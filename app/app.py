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

from waggle.plugin import Plugin, get_timestamp


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
    uploaded_columns = [
        "original_path", "filename_at_upload", "size_bytes",
        "last_modified_timestamp_source", "upload_timestamp_utc",
        "metadata_sent_json", "upload_status"
    ]
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



def file_already_uploaded(file_path, size, mtime, uploaded_df):
    matches = uploaded_df[
        (uploaded_df['original_path'] == file_path) &
        (uploaded_df['size_bytes'] == size) &
        (uploaded_df['last_modified_timestamp_source'] == mtime) &
        (uploaded_df['upload_status'] == "success")
    ]
    return not matches.empty


def apply_filename_modifiers(filename, prefix, suffix):
    """Applies a prefix/suffix to a filename (before extension)."""
    base, ext = os.path.splitext(filename)
    return f"{prefix}{base}{suffix}{ext}"


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
    logging.info("Scanning for files...")
    all_files = []
    pattern = "**/*" if recursive else "*"
    if glob_pattern:
        pattern = glob_pattern

    paths = sorted(Path(folder_path).rglob(pattern) if recursive else Path(folder_path).glob(pattern))
    for p in paths:
        if p.is_dir():
            continue
        if p.is_symlink() and not transfer_symlinks:
            logging.info(f"Skipping symlink: {p}")
            continue

        str_path = str(p)
        try:
            stat = p.stat()
        except Exception as e:
            logging.warning(f"Error stat-ing file {p}: {e}")
            continue

        size = stat.st_size
        mtime = stat.st_mtime
        if file_already_uploaded(str_path, size, mtime, uploaded_df):
            continue

        all_files.append({
            "path": str_path,
            "size": size,
            "mtime": mtime,
            "name": p.name
        })

    # Sort
    if sort_key == "mtime":
        all_files.sort(key=lambda x: x["mtime"])
    else:
        all_files.sort(key=lambda x: x["name"])

    if skip_last_n > 0:
        deferred = all_files[-skip_last_n:]
        all_files = all_files[:-skip_last_n]
        logging.info(f"Skipping {len(deferred)} recently modified files")

    return all_files


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

    filename_on_beehive = apply_filename_modifiers(filename, args.prefix, args.suffix)
    file_to_upload = path

    temp_file = None
    try:
        if args.dry_run:
            logging.info(f"[Dry Run] Would upload: {file_to_upload}")
            return True, size

        plugin.publish("status", f'''Uploading {filename_on_beehive} 
                       upload_name: {metadata.get("upload_name", "unknown")}''')
        
        if args.timestamp == 'mtime':
            timestamp=int(mtime * 1e9)
        else:
            timestamp = get_timestamp()


        plugin.upload_file(file_to_upload, metadata, timestamp=timestamp, keep=True)
        append_to_csv(args.uploaded_csv, {
            "original_path": path,
            "filename_at_upload": filename_on_beehive,
            "size_bytes": size,
            "last_modified_timestamp_source": mtime,
            "upload_timestamp_utc": iso_utc(time.time()),
            "metadata_sent_json": json.dumps(metadata),
            "upload_status": "success"
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
    finally:
        if temp_file and os.path.exists(temp_file):
            os.remove(temp_file)


# ========== Main ==========
def main():
    parser = argparse.ArgumentParser(
        description="FileForager - Sync files from local folders to Beehive via Waggle plugin.upload_file."
    )

    parser.add_argument("--source", default="/data/", help="Source directory containing files to upload.")
    parser.add_argument("--glob", default=None, help="Optional glob pattern to filter files (e.g., '*.csv').")
    parser.add_argument("--timestamp", default='current', choices=['mtime', 'current'], help="Files timestamp in beehive")
    parser.add_argument("-r", "--recursive", action="store_true", help="Recursively scan subdirectories.")
    parser.add_argument("--skip-last-file", type=int, default=1, help="Skip the most recently modified N files.")
    parser.add_argument("--sort-key", choices=["mtime", "name"], default="mtime", help="Sort files by 'mtime' or 'name'.")
    parser.add_argument("--max-file-size", type=int, default=1 * 1024 * 1024 * 1024, help="Maximum file size to upload (in bytes).")
    parser.add_argument("--num-files", type=int, default=10, help="Number of files to upload per run.")
    parser.add_argument("--sleep", type=float, default=3, help="Sleep time (in seconds) between file uploads.")
    parser.add_argument("--prefix", default="", help="Prefix to prepend to uploaded file names.")
    parser.add_argument("--suffix", default="", help="Suffix to append to uploaded file names.")
    parser.add_argument("--dry-run", action="store_true", help="Preview actions without uploading files.")
    parser.add_argument("--delete-files", action="store_true", help="Delete source files after successful upload.")
    parser.add_argument("--transfer-symlinks", action="store_true", help="Follow and upload symlinks (default: skip).")
    parser.add_argument("--DEBUG", action="store_true", help="Enable detailed debug logging.")


    args = parser.parse_args()
    setup_logging(args)

    config_dir = f'{args.source}/.forager/'

    #check that source directory and config directory exist
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

    # if no csv file found then you need to create a file that gives empty df with correct column names.
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
