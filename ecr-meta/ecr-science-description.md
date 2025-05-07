# FileForager

**FileForager** is a Waggle plugin for edge nodes that reliably uploads files from local directories to Beehive storage. It supports filtering, batching, custom metadata, and robust logging.

---

## 🚀 Features

- Uploads files using `plugin.upload_file()` with custom metadata
- Maintains persistent upload state with local CSV logs
- Skips already uploaded or oversized files
- Supports recursive folder scanning, glob filters, and symlink handling
- Publishes real-time status and error messages via `plugin.publish`

---

## ⚙️ Setup Instructions

### 1. Create Test Directory

```bash
mkdir ff-test-data/
cd ff-test-data
mkdir .forager
cd .forager
```

### 2. Create `metadata.yaml`
```yaml
upload_name: "test upload"
site: "some place"
sensor: "some sensor"
creator: "your name"
original_path: "orignal waggle path"
project: "your project"
```
All values must be strings. These fields are required — the program will exit if any are missing.

### 3. Create test files
```bash
cd ..
echo "file 1" > test1.txt
echo "file 2" > test2.txt
```

### 4. Build and test on the node 
```bash
git clone https://github.com/waggle-sensor/file-forager
cd file-forager
sudo pluginctl build .
```
```bash
sudo pluginctl deploy -n fftest \
  --entrypoint /bin/bash \
  --selector zone=core \
  -v /home/waggle/bhupendra/ff-test-data/:/data/ \
  10.31.81.1:5000/local/file-forager \
  -- -c 'while true; do date; sleep 1; done'

sudo pluginctl ps
```

```bash
sudo pluginctl exec -ti fftest -- /bin/bash

```

### 5. Running forager in the container
```bash
python3 fileforager.py \
  --source /data/ \
  --recursive \
  --glob "*.txt" \
  --skip-last-file 1 \
  --num-files 2 \
  --dry-run \
  --DEBUG
```
Remove --dry-run to perform actual uploads.

## 🛠️ FileForager CLI Options

| Option                | Description                           |
| --------------------- | ------------------------------------- |
| `--source`            | Source directory (default: `/data/`)  |
| `--glob`              | Glob pattern (e.g., `*.csv`)          |
| `--recursive`         | Recursively scan subfolders           |
| `--skip-last-file`    | Skip N most recently modified files   |
| `--sort-key`          | Sort by `"mtime"` or `"name"`         |
| `--max-file-size`     | Max size per file (in bytes)          |
| `--num-files`         | Number of files to upload per run     |
| `--sleep`             | Delay between uploads                 |
| `--dry-run`           | Don't actually upload — just simulate |
| `--delete-files`      | Delete original file after upload     |
| `--transfer-symlinks` | Follow symlinks (default: skip)       |
| `--DEBUG`             | Enable debug logging                  |

## 📝 Logging and Status

Logs are written to .forager/processing_errors.log

Upload state is tracked in:

    .forager/uploaded_files.csv

    .forager/skipped_files.csv

Status is published via `plugin.publish("status", ...)`
Errors are published via `plugin.publish("error", ...)`
Final stats published via `plugin.publish("upload.stats", ...)`


## 🔁 Job Submission

```yaml
name: cl61-upload
plugins:
- name: cl61-upload
  pluginSpec:
    image: registry.sagecontinuum.org/bhupendraraut/file-forager:0.25.x.x
    args:
    - --glob
    - '*.txt'
    - --recursive
    - --timestamp
    - mtime
    - --skip-last-file
    - "1"
    - --sort-key
    - mtime
    - --num-files
    - "10"
    - --sleep
    - "5"
    selector:
      zone: core
    volume:
      /home/waggle/data: /data
nodeTags: []
nodes:
  W0xx: true
scienceRules:
- 'schedule("cl61-upload"): cronjob("cl61-upload", "20 * * * *")'
successCriteria: []
```

## ⬇️ Querying Uploads with Metadata Filtering

To ensure each user accesses only their own uploaded data, we use metadata keys defined in `metadata.yaml` to filter results. The query below retrieves data from the File Forager app using `sage_data_client.query()` with appropriate constraints:

```python
import sage_data_client

df = sage_data_client.query(
    start="2025-04-10T21:26:00Z",
    end="2025-04-10T22:26:00Z",
    filter={
        "plugin": ".*file-forager:0.25.5.7",
        "vsn": "W09A",
        "upload_name": "cl61_files",
        "site": "ATMOS",
        "sensor": "vaisala_cl61",
    }
)
```

## 📢 Contact

Questions or contributions? Reach out to the Waggle or CROCUS community or open a GitHub issue.