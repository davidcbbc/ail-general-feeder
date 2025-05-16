# ail-general-feeder

## Overview

`ail-general-feeder` is an asynchronous file processing pipeline that takes arbitrary files (including archives with optional password hints), extracts and merges their contents, and feeds them into an AIL (Analysis Information Leak) framework instance in configurable-sized chunks.

## Features

- Celery-based task queue with RabbitMQ broker
- Archive extraction with optional password hints and recursive handling
- Flattening directory structures and merging pure text files
- Splitting large merged text files into chunks and uploading via HTTP API
- Configurable chunk size, wait times, and metadata

## Architecture

1. **Publisher (`publisher.py`)**: Enqueues a file processing task with an optional message containing password hints.
2. **Worker (`worker.py`)**: A Celery task that:
   - Moves the incoming file to local storage
   - Detects MIME type to choose between text or archive handling
   - Extracts archives using `patoolib`, with or without passwords, recursively
   - Invokes post-processing steps
3. **Post-Processing**:
   - **Merge (`merge_files.py`)**: Flattens any nested directories under the working folder and merges all real text files into `merged.txt`
   - **Split (`splitter.py`)**: Splits `merged.txt` into compressed, base64-encoded chunks and uploads them to an AIL instance via its HTTP JSON API

## Prerequisites

- Python ≥ 3.9
- RabbitMQ server
- System dependencies for archive handling and MIME detection:
  - `libmagic` (for `python-magic`)
  - `patool`
  - Archive utilities: `zip`, `unrar`, `p7zip-full`, `tar`, etc.

## Installation

```bash
git clone https://github.com/davidcbbc/ail-general-feeder.git
cd ail-general-feeder
pip install -r requirements.txt
```

On Debian/Ubuntu:
```bash
sudo apt-get update
sudo apt-get install -y python3-dev libmagic1 libmagic-dev poppler-utils unrar p7zip-full \
    zip tar gzip bzip2 xz-utils
```

## Configuration

> **Note:** Configuration values are hardcoded in `worker.py`. Update the following variables to match your environment:

- `LOCAL_STORAGE`: Directory to which incoming files are moved
- `EXTRACTION_PATH`: Working directory for extraction and merging
- Celery broker URL (in the `Celery(...)` call)
- `CHUNK_SIZE`: Maximum bytes per chunk in `splitter.py`
- `API_KEY`, `AIL_URL`, `UUID`, `NAME`, `WAIT`: Metadata and connection details for the AIL instance

## Usage

### 1. Start RabbitMQ

```bash
sudo systemctl start rabbitmq-server
```

(Optional) Change the worker timeout to 10 hours. This will prevent workers to stop working
```bash
# Set the value to 10h
rabbitmqctl eval 'application:set_env(rabbit, consumer_timeout, 36000000).'

# Check if the value was set
rabbitmqctl eval 'application:get_env(rabbit, consumer_timeout).'
```

### 2. Start the Celery Worker

```bash
celery -A worker worker --loglevel=info --detach --logfile=./worker.log --pidfile=./worker.pid --concurrency=1 --prefetch-multiplier=1
```

### 3. Submit Files for Processing

```bash
python3 publisher.py /path/to/leak-file.zip
```

You can also pass an optional password hint string:
```bash
python3 publisher.py /path/to/leak-file.zip "password: secret123 pwd=guess456"
```

## Directory Structure

```
├── merge_files.py    # Flatten and merge text files
├── splitter.py       # Split merged file and send to AIL
├── publisher.py      # Enqueue processing tasks
├── worker.py         # Celery worker and task definitions
├── requirements.txt  # Python dependencies
└── README.md         # Project documentation
```

## Contributing

Contributions, issues, and feature requests are welcome! Feel free to fork the repository and open a pull request.
