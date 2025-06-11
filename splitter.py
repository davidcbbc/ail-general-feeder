"""
splitter library

Provides a programmatic interface to split a large file into chunks
and send each chunk to an AIL instance via its HTTP API.

Usage:
    from single_file_ail_feeder import merge

    merge(
        file="/data/big_leak.txt",
        chunk_size=5000000,
        api_key="ABCDEF123456",
        ail_url="https://ail.example.org",
        uuid="123e4567-e89b-12d3-a456-426614174000",
        name="LeakFeeder",
        wait=0.5,
    )
"""
from __future__ import annotations

import base64
import gzip
import hashlib
import os
import re
import time
from pathlib import Path
from threading import Event
from concurrent.futures import ThreadPoolExecutor
from types import SimpleNamespace
from typing import List, Optional
import subprocess
import shlex

import requests
from fsplit.filesplit import Filesplit
from requests.packages.urllib3.exceptions import InsecureRequestWarning


def check_ail(ail_url: str, api_key: str) -> bool:
    """
    Return True if the AIL instance is reachable and responds with pong.

    :param ail_url: Base URL of the AIL instance (no trailing slash)
    :param api_key: Authentication token
    """
    try:
        requests.packages.urllib3.disable_warnings(InsecureRequestWarning)
        resp = requests.get(
            f"{ail_url}/ping",
            headers={"Content-Type": "application/json", "Authorization": api_key},
            timeout=30,
            verify=False,
        )
        return resp.json().get("status") == "pong"
    except Exception:
        return False


def publish_chunk(
    cfg: SimpleNamespace,
    chunk_path: str,
    sha256: str,
    lines: List[str],
    logger
) -> bool:
    """
    Compress the chunk to a .gz file, SCP it to a remote server and the execute the AIL FileImporter remotely through SSH.

    :param cfg:         Configuration namespace
    :param chunk_path:  Local chunk file path (no extension)
    :param lines:       Lines of text in the chunk
    :param logger:      Celery logger
    :return:            True on success, False on failure
    """
    # 1) write out a gzip file
    gz_path = f"{chunk_path}.gz"
    try:
        with gzip.open(gz_path, "wb") as gz:
            gz.write("".join(lines).encode("utf-8"))
    except Exception as e:
        logger.error(f"Failed to gzip-compress chunk: {e}")
        return False

    # 2) build scp command
    remote_src = f"{cfg.remote_user}@{cfg.server_ip}:{cfg.ail_gzip_path}/{sha256}.gz"
    scp_cmd = [
        "scp",
        "-i", f"{cfg.private_key}",
        "-o", "StrictHostKeyChecking=no",   # optional, skip host‐key prompt
        gz_path,
        remote_src
    ]

    
    # 3) run scp
    try:
        subprocess.run(scp_cmd, check=True, timeout=60)
    except subprocess.CalledProcessError as e:
        logger.error(f"SCP command failed (exit {e.returncode}): {e}")
        return False
    except Exception as e:
        logger.error(f"Error invoking SCP: {e}")
        return False
    
    

    # 4) run ssh command to inject the gzip into the AIL processing queue
    python_inline = (
        f"import sys; "
        f"import os; "
        f"sys.path.insert(0, '{cfg.ail_folder_path}/bin/importer/'); "
        f"os.environ['AIL_BIN'] = '{cfg.ail_folder_path}/bin'; "
        f"import FileImporter; "
        f"FileImporter.FileImporter().importer('{sha256}.gz')"
    )

    # 2) shell-quote it for python -c
    py_cmd = f"source {cfg.ail_folder_path}/AILENV/bin/activate && cd {cfg.ail_gzip_path} && python3 -c {shlex.quote(python_inline)}"

    # 3) shell-quote *that* for bash -lc
    bash_cmd = shlex.quote(py_cmd)
    ssh_cmd = [
        "ssh", 
        "-i", f"{cfg.private_key}",
        "-o", "StrictHostKeyChecking=no",
        f"{cfg.remote_user}@{cfg.server_ip}",
        "bash", "-lc", bash_cmd
    ]
    try:
        subprocess.run(ssh_cmd, check=True, timeout=200)
    except subprocess.CalledProcessError as e:
        logger.error(f"SSH inline-Python failed (exit {e.returncode}): {e}")
        return False
    except Exception as e:
        logger.error(f"Error invoking SSH: {e}")
        return False
    
    return True


def glob_chunk_files(dir_path: str, leak_path: str) -> List[str]:
    """
    Find all chunk files matching the pattern `<stem>_<index><suffix>` and return
    them sorted by numeric index.

    :param dir_path:  Directory to search
    :param leak_path: Original file path used to derive stem and suffix
    """
    stem = Path(leak_path).stem
    suffix = Path(leak_path).suffix
    pattern = f"{stem}_*{suffix}"
    paths = list(Path(dir_path).glob(pattern))
    idx_re = re.compile(rf"_(\d+){re.escape(suffix)}$")

    def numeric_key(p: Path) -> int:
        m = idx_re.search(p.name)
        return int(m.group(1)) if m else -1

    paths.sort(key=numeric_key)
    return [str(p) for p in paths]


def split_and_send(cfg: SimpleNamespace, leak_path: str, logger) -> None:
    """
    Split the file at `leak_path` into chunks of size `cfg.chunk_size` and
    send each chunk to the configured AIL instance.
    """
    leak_path = os.path.abspath(leak_path)
    dir_path = os.path.dirname(leak_path)

    # 1) Split ---------------------------------------------------------------
    logger.info(f"Splitting '{leak_path}' into ≈{cfg.chunk_size}‑byte chunks …")

    fs = Filesplit()
    fs.split(
        file=leak_path,
        split_size=cfg.chunk_size,
        output_dir=dir_path,
        newline=True,
        logger=logger
    )

    chunk_files = glob_chunk_files(dir_path, leak_path)
    if not chunk_files:
        logger.info("No chunks produced – nothing to do.")
        return

    if not check_ail(cfg.ail_url, cfg.api_key):
        raise ConnectionError("AIL instance unreachable")

    def load_chunk_data(chunk_path: str):
        with open(chunk_path, "rb") as fr:
            sha = hashlib.sha256(fr.read()).hexdigest()
        with open(chunk_path, encoding="utf-8", errors="ignore") as fr:
            lines = fr.readlines()
        return sha, lines

    with ThreadPoolExecutor(max_workers=min(len(chunk_files), os.cpu_count() or 4)) as pool:
        future_to_path = {}
        for chunk in chunk_files:
            chunk_abs = os.path.abspath(chunk)
            try:
                sha256, lines = load_chunk_data(chunk_abs)
            except Exception as e:
                raise IOError(f"Failed to read chunk {chunk_abs}: {e}")

            future = pool.submit(publish_chunk, cfg, chunk_abs, sha256, lines, logger=logger)
            future_to_path[future] = chunk_abs

        for fut in future_to_path:
            success = fut.result()
            if not success:
                raise RuntimeError(f"Upload failed for chunk {future_to_path[fut]}")
            if cfg.wait and cfg.wait > 0:
                Event().wait(cfg.wait)


def split(
    logger,
    ail_folder_path: str,
    ail_gzip_path: str,
    remote_user: str,
    server_ip: str,
    private_key: str,
    file: str,
    chunk_size: int,
    api_key: str,
    ail_url: str,
    uuid: str,
    name: str = "single_file_feeder",
    wait: Optional[float] = 0.5,
) -> None:
    """
    Public API: split `file` into chunks and push to AIL.

    :param file:       Path to the file to process
    :param chunk_size: Maximum chunk size in bytes
    :param api_key:    AIL API token
    :param ail_url:    Base URL of the AIL instance
    :param uuid:       Feeder UUID
    :param name:       Source name for metadata
    :param wait:       Seconds to wait between uploads
    :param logger:     Celery logger
    """
    cfg = SimpleNamespace(
        chunk_size=chunk_size,
        ail_folder_path=ail_folder_path,
        ail_gzip_path=ail_gzip_path,
        remote_user=remote_user,
        server_ip=server_ip,
        private_key=private_key,
        api_key=api_key,
        ail_url=ail_url,
        uuid=uuid,
        name=name,
        wait=wait or 0,
    )
    start = time.time()
    split_and_send(cfg, file, logger)
    elapsed = time.time() - start
    # Optionally return runtime or other metrics
    return elapsed
