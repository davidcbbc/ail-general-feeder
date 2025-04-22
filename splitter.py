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
from types import SimpleNamespace
from typing import List, Optional

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
    leak_path: str,
    chunk_path: str,
    sha256: str,
    lines: List[str],
) -> bool:
    """
    Compress, base64-encode, and send a chunk to the AIL API.

    :param cfg:        Configuration namespace (name, uuid, api_key, ail_url)
    :param leak_path:  Original file path
    :param chunk_path: Chunk file path
    :param sha256:     SHA256 digest of the chunk
    :param lines:      Lines of text in the chunk
    :return:           True on success, False on failure
    """
    comp_b64 = base64.b64encode(gzip.compress("".join(lines).encode("utf-8"))).decode()
    payload = {
        "source": cfg.name,
        "source-uuid": cfg.uuid,
        "default-encoding": "UTF-8",
        "meta": {
            "Leaked:FileName": os.path.basename(leak_path),
            "Leaked:Chunked": os.path.basename(chunk_path),
        },
        "data-sha256": sha256,
        "data": comp_b64,
    }
    url = f"{cfg.ail_url}/import/json/item"
    try:
        resp = requests.post(
            url,
            headers={"Content-Type": "application/json", "Authorization": cfg.api_key},
            json=payload,
            timeout=120,
            verify=False,
        )
        data = resp.json()
        if data.get("status") == "success":
            os.remove(chunk_path)
            return True
        return False
    except Exception:
        return False


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


def split_and_send(cfg: SimpleNamespace, leak_path: str) -> None:
    """
    Split the file at `leak_path` into chunks of size `cfg.chunk_size` and
    send each chunk to the configured AIL instance.
    """
    leak_path = os.path.abspath(leak_path)
    dir_path = os.path.dirname(leak_path)

    # 1) Split ---------------------------------------------------------------
    print(f"[INFO] Splitting '{leak_path}' into ≈{cfg.chunk_size}‑byte chunks …")

    fs = Filesplit()
    fs.split(
        file=leak_path,
        split_size=cfg.chunk_size,
        output_dir=dir_path,
        newline=True,
    )

    chunk_files = glob_chunk_files(dir_path, leak_path)
    if not chunk_files:
        print("[DEBUG] No chunks produced – nothing to do.")
        return

    if not check_ail(cfg.ail_url, cfg.api_key):
        raise ConnectionError("AIL instance unreachable")

    for chunk in chunk_files:
        chunk_abs = os.path.abspath(chunk)
        try:
            with open(chunk_abs, "rb") as fr:
                sha256 = hashlib.sha256(fr.read()).hexdigest()
            with open(chunk_abs, encoding="utf-8", errors="ignore") as fr:
                lines = fr.readlines()
        except Exception as e:
            raise IOError(f"Failed to read chunk {chunk_abs}: {e}")

        success = publish_chunk(cfg, leak_path, chunk_abs, sha256, lines)
        if not success:
            raise RuntimeError(f"Upload failed for chunk {chunk_abs}")
        if cfg.wait and cfg.wait > 0:
            Event().wait(cfg.wait)


def split(
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
    """
    cfg = SimpleNamespace(
        chunk_size=chunk_size,
        api_key=api_key,
        ail_url=ail_url,
        uuid=uuid,
        name=name,
        wait=wait or 0,
    )
    start = time.time()
    split_and_send(cfg, file)
    elapsed = time.time() - start
    # Optionally return runtime or other metrics
    return elapsed
