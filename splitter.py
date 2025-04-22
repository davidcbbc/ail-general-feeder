#!/usr/bin/env python3
"""
Single‑file AIL feeder (glob version, **fixed pattern**)
-------------------------------------------------------
`Filesplit` names chunks as `original_stem_<index>.ext` (e.g. `big_leak_0.txt`).
This revision adjusts the glob and numeric‑sort logic accordingly.

Example:
    python single_file_ail_feeder.py \
        --file /data/big_leak.txt \
        --chunk-size 5000000 \
        --api-key ABCDEF123456 \
        --ail-url https://ail.example.org \
        --uuid 123e4567-e89b-12d3-a456-426614174000 \
        --name "Corporate Feeder" \
        --wait 0.5
"""
from __future__ import annotations

import argparse
import base64
import gzip
import hashlib
import os
import re
import time
from pathlib import Path
from threading import Event
from types import SimpleNamespace
from typing import List

import requests
from fsplit.filesplit import Filesplit
from requests.packages.urllib3.exceptions import InsecureRequestWarning

# ---------------------------------------------------------------------------
# AIL interaction helpers
# ---------------------------------------------------------------------------

def check_ail(ail_url: str, api_key: str) -> bool:
    """Return *True* if the AIL instance answers with *pong*."""
    try:
        requests.packages.urllib3.disable_warnings(InsecureRequestWarning)
        resp = requests.get(
            f"{ail_url}/ping",
            headers={"Content-Type": "application/json", "Authorization": api_key},
            timeout=30,
            verify=False,
        )
        return resp.json().get("status") == "pong"
    except Exception as exc:
        print(f"[check_ail] Could not reach AIL: {exc}")
        return False


def publish_chunk(cfg: SimpleNamespace, leak_path: str, chunk_path: str, sha256: str, lines: List[str]) -> bool:
    """Compress + encode the chunk and submit it to AIL."""
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
            print(f"[+] {os.path.basename(chunk_path)} uploaded successfully")
            os.remove(chunk_path)
            return True
        print(f"[!] Upload failed for {chunk_path}: {data.get('reason')}")
        return False
    except Exception as exc:
        print(f"[publish_chunk] Error while pushing {chunk_path}: {exc}")
        return False

# ---------------------------------------------------------------------------
# Core workflow
# ---------------------------------------------------------------------------

def glob_chunk_files(dir_path: str, leak_path: str) -> List[str]:
    """Return chunk paths (e.g. `big_leak_0.txt`) sorted by the numeric index."""
    stem = Path(leak_path).stem      # "big_leak"
    suffix = Path(leak_path).suffix  # ".txt"
    pattern = f"{stem}_*{suffix}"    # "big_leak_*\.txt"

    paths = list(Path(dir_path).glob(pattern))

    # Sort by the digits between the underscore and the extension
    idx_re = re.compile(rf"_(\d+){re.escape(suffix)}$")

    def numeric_key(p: Path):
        m = idx_re.search(p.name)
        return int(m.group(1)) if m else -1

    paths.sort(key=numeric_key)
    return [str(p) for p in paths]


def split_and_send(cfg: SimpleNamespace, leak_path: str) -> None:
    leak_path = os.path.abspath(leak_path)
    dir_path = os.path.dirname(leak_path)

    # 1) Split ---------------------------------------------------------------
    print(f"[*] Splitting '{leak_path}' into ≈{cfg.chunk_size}‑byte chunks …")
    fs = Filesplit()
    fs.split(
        file=leak_path,
        split_size=cfg.chunk_size,
        output_dir=dir_path,
        newline=True,
    )

    # Collect the chunk list via glob ---------------------------------------
    chunk_files = glob_chunk_files(dir_path, leak_path)

    if not chunk_files:
        print("[!] No chunks produced – nothing to do.")
        return

    # 2) Ping AIL once before the upload loop --------------------------------
    if not check_ail(cfg.ail_url, cfg.api_key):
        print("[!] AIL instance unreachable – aborting.")
        return

    # 3) Upload each chunk ----------------------------------------------------
    for chunk in chunk_files:
        chunk = os.path.abspath(chunk)
        try:
            with open(chunk, "rb") as fr:
                raw_bytes = fr.read()
                sha256 = hashlib.sha256(raw_bytes).hexdigest()
            with open(chunk, encoding="utf-8", errors="ignore") as fr:
                lines = fr.readlines()
        except Exception as exc:
            print(f"[read] Unable to read {chunk}: {exc}")
            continue

        if not publish_chunk(cfg, leak_path, chunk, sha256, lines):
            print("[!] Stopping after failure – remaining chunks kept on disk.")
            break

        if cfg.wait > 0:
            Event().wait(cfg.wait)

    print("[*] Done.")

# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def build_arg_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Split a single file and push its chunks to AIL (glob version)")
    p.add_argument("--file", "-f", required=True, help="Path to the leak file to process")
    p.add_argument("--chunk-size", "-c", type=int, required=True, help="Chunk size in bytes")
    p.add_argument("--api-key", "-k", required=True, help="AIL API token")
    p.add_argument("--ail-url", "-u", required=True, help="Base URL of the AIL instance (without trailing slash)")
    p.add_argument("--uuid", "-i", required=True, help="Feeder UUID")
    p.add_argument("--name", "-n", default="single_file_feeder", help="Feeder name visible in AIL")
    p.add_argument("--wait", "-w", type=float, default=0.5, help="Seconds to sleep between uploads")
    return p


def main() -> None:
    args = build_arg_parser().parse_args()

    cfg = SimpleNamespace(**vars(args))

    start = time.time()
    split_and_send(cfg, cfg.file)
    print(f"Run time(s): {time.time() - start:.2f}")


if __name__ == "__main__":
    main()
