#!/usr/bin/env python3
"""
multimove_merge.py  –  drop‑in Python replacement for the Bash script
---------------------------------------------------------------
1.  Flatten the directory tree (move every file in sub‑folders up to the
    given directory), renaming on collisions by appending “_N”.
2.  Delete the now‑empty sub‑folders.
3.  Detect **real** text files by MIME type (extension‑agnostic) and
    merge them into merged.txt.
4.  Remove every file except merged.txt.

The heavy‑lifting I/O (moving and merging) is parallelised with the
standard‑library ThreadPoolExecutor.

Requires:  Python ≥ 3.9 and the package *python‑magic*  
       `pip install python‑magic`
"""

from __future__ import annotations

import argparse
import shutil
import sys
import threading
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

try:
    import magic  # python‑magic – wrapper around libmagic / the *file* utility
except ImportError as exc:  # pragma: no cover
    sys.exit(
        "python‑magic is required (pip install python‑magic). "
        "It in turn needs libmagic on your system."
    )


def move_with_collision_handling(src: Path, target_dir: Path, lock: threading.Lock, names_taken: set[str]) -> None:
    """Move *src* into *target_dir*, renaming if necessary so the basename is unique."""
    name = src.name
    with lock:  # ensure the name negotiation is atomic
        dest = target_dir / name
        if dest.exists() or dest.name in names_taken:
            base, ext = dest.stem, dest.suffix
            counter = 1
            while True:
                candidate = target_dir / f"{base}_{counter}{ext}"
                if not candidate.exists() and candidate.name not in names_taken:
                    dest = candidate
                    break
                counter += 1
        names_taken.add(dest.name)

    shutil.move(src, dest)


def append_if_text(fpath: Path, merged_path: Path, lock: threading.Lock) -> None:
    """Append file content to *merged_path* if libmagic says it is text/* ."""
    mime = magic.from_file(str(fpath), mime=True)
    if mime and mime.startswith("text/"):
        # Read + append under lock so lines from different threads never interleave
        with lock:
            with merged_path.open("a", encoding="utf-8", errors="ignore") as merged, \
                    fpath.open("r", encoding="utf-8", errors="ignore") as src:
                shutil.copyfileobj(src, merged, length=1024 * 1024)
                merged.write("\n")          # separator between files


def main() -> None:
    parser = argparse.ArgumentParser(description="Flatten directory, merge text files, leave only merged.txt.")
    parser.add_argument("directory", type=Path, help="Path to the target directory")
    args = parser.parse_args()

    target_dir: Path = args.directory.expanduser().resolve()

    if not target_dir.is_dir():
        sys.exit(f"Error: '{target_dir}' is not a valid directory.")

    # ------------------------------------------------------------------ #
    # 1. Move every file from sub‑dirs up one level                       #
    # ------------------------------------------------------------------ #
    to_move = [p for p in target_dir.rglob("*") if p.is_file() and p.parent != target_dir]

    lock = threading.Lock()
    existing_names = {p.name for p in target_dir.iterdir() if p.is_file()}

    with ThreadPoolExecutor() as pool:
        for path in to_move:
            pool.submit(move_with_collision_handling, path, target_dir, lock, existing_names)

    # Remove every (now empty) sub‑directory – walk bottom‑up
    for subdir in sorted([d for d in target_dir.rglob("*") if d.is_dir()], reverse=True):
        subdir.rmdir()

    # ------------------------------------------------------------------ #
    # 2. Merge every *real* text file into merged.txt                    #
    # ------------------------------------------------------------------ #
    merged_path = target_dir / "merged.txt"
    if merged_path.exists():
        merged_path.unlink()
    merged_path.touch()

    merge_lock = threading.Lock()
    candidates = [p for p in target_dir.iterdir() if p.is_file() and p != merged_path]

    with ThreadPoolExecutor() as pool:
        for fpath in candidates:
            pool.submit(append_if_text, fpath, merged_path, merge_lock)

    # ------------------------------------------------------------------ #
    # 3. Delete everything except merged.txt                             #
    # ------------------------------------------------------------------ #
    for p in target_dir.iterdir():
        if p.is_file() and p != merged_path:
            p.unlink()

    print(f"✅  Done. All text files merged into {merged_path}.")


if __name__ == "__main__":
    main()
