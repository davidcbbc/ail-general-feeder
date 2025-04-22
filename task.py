import os
import shutil
import mimetypes
import re
import zipfile
import subprocess
from celery import Celery

# Optional dependencies for RAR and 7z archives.
try:
    import rarfile  # pip install rarfile
except ImportError:
    rarfile = None

try:
    import py7zr  # pip install py7zr
except ImportError:
    py7zr = None

# Configure Celery to use RabbitMQ as broker
app = Celery('tasks', broker='pyamqp://guest@localhost//')

LOCAL_STORAGE = "./local_storage"


# Directories for storage and extraction\LOCAL_STORAGE = "./local_storage"
EXTRACTION_PATH = "./Leaks_Folder"
for path in (LOCAL_STORAGE, EXTRACTION_PATH):
    os.makedirs(path, exist_ok=True)

# Regex to extract candidate passwords from a message
PASSWORD_PATTERN = re.compile(r'\b(?:pwd|pw|password|pass)\b[\s:=]+(\S+)', re.IGNORECASE)


def extract_password_candidates(msg):
    """
    Extracts all candidate passwords from a text message using regex.

    Args:
        msg (str): The optional message containing password hints.

    Returns:
        list[str]: A list of extracted candidate passwords.
    """
    matches = PASSWORD_PATTERN.findall(msg or "")
    print(f"[DEBUG] extract_password_candidates -> {matches}")
    return matches


def try_zip_password(path, pwd, chunk_size=1024):
    """
    Tests a password against a ZIP archive by reading a small chunk.

    Args:
        path (str): Path to the ZIP file.
        pwd (str): Candidate password.
        chunk_size (int): Number of bytes to read.

    Returns:
        bool: True if decryption succeeded, False otherwise.
    """
    print(f"[DEBUG] try_zip_password: '{pwd}' on {path}")
    try:
        with zipfile.ZipFile(path) as zf:
            with zf.open(zf.namelist()[0], pwd=pwd.encode()) as f:
                f.read(chunk_size)
        print(f"[DEBUG] ZIP '{pwd}' succeeded")
        return True
    except Exception as e:
        print(f"[DEBUG] ZIP '{pwd}' failed: {e}")
        return False

def try_rar_password(path, pwd, chunk_size=1024):
    """
    Tests a password against a RAR archive by reading a small chunk from the first file.

    Args:
        path (str): Path to the RAR file.
        pwd (str): Candidate password.
        chunk_size (int): Number of bytes to read.

    Returns:
        bool: True if decryption succeeded, False otherwise.
    """
    print(f"[DEBUG] try_rar_password: '{pwd}' on {path}")
    if rarfile is None:
        print("[ERROR] rarfile module missing")
        return False
    try:
        with rarfile.RarFile(path) as rf:
            # Open the first file and read a small chunk to test password
            first = rf.namelist()[0]
            with rf.open(first, pwd=pwd) as f:
                f.read(chunk_size)
        print(f"[DEBUG] RAR '{pwd}' succeeded")
        return True
    except Exception as e:
        print(f"[DEBUG] RAR '{pwd}' failed: {e}")
        return False


def try_7z_password(path, pwd):
    """
    Tests a password against a 7z archive by listing its contents.

    Args:
        path (str): Path to the 7z file.
        pwd (str): Candidate password.

    Returns:
        bool: True if decryption succeeded, False otherwise.
    """
    print(f"[DEBUG] try_7z_password: '{pwd}' on {path}")
    if py7zr is None:
        print("[ERROR] py7zr module missing")
        return False
    try:
        with py7zr.SevenZipFile(path, mode='r', password=pwd) as a:
            a.getnames()
        print(f"[DEBUG] 7z '{pwd}' succeeded")
        return True
    except Exception as e:
        print(f"[DEBUG] 7z '{pwd}' failed: {e}")
        return False


def extract_zip(path, pwd, dest):
    """
    Extracts a ZIP archive to a destination directory.

    Args:
        path (str): Path to the ZIP file.
        pwd (str|None): Password or None for no password.
        dest (str): Destination directory.

    Returns:
        bool: True if extraction succeeded, False otherwise.
    """
    print(f"[DEBUG] extract_zip: {path} -> {dest}, pwd='{pwd}'")
    try:
        with zipfile.ZipFile(path) as zf:
            zf.extractall(dest, pwd=pwd.encode() if pwd else None)
        print("[DEBUG] ZIP extracted")
        return True
    except Exception as e:
        print(f"[ERROR] ZIP extract failed: {e}")
        return False


def extract_rar(path, pwd, dest):
    """
    Extracts a RAR archive to a destination directory.

    Args:
        path (str): Path to the RAR file.
        pwd (str|None): Password or None.
        dest (str): Destination directory.

    Returns:
        bool: True on success, False on error.
    """
    print(f"[DEBUG] extract_rar: {path} -> {dest}, pwd='{pwd}'")
    if rarfile is None:
        print("[ERROR] rarfile module missing")
        return False
    try:
        with rarfile.RarFile(path) as rf:
            rf.extractall(dest, pwd=pwd) if pwd else rf.extractall(dest)
        print("[DEBUG] RAR extracted")
        return True
    except Exception as e:
        print(f"[ERROR] RAR extract failed: {e}")
        return False


def extract_7z(path, pwd, dest):
    """
    Extracts a 7z archive to a destination directory.

    Args:
        path (str): Path to the 7z file.
        pwd (str|None): Password or None.
        dest (str): Destination directory.

    Returns:
        bool: True on success, False on error.
    """
    print(f"[DEBUG] extract_7z: {path} -> {dest}, pwd='{pwd}'")
    if py7zr is None:
        print("[ERROR] py7zr module missing")
        return False
    try:
        with py7zr.SevenZipFile(path, mode='r', password=pwd) as a:
            a.extractall(dest)
        print("[DEBUG] 7z extracted")
        return True
    except Exception as e:
        print(f"[ERROR] 7z extract failed: {e}")
        return False


def post_process():
    """
    Runs post-extraction steps: merge files and invoke main.py with config.
    """
    try:
        print("[DEBUG] Calling merge_files.sh")
        subprocess.run(["bash", "merge_files.sh","./Leaks_Folder"], check=True)
    except Exception as e:
        print(f"[ERROR] merge_files.sh failed: {e}")
    try:
        print("[DEBUG] Calling main.py")
        subprocess.run([
            "python3", "main.py",
            "-g", "/home/kali/ail-importers/ail-feeder-leak/config.yaml"
        ], check=True)
    except Exception as e:
        print(f"[ERROR] main.py failed: {e}")

@app.task
def process_file(file_path, optional_msg):
    """
    Processes a file: moves to storage, detects type, handles archives or text,
    extracts with or without password, then post-processes.

    Args:
        file_path (str): Path to the incoming file.
        optional_msg (str): Message containing password hints.
    """
    print(f"[INFO] process_file: path={file_path}, msg={optional_msg}")

    # Move to local storage
    try:
        dest = os.path.join(LOCAL_STORAGE, os.path.basename(file_path))
        shutil.move(file_path, dest)
        print(f"[DEBUG] moved to {dest}")
    except Exception as e:
        print(f"[ERROR] move failed: {e}")
        return

    # Detect MIME type
    mime, _ = mimetypes.guess_type(dest)
    print(f"[DEBUG] mime_type={mime}")

    # Handle uncompressed text files
    if mime and mime.startswith('text'):
        print("[DEBUG] Uncompressed text detected")
        try:
            shutil.copy(dest, EXTRACTION_PATH)
            print(f"[DEBUG] copied to {EXTRACTION_PATH}")
        except Exception as e:
            print(f"[ERROR] copy failed: {e}")
        post_process()
        return

    # Archive handling
    candidates = extract_password_candidates(optional_msg)
    lower = dest.lower()
    found = None
    atype = None

    def test(func, label):
        """
        Tests candidate passwords for a given archive type.
        """
        nonlocal found, atype
        atype = label
        for pwd in candidates:
            if func(dest, pwd):
                found = pwd
                break

    # ZIP
    if lower.endswith('.zip'):
        print("[DEBUG] handling ZIP")
        with zipfile.ZipFile(dest) as zf:
            enc = any(i.flag_bits & 1 for i in zf.infolist())
        print(f"[DEBUG] zip encrypted={enc}")
        if not enc:
            extract_zip(dest, None, EXTRACTION_PATH)
        else:
            test(try_zip_password, 'ZIP')
            if found:
                extract_zip(dest, found, EXTRACTION_PATH)

    # RAR
    elif lower.endswith('.rar'):
        print("[DEBUG] handling RAR")
        if rarfile is None:
            print("[ERROR] rarfile missing")
            return
        try:
            with rarfile.RarFile(dest) as rf:
                enc = rf.needs_password()
        except Exception as e:
            print(f"[ERROR] needs_password failed: {e}")
            enc = True
        print(f"[DEBUG] rar encrypted={enc}")
        if not enc:
            extract_rar(dest, None, EXTRACTION_PATH)
        else:
            test(try_rar_password, 'RAR')
            if found:
                extract_rar(dest, found, EXTRACTION_PATH)

    # 7z
    elif lower.endswith('.7z'):
        print("[DEBUG] handling 7z")
        if py7zr is None:
            print("[ERROR] py7zr missing")
            return
        try:
            with py7zr.SevenZipFile(dest, mode='r') as a:
                a.getnames()
            enc = False
        except:
            enc = True
        print(f"[DEBUG] 7z encrypted={enc}")
        if not enc:
            extract_7z(dest, None, EXTRACTION_PATH)
        else:
            test(try_7z_password, '7z')
            if found:
                extract_7z(dest, found, EXTRACTION_PATH)

    else:
        print("[WARN] unsupported type")
        return

    # Report password result
    if atype and found:
        print(f"[INFO] {atype} password found: {found}")
    elif atype:
        print(f"[WARN] no password for {atype}")

    print(f"[INFO] extraction complete at {EXTRACTION_PATH}")
    post_process()
