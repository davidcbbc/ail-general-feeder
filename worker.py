import os
import shutil
import mimetypes
import re
import patoolib
from patoolib.util import PatoolError
from celery import Celery
from celery.utils.log import get_task_logger
import magic
import merge_files
import splitter

#### Worker Configurations ####
# Configure Celery to use RabbitMQ as broker
app = Celery('tasks', broker='pyamqp://guest@localhost//')

# Set celery logger
logger = get_task_logger(__name__)

# Local storage for copying the files
LOCAL_STORAGE = "./Leaks_Storage"

# Directories for storage and extraction\LOCAL_STORAGE = "./Leaks_Storage"
EXTRACTION_PATH = "./Working_Folder"

# Regex to extract candidate passwords from a message
PASSWORD_PATTERN = re.compile(r'\b(?:pwd|pw|password|pass)\b[\s:=]+(\S+)', re.IGNORECASE)

#### Splitter Configurations ####
# Maximum chunk size in bytes
CHUNK_SIZE = 1000000

# AIL API token
API_KEY = ""

# Base URL of the AIL instance with API endpoint
AIL_URL = "https://localhost:443/api/v1"

# Feeder UUID
UUID = "17450648-9581-42a6-b7c4-28c13f4664bf"

# Source name for metadata
NAME = "LeakFeeder"

# Seconds to wait between uploads
WAIT = 0.2

#### Functions ####

def cleanup_extraction_path():
    """
    Remove all files in the extraction path and recreate the directory.
    """
    logger.info(f"cleaning {EXTRACTION_PATH} ...")
    try:
        shutil.rmtree(EXTRACTION_PATH)
    except Exception as e:
        logger.error(f"Failed to clean extraction path: {e}")
    os.makedirs(EXTRACTION_PATH, exist_ok=True)

def extract_password_candidates(msg):
    """
    Extracts all candidate passwords from a text message using regex.

    Args:
        msg (str): The optional message containing password hints.

    Returns:
        list[str]: A list of extracted candidate passwords.
    """
    matches = PASSWORD_PATTERN.findall(msg or "")
    logger.info(f"extract_password_candidates -> {matches}")
    return matches


def is_archive_file(path: str) -> bool:
    """
    Return True if 'path' is a recognized archive type, based on its MIME type.

    :param path: Filesystem path to test
    :return:     True if libmagic reports an archive mimetype, False otherwise
    """
    # Initialize the magic object (mime=True for MIME types)
    mime = magic.Magic(mime=True)

    try:
        mtype = mime.from_file(path)
    except Exception as e:
        logger.error(f"Error determining mime type for {path}: {e}")
        return False

    # Common archive MIME types
    archive_mimes = {
        "application/zip",           # ZIP files
        "application/x-rar",         # RAR files
        "application/vnd.rar",       # Alternate RAR vendor type
        "application/x-7z-compressed",  # 7z files
        "application/x-tar",         # TAR archives
        "application/gzip",          # .gz
        "application/x-bzip2",       # .bz2
        "application/x-xz",          # .xz
    }

    #print("[info] Archive mime type is {mtype}")

    return mtype in archive_mimes

def is_valid_password(archive_path: str, password: str) -> bool:
    """
    Verify that `password` can unlock and pass an integrity test on the compressed file.

    :param archive_path: Path to the compressed file
    :param password:     Candidate password for the archive
    :return:             True if password is correct, False otherwise
    :raises FileNotFoundError: If the archive does not exist
    """
    if not os.path.isfile(archive_path):
        raise FileNotFoundError(f"[ERROR] No such file: {archive_path}")

    try:
        # Runs the equivalent of `patool test --verbose archive.rar`
        logger.info(f"Testing password for archive {archive_path}")
        patoolib.test_archive(
            archive_path,
            verbosity=-2,
            interactive=False,
            password=password)
        return True
    except PatoolError as e:
        logger.error(f"Password test failed for {archive_path}: {e}")
        return False

def recursive_extract(directory: str, password: str = None, max_depth: int = 8, _current_depth: int = 0) -> None:
    """
    Recursively scan 'directory' for archives. Extract any found in place,
    then recurse into newly created subdirectories or files, up to max_depth.

    :param directory:     Root directory to scan for archives.
    :param password:      Password to pass to patool (if needed).
    :param max_depth:     Maximum recursion depth.
    :param _current_depth: Internal counter; do not pass in manually.
    :raises RuntimeError: If any extraction fails.
    """
    # Stop if we've reached maximum recursion depth
    if _current_depth >= max_depth:
        return

    # Find all archive files under 'directory'
    archives = []
    for root, dirs, files in os.walk(directory):
        for fname in files:
            full_path = os.path.join(root, fname)
            if is_archive_file(full_path):
                archives.append(full_path)

    # If no archives found, nothing to do
    if not archives:
        return

    # Extract each archive and recurse into its extraction folder
    for archive_path in archives:
        parent = os.path.dirname(archive_path)
        base_name, _ = os.path.splitext(os.path.basename(archive_path))
        extract_path = os.path.join(parent, base_name)
        os.makedirs(extract_path, exist_ok=True)
        logger.info(f"Extracting {archive_path}")
        try:
            patoolib.extract_archive(
                archive_path,
                outdir=extract_path,
                verbosity=-2,
                interactive=False,
                password=password
            )
        except PatoolError as e:
            raise RuntimeError(f"[EXCEPTION] Failed to extract {archive_path}: {e}")

        # Recurse into the new folder
        recursive_extract(
            extract_path,
            password=password,
            max_depth=max_depth,
            _current_depth=_current_depth + 1
        )

def post_process():
    """
    Runs post-extraction steps: merge files and split files.
    """
    try:
        logger.info("Merging files into single txt file...")
        merge_files.merge(EXTRACTION_PATH)
    except Exception as e:
        logger.error(f"Merge files failed: {e}")
        cleanup_extraction_path()
        return
    try:
        logger.info("Splitting files and sending them to AIL ...")
        splitter.split(file=f"./{EXTRACTION_PATH}/merged.txt",
                       chunk_size=CHUNK_SIZE,
                       api_key=API_KEY,
                       ail_url=AIL_URL,
                       uuid=UUID,
                       name=NAME,
                       wait=WAIT)
    except Exception as e:
        logger.error(f"Splitter failed: {e}")
        cleanup_extraction_path()
        return


@app.task
def process_file(file_path, optional_msg):
    """
    Processes a file: moves to storage, detects type, handles archives or text,
    extracts with or without password, then post-processes.

    Args:
        file_path (str): Path to the incoming file.
        optional_msg (str): Message containing password hints.
    """
    for path in (LOCAL_STORAGE, EXTRACTION_PATH):
        os.makedirs(path, exist_ok=True)

    logger.info(f"process_file: path={file_path}, msg={optional_msg}")
    try:

        # Move to local storage
        dest = os.path.join(LOCAL_STORAGE, os.path.basename(file_path))
        try:
            shutil.move(file_path, dest)
            logger.info(f"Moved to {dest}")
        except Exception as e:
            logger.error(f"Move failed: {e}")
            cleanup_extraction_path()
            return

        # Detect MIME type
        mime, _ = mimetypes.guess_type(dest)
        logger.info(f"mime_type={mime}")

        # Move file to extraction path
        try:
            shutil.copy(dest, EXTRACTION_PATH)
            logger.info(f"Copied to {EXTRACTION_PATH}")
        except Exception as e:
            logger.error(f"Copy failed: {e}")
            cleanup_extraction_path()
            return

        # Handle uncompressed text files
        if mime and mime.startswith('text'):
            logger.info("Uncompressed text detected, moving to post-process methods.")
            post_process()
            return

        # Archive handling
        candidates = extract_password_candidates(optional_msg)

        if not candidates:
            logger.info("No candidate passwords found, trying to decompress without password ...")
            if is_valid_password(dest, ""):
                logger.info("Using no password worker, extracting the archive ...")
                recursive_extract(EXTRACTION_PATH, None)
                post_process()
                return
            logger.error("Using no password didn't work - file ignored.")
            cleanup_extraction_path()
            return

        for candidate in candidates:
            if is_valid_password(dest, candidate):
                logger.info(f"Found a valid password -> {candidate}")
                recursive_extract(EXTRACTION_PATH, candidate)
                post_process()
                return
        
        logger.error("All candidate passwords failed - file ignored.")
        cleanup_extraction_path()
        return
    except Exception as e:
        logger.exception(f"Unexpected error in process_file: {e}")
        cleanup_extraction_path()
        return
