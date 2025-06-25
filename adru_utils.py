import hashlib
import threading
import time
from pathlib import Path
from typing import List

_md5_cache = {}


def compute_md5_cached(file_path: Path, chunk_size: int = 4 * 1024 * 1024) -> str:
    """
    Compute MD5 hash of a file in chunks.

    Args:
        file_path (Path): File path
        chunk_size (int): Size of each read chunk in bytes (default: 4MB)

    Returns:
        str: MD5 hex digest
    """
    if file_path in _md5_cache:
        return _md5_cache[file_path]

    hash_md5 = hashlib.md5()
    with file_path.open("rb") as f:
        for chunk in iter(lambda: f.read(chunk_size), b""):
            hash_md5.update(chunk)

    digest = hash_md5.hexdigest()
    _md5_cache[file_path] = digest
    return digest


def count_msg_in_txt(txt_path: Path) -> int:
    """
    Counts the number of 'Msg #' blocks in a .txt file and prints a live counter.

    Args:
        txt_path (Path): Path to the large .txt file.

    Returns:
        int: Total number of messages found
    """
    msg_count = 0

    with txt_path.open("r", encoding="utf-8", errors="ignore") as file:
        for line in file:
            if line.strip().startswith("Msg "):
                msg_count += 1
                print(f"\r🧮 Loading messages: {msg_count}", end="")

    print()  # newline after final count
    return msg_count


def extract_unique_attributes(txt_path: Path, total_messages: int) -> tuple[list[str], list[str], list[str]]:
    """
    Extracts all unique attribute names from JRU, ETCS, and DRU blocks in a decoded ADRU text file.

    Args:
        txt_path (Path): Path to the large .txt file.
        total_messages (int): Total number of messages to show progress while scanning

    Returns:
        tuple: (sorted list of JRU attribute names, sorted list of ETCS attribute names, sorted list of DRU attribute names)
    """
    unique_jru_attrs = set()
    unique_etcs_attrs = set()
    unique_dru_attrs = set()

    inside_jru = inside_etcs = inside_dru = False
    current_msg = 0

    with txt_path.open("r", encoding="utf-8", errors="ignore") as file:
        for line in file:
            line = line.strip()

            # Detect new message
            if line.startswith("Msg "):
                current_msg += 1
                print(f"\r🔍 Scanning message {current_msg} of {total_messages} for attributes", end="")
                inside_jru = inside_etcs = inside_dru = False
                continue

            # Section openers
            if line == "JRU (":
                inside_jru, inside_etcs, inside_dru = True, False, False
                continue
            elif line == "ETCS ON-BOARD PROPRIETARY JURIDICAL DATA (":
                inside_jru, inside_etcs, inside_dru = False, True, False
                continue
            elif line == "DRU ETCS (":
                inside_jru, inside_etcs, inside_dru = False, False, True
                continue

            # Section closers
            if line == ")" and (inside_jru or inside_etcs or inside_dru):
                inside_jru = inside_etcs = inside_dru = False
                continue

            # Detect correct delimiter by order of appearance
            first_colon = line.find(":")
            first_equal = line.find("=")

            if first_colon == -1 and first_equal == -1:
                continue  # no known delimiter

            if first_equal != -1 and (first_colon == -1 or first_equal < first_colon):
                # '=' comes first
                parts = line.split("=", 1)
            else:
                # ':' comes first
                parts = line.split(":", 1)

            if len(parts) == 2:
                attr_name = parts[0].strip()
                if inside_jru:
                    unique_jru_attrs.add(attr_name)
                elif inside_etcs:
                    unique_etcs_attrs.add(attr_name)
                elif inside_dru:
                    unique_dru_attrs.add(attr_name)

    print()  # newline after progress bar
    return sorted(unique_jru_attrs), sorted(unique_etcs_attrs), sorted(unique_dru_attrs)


def find_adru_files(input_dir: str) -> List[Path]:
    """
    Scans the given directory for .adru files and returns a list of their Paths.

    Args:
        input_dir (str): Directory to scan

    Returns:
        List[Path]: List of .adru file paths
    """
    input_path = Path(input_dir)
    if not input_path.exists() or not input_path.is_dir():
        print(f"❌ Input directory does not exist: {input_dir}")
        return []

    adru_files = list(input_path.glob("*.adru"))
    print(f"📂 Found {len(adru_files)} .adru files in {input_dir}")
    return adru_files


def get_latest_txt_file(directory: Path) -> Path | None:
    txt_files = list(directory.glob("*.txt"))
    if not txt_files:
        return None
    return max(txt_files, key=lambda f: f.stat().st_ctime)


def prompt_user_and_wait_for_txt(cmd_str: str, output_dir: Path, timeout=32000) -> Path:
    print("\n🚀 Please run the following command in a terminal:\n")
    print(cmd_str)
    print("\n⏳ Waiting for a .txt file to be finalized in:", output_dir)
    print("⌨️  When done, type 'g' and press Enter to continue.\n")

    latest_txt_file = get_latest_txt_file(output_dir)
    if not latest_txt_file:
        print("⚠️ No .txt file found yet. Please ensure the process generates one.")

    # Start timer print loop
    start_time = time.time()

    def print_timer():
        while not stop_event.is_set():
            elapsed = int(time.time() - start_time)
            print(f"\r⏱️ Elapsed: {elapsed}s | Waiting for user confirmation (press 'g' and then enter quick)...",
                  end="")
            time.sleep(1)

    stop_event = threading.Event()
    timer_thread = threading.Thread(target=print_timer)
    timer_thread.start()

    try:
        while True:
            user_input = input().strip().lower()
            if user_input == "g":
                break
    finally:
        stop_event.set()
        timer_thread.join()

    latest_txt_file = get_latest_txt_file(output_dir)
    if not latest_txt_file:
        raise ValueError("❌ No .txt file found after confirmation.")

    print(f"\n✅ Confirmed. Using latest .txt file: {latest_txt_file.name}")
    return latest_txt_file
