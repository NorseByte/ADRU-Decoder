import sqlite3
from datetime import datetime
from pathlib import Path

import pandas as pd

from adru_utils import compute_md5_cached, count_msg_in_txt


def initialize_adru_database(db_path: Path, jru_attributes: list, etcs_attributes: list, dru_attributes: list):
    """
    Creates the ADRU database with the specified schema and inserts unique JRU and ETCS attributes
    as columns in their respective tables if the database does not already exist.

    Args:
        db_path (Path): Path to the SQLite database file
        jru_attributes (list): List of unique JRU attribute names
        etcs_attributes (list): List of unique ETCS attribute names
        dru_attributes (list): List of unique DRU attribute names
    """
    if db_path.exists():
        print("📦 Database already exists. No action taken.")
        return

    print("🛠️ Creating new database and tables...")

    with sqlite3.connect(db_path) as conn:
        cursor = conn.cursor()

        # Create adru_file table
        cursor.execute("""
            CREATE TABLE adru_file (
                af_id INTEGER PRIMARY KEY,
                af_name TEXT,
                af_md5_hash TEXT UNIQUE,
                af_created_at TIMESTAMP
            )
        """)

        # Create adru_message_file table
        cursor.execute("""
            CREATE TABLE adru_message_file (
                amf_id INTEGER PRIMARY KEY,
                amf_af_id INTEGER,
                amf_name TEXT,
                amf_md5_hash TEXT UNIQUE,
                amf_message_count INTEGER,
                amf_created_at TIMESTAMP,
                FOREIGN KEY (amf_af_id) REFERENCES adru_file (af_id)
            )
        """)

        # Create adru_messages table
        cursor.execute("""
            CREATE TABLE adru_messages (
                am_id INTEGER PRIMARY KEY,
                am_local_id INTEGER,
                am_amf_id INTEGER,
                FOREIGN KEY (am_amf_id) REFERENCES adru_message_file (amf_id)
            )
        """)

        # Create adru_message_jru table
        columns_jru = ",\n".join([f'"{attr}" TEXT' for attr in jru_attributes])
        cursor.execute(f"""
            CREATE TABLE adru_message_jru (
                amj_id INTEGER PRIMARY KEY,
                amj_am_id INTEGER,
                {columns_jru},
                FOREIGN KEY (amj_am_id) REFERENCES adru_messages (am_id)
            )
        """)

        # Create adru_message_etcs table
        columns_etcs = ",\n".join([f'"{attr}" TEXT' for attr in etcs_attributes])
        cursor.execute(f"""
            CREATE TABLE adru_message_etcs (
                ame_id INTEGER PRIMARY KEY,
                ame_am_id INTEGER,
                {columns_etcs},
                FOREIGN KEY (ame_am_id) REFERENCES adru_messages (am_id)
            )
        """)

        # Create dru_message_etcs table
        columns_dru = ",\n".join([f'"{attr}" TEXT' for attr in dru_attributes])
        cursor.execute(f"""
            CREATE TABLE adru_message_dru (
                amd_id INTEGER PRIMARY KEY,
                amd_am_id INTEGER,
                {columns_dru},
                FOREIGN KEY (amd_am_id) REFERENCES adru_messages (am_id)
            )
        """)

    print("✅ Database and tables created successfully.")


def add_message_file_to_db(db_path: Path, file_path: Path, adru_file_id: int) -> tuple[int, int]:
    """
    Check if a file exists in the database by MD5. If it does, return its af_id and message count.
    If not, compute total messages, insert it, and return both values.

    Args:
        db_path (Path): SQLite database path
        file_path (Path): Path to the input ADRU .txt file
        adru_file_id (int): The af_id from adru_file to associate with this message file

    Returns:
        tuple[int, int]: (amf_id, total_messages)
    """
    print(f"🧮 Calculating MD5 hash for file: {file_path.name} (size: {file_path.stat().st_size} bytes)")
    md5_hash = compute_md5_cached(file_path)

    with sqlite3.connect(db_path) as conn:
        cursor = conn.cursor()

        # Check for existing file
        cursor.execute("SELECT amf_id, amf_message_count FROM adru_message_file WHERE amf_md5_hash = ?", (md5_hash,))
        row = cursor.fetchone()
        if row:
            print(f"✅ File already exists in DB (af_id={row[0]})")
            return row[0], row[1]

        # If not found, count messages and insert
        total_messages = count_msg_in_txt(file_path)

        now = datetime.now().isoformat()
        cursor.execute("""
            INSERT INTO adru_message_file (amf_name, amf_md5_hash, amf_message_count, amf_created_at, amf_af_id)
            VALUES (?, ?, ?, ?, ?)
        """, (file_path.name, md5_hash, total_messages, now, adru_file_id))
        amf_id = cursor.lastrowid
        print(f"🆕 Added new file entry to DB with amf_id={amf_id}")
        return amf_id, total_messages


def add_adru_file_to_db(db_path: Path, file_path: Path) -> int:
    """
    Check if a file exists in the database by MD5. If it does, return its af_id and message count.
    If not, compute total messages, insert it, and return both values.

    Args:
        db_path (Path): SQLite database path
        file_path (Path): Path to the input ADRU file

    Returns:
        int: af_id
    """
    print(f"🧮 Calculating MD5 hash for file: {file_path.name} (size: {file_path.stat().st_size} bytes)")
    md5_hash = compute_md5_cached(file_path)

    with sqlite3.connect(db_path) as conn:
        cursor = conn.cursor()

        # Check for existing file
        cursor.execute("SELECT af_id FROM adru_file WHERE af_md5_hash = ?", (md5_hash,))
        row = cursor.fetchone()
        if row:
            print(f"✅ File already exists in DB (af_id={row[0]})")
            return row[0]

        # If not found, count messages and insert
        total_messages = count_msg_in_txt(file_path)

        now = datetime.now().isoformat()
        cursor.execute("""
            INSERT INTO adru_file (af_name, af_md5_hash, af_created_at)
            VALUES (?, ?, ?)
        """, (file_path.name, md5_hash, now))
        af_id = cursor.lastrowid
        print(f"🆕 Added new file entry to DB with af_id={af_id}")
        return af_id


def exist_txt_file_for_adru(adru_file_id: int, db_path: Path, output_folder: Path) -> bool:
    """
    Checks if a .txt file already exists in the output folder that matches a known MD5 hash
    for a given adru_file (via its associated adru_message_file entries).

    Args:
        adru_file_id (int): The af_id from adru_file
        db_path (Path): Path to the SQLite database
        output_folder (Path): Path to the output folder containing .txt files

    Returns:
        bool: True if a matching .txt file exists, False otherwise
    """
    print("🔍 Searching for existing .txt files for adru_file_id:", adru_file_id)

    with sqlite3.connect(db_path) as conn:
        cursor = conn.cursor()

        # Get all known txt file MD5 hashes for this adru file
        cursor.execute("""
            SELECT amf_md5_hash FROM adru_message_file WHERE amf_af_id = ?
        """, (adru_file_id,))
        known_hashes = {row[0] for row in cursor.fetchall()}

    # Check if any .txt file in the output folder matches
    for txt_file in Path(output_folder).glob("*.txt"):
        file_md5 = compute_md5_cached(txt_file)
        if file_md5 in known_hashes:
            return True

    return False


def get_all_adru_files_that_has_txt_files_and_latest_txt(db_path: Path) -> list[dict]:
    """
    Fetches all ADRU files that have associated .txt files in the output folder,
    returning a list of dicts containing file_id, file_name, and latest txt file path.

    Returns:
        list[dict]: [{ file_id, file_name, latest_txt_file }]
    """
    with sqlite3.connect(db_path) as conn:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT af.af_id, af.af_name
            FROM adru_file af
            WHERE af.af_id IN (
                SELECT DISTINCT amf_af_id FROM adru_message_file
            ) 
        """)
        rows = cursor.fetchall()

    print("🔍 Searching for adru files with text files...")

    results = []
    for adru_file_id, adru_file_name in rows:
        try:
            results.append({
                "file_id": adru_file_id,
                "file_name": adru_file_name,
            })
        except ValueError as e:
            print(f"⚠️ {e}")

    return results


def fetch_newest_txt_file_for_adru(adru_file_id: int, db_path: Path, output_folder: Path) -> tuple[int, Path, int, int]:
    """
    Fetches the newest known .txt file for a given ADRU file ID based on the latest amf_created_at timestamp
    in the adru_message_file table, and returns the matching file from the output folder.

    Raises:
        ValueError: If no known hashes exist or no matching file is found
    """
    with sqlite3.connect(db_path) as conn:
        cursor = conn.cursor()

        cursor.execute("""
            SELECT amf_md5_hash, amf_id, amf_message_count
            FROM adru_message_file
            WHERE amf_af_id = ?
            ORDER BY amf_created_at DESC
            LIMIT 1
        """, (adru_file_id,))
        row = cursor.fetchone()

    if not row:
        raise ValueError(f"No .txt file entries found in DB for adru_file_id {adru_file_id}")

    print(f"🔍 Searching for latest text file for adru_file_id: {adru_file_id}...")

    newest_md5 = row[0]

    for txt_file in Path(output_folder).glob("*.txt"):
        file_md5 = compute_md5_cached(txt_file)
        if file_md5 == newest_md5:
            return row[0], txt_file, row[2], row[1]  # Return (md5, path, message_count, amf_id)

    raise ValueError(f"No matching .txt file found in {output_folder} for newest MD5 {newest_md5}")


def is_txt_content_in_db_with_entries(db_path: Path, amf_id: int) -> bool:
    """
    Checks if a .txt file has already been processed and inserted into the database

    Args:
        db_path (Path): Path to the SQLite database.
        txt_path (Path): Path to the decoded .txt file.
        amf_id (int): ID from adru_message_file table for this txt file.
    """
    with sqlite3.connect(db_path) as conn:
        cursor = conn.cursor()

        cursor.execute("""
            SELECT am_id
            FROM adru_messages
            WHERE am_amf_id = ?
            LIMIT 1
        """, (amf_id,))
        row = cursor.fetchone()

    if not row:
        return False

    return True


def insert_messages_from_txt(txt_path: Path, db_path: Path, amf_id: int, total_messages: int):
    """
    Reads a decoded .txt file and inserts each Msg block into the database.

    Args:
        txt_path (Path): Path to the decoded .txt file.
        db_path (Path): Path to the SQLite database.
        amf_id (int): ID from adru_message_file table for this txt file.
        total_messages (int): Total number of messages in the file, used for progress tracking.
    """
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    current_msg_local_id = None
    current_jru_data = {}
    current_etcs_data = {}
    current_dru_data = {}
    inside_dru = False
    inside_jru = False
    inside_etcs = False
    inserted_count = 0  # Track inserted messages

    def insert_current_msg():
        nonlocal inserted_count
        if current_msg_local_id is None:
            return

        inserted_count += 1
        print(f"\r📝 Inserting message {inserted_count} of {total_messages}", end="")

        # Insert into adru_messages
        cursor.execute(
            "INSERT INTO adru_messages (am_local_id, am_amf_id) VALUES (?, ?)",
            (current_msg_local_id, amf_id)
        )
        am_id = cursor.lastrowid

        # Insert into adru_message_jru
        if current_jru_data:
            jru_cols = ", ".join(f'"{k}"' for k in current_jru_data.keys())
            jru_placeholders = ", ".join("?" for _ in current_jru_data)

            cursor.execute(
                f"INSERT INTO adru_message_jru (amj_am_id, {jru_cols}) VALUES (?, {jru_placeholders})",
                (am_id, *current_jru_data.values())
            )

        # Insert into adru_message_etcs
        if current_etcs_data:
            etcs_cols = ", ".join(f'"{k}"' for k in current_etcs_data.keys())
            etcs_placeholders = ", ".join("?" for _ in current_etcs_data)
            cursor.execute(
                f"INSERT INTO adru_message_etcs (ame_am_id, {etcs_cols}) VALUES (?, {etcs_placeholders})",
                (am_id, *current_etcs_data.values())
            )

        # Insert into adru_message_dru
        if current_dru_data:
            dru_cols = ", ".join(f'"{k}"' for k in current_dru_data.keys())
            dru_placeholders = ", ".join("?" for _ in current_dru_data)
            cursor.execute(
                f"INSERT INTO adru_message_dru (amd_am_id, {dru_cols}) VALUES (?, {dru_placeholders})",
                (am_id, *current_dru_data.values())
            )

    with txt_path.open("r", encoding="utf-8", errors="ignore") as f:
        for line in f:
            line = line.strip()

            # Message start
            if line.startswith("Msg "):
                insert_current_msg()
                current_msg_local_id = int(line.split(" ")[1].rstrip(":"))
                current_jru_data.clear()
                current_etcs_data.clear()
                current_dru_data.clear()
                inside_jru = False
                inside_etcs = False
                inside_dru = False
                continue

            if line == "DRU ETCS (":
                inside_dru = True
                continue

            if line == ")" and inside_dru:
                inside_dru = False
                continue

            if line == "JRU (":
                inside_jru = True
                continue

            if line == "ETCS ON-BOARD PROPRIETARY JURIDICAL DATA (":
                inside_etcs = True
                continue

            if line == ")" and inside_etcs:
                inside_etcs = False
                continue

            if line == ")" and inside_jru and not inside_etcs:
                inside_jru = False
                continue

            if ":" in line:
                parts = line.split(":", 1)
                if len(parts) == 2:
                    attr = parts[0].strip().split(maxsplit=1)[-1]
                    value = parts[1].strip()
                    if inside_etcs:
                        current_etcs_data[attr] = value
                    elif inside_jru:
                        current_jru_data[attr] = value
                    elif inside_dru:
                        current_dru_data[attr] = value

    # Insert final message
    insert_current_msg()

    conn.commit()
    conn.close()
    print("\n✅ All messages inserted successfully.")


def enrich_dataframe_with_db_values(df: pd.DataFrame, db_path: Path, adru_file_id: int) -> pd.DataFrame:
    """
    Enrich the given DataFrame with additional values from the SQLite database for matching messages,
    including JRU, ETCS, and DRU data.

    Args:
        df (pd.DataFrame): DataFrame read from CSV
        db_path (Path): Path to the SQLite database
        adru_file_id (int): The adru_file ID (af_id) that links to the messages

    Returns:
        pd.DataFrame: A new DataFrame with missing columns filled in from the database
    """
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # Get amf_id for this ADRU file
    cursor.execute("SELECT amf_id FROM adru_message_file WHERE amf_af_id = ?", (adru_file_id,))
    row = cursor.fetchone()
    if not row:
        print(f"❌ No amf_id found for adru_file_id {adru_file_id}")
        return df

    amf_id = row[0]
    print(f"🔗 Using amf_id {amf_id} for adru_file_id {adru_file_id}")

    all_new_columns = set()
    enriched_rows = []

    for i, (_, row) in enumerate(df.iterrows(), start=1):
        print(f"\r🔍 Enriching row {i} of {len(df)} (local_id (N°) {row['N°']})...", end="")
        local_id = row["N°"]

        # Start with existing CSV data
        enriched_data = row.to_dict()

        # Get message ID
        cursor.execute("""
            SELECT am_id FROM adru_messages WHERE am_local_id = ? AND am_amf_id = ?
        """, (local_id, amf_id))
        result = cursor.fetchone()

        if not result:
            enriched_rows.append(enriched_data)
            continue

        am_id = result[0]

        # JRU
        cursor.execute("SELECT * FROM adru_message_jru WHERE amj_am_id = ?", (am_id,))
        jru = cursor.fetchone()
        if jru:
            col_names = [desc[0] for desc in cursor.description]
            for col, val in zip(col_names, jru):
                if col not in ("amj_id", "amj_am_id"):
                    enriched_data[col] = val
                    all_new_columns.add(col)

        # ETCS
        cursor.execute("SELECT * FROM adru_message_etcs WHERE ame_am_id = ?", (am_id,))
        etcs = cursor.fetchone()
        if etcs:
            col_names = [desc[0] for desc in cursor.description]
            for col, val in zip(col_names, etcs):
                if col not in ("ame_id", "ame_am_id"):
                    enriched_data[col] = val
                    all_new_columns.add(col)

        # DRU
        cursor.execute("SELECT * FROM adru_message_dru WHERE amd_am_id = ?", (am_id,))
        dru = cursor.fetchone()
        if dru:
            col_names = [desc[0] for desc in cursor.description]
            for col, val in zip(col_names, dru):
                if col not in ("amd_id", "amd_am_id"):
                    enriched_data[col] = val
                    all_new_columns.add(col)

        enriched_rows.append(enriched_data)

    # Create DataFrame
    enriched_df = pd.DataFrame(enriched_rows)

    # Ensure all new columns are placed at the end
    ordered_columns = list(df.columns) + sorted(c for c in all_new_columns if c not in df.columns)
    enriched_df = enriched_df[ordered_columns]

    print("\n✅ All rows enriched.")
    conn.close()
    return enriched_df
