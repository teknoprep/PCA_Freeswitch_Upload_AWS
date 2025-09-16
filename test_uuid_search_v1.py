#!/usr/bin/env python3
"""
test_uuid_search_v1.py

Search FusionPBX CDRs for an exact UUID match the SAME WAY the xml_cdr app does,
then print who answered the call *and* the 10-digit external number involved:
- inbound: caller's 10-digit number
- outbound: dialed party's 10-digit number

Usage:
  python3 test_uuid_search_v1.py --uuid <UUID_STRING>

Requires:
  - psycopg2 (pip install psycopg2-binary)
  - python-dotenv (pip install python-dotenv)
"""

import argparse
import sys
import os
import re
import psycopg2
import psycopg2.extras
from dotenv import load_dotenv

# Load .env file from current directory
load_dotenv()

DB_HOST = os.getenv("DB_HOST")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASS = os.getenv("DB_PASS")

def normalize_10_digits(number_str: str | None) -> str | None:
    """Return the last 10 digits of the number, stripping non-digits.
    If fewer than 10 digits remain, return None.
    """
    if not number_str:
        return None
    import re as _re
    digits = _re.sub(r"\D", "", number_str)
    if len(digits) < 10:
        return None
    # Use the last 10 digits to accommodate country code or prefixes
    return digits[-10:]

def get_record_by_uuid(conn, uuid_str):
    """
    Mimic FusionPBX xml_cdr 'UUID' search:
    WHERE c.xml_cdr_uuid = :xml_cdr_uuid  (exact match)
    """
    sql = """
        SELECT
            c.xml_cdr_uuid,
            c.bridge_uuid,
            c.direction,
            c.status,
            c.answer_stamp,
            c.start_stamp,
            c.end_stamp,
            c.caller_id_name,
            c.caller_id_number,
            c.destination_number,
            c.extension_uuid,
            e.extension,
            e.effective_caller_id_name AS extension_name
        FROM v_xml_cdr AS c
        LEFT JOIN v_extensions AS e ON e.extension_uuid = c.extension_uuid
        WHERE c.xml_cdr_uuid = %s
        ORDER BY c.start_stamp DESC
        LIMIT 1;
    """
    with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
        cur.execute(sql, (uuid_str,))
        return cur.fetchone()

def main():
    parser = argparse.ArgumentParser(
        description="Search FusionPBX CDRs by UUID (exact match) and print who answered + external 10-digit number."
    )
    parser.add_argument("--uuid", required=True, help="UUID to search (exact match on xml_cdr_uuid)")
    args = parser.parse_args()

    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASS,
            connect_timeout=5,
            application_name="test_uuid_search_v1.py",
        )
    except Exception as e:
        print(f"ERROR: could not connect to database: {e}", file=sys.stderr)
        sys.exit(2)

    try:
        row = get_record_by_uuid(conn, args.uuid)
        if not row:
            print("No CDR found for that UUID (xml_cdr_uuid exact match).")
            sys.exit(1)

        status = (row["status"] or "").lower()
        ext = row["extension"]
        ext_name = row["extension_name"]
        direction = (row["direction"] or "").lower()
        answered = bool(row["answer_stamp"]) or status == "answered"

        # Determine external party number based on direction
        if direction == "outbound":
            external_raw = row["destination_number"]
        else:
            # treat anything else (inbound, local, internal) as inbound for external number purposes
            external_raw = row["caller_id_number"]
        external_10 = normalize_10_digits(external_raw)

        # Output
        if answered and ext:
            who = f"{ext} {ext_name}".strip()
        elif answered:
            who = (row["caller_id_name"] or "Unknown").strip()
        else:
            who = status if status else "Unknown status"

        # Print simple machine-friendly lines
        print(f"answered_by: {who}")
        print(f"direction: {direction or 'unknown'}")
        if external_10:
            print(f"external_10_digit: {external_10}")
        else:
            # Provide the raw in case normalization failed
            raw_display = external_raw if external_raw else "unknown"
            print(f"external_number_raw: {raw_display}")
    finally:
        try:
            conn.close()
        except Exception:
            pass

if __name__ == "__main__":
    main()
