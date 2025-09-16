#!/usr/bin/env python3
"""
test_uuid_search_v1.py

Search FusionPBX CDRs for an exact UUID match the SAME WAY the xml_cdr app does,
then print who answered the call *and* the 10-digit external number involved:
- inbound: caller's 10-digit number
- outbound: dialed party's 10-digit number

If the first CDR row doesn't yield a reliable external number (or it's clearly
the agent/extension), we look up the peer leg using bridge_uuid and re-derive.

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

def digits_only(s: str | None) -> str:
    return re.sub(r"\D", "", s or "")

def normalize_10_digits(number_str: str | None) -> str | None:
    """Return the last 10 digits of the number, stripping non-digits.
    If fewer than 10 digits remain, return None.
    """
    if not number_str:
        return None
    digits = digits_only(number_str)
    if len(digits) < 10:
        return None
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

def get_record_by_xml_or_bridge(conn, xml_uuid: str | None = None, bridge_uuid: str | None = None):
    """Fetch a related/peer leg: either where xml_cdr_uuid = bridge_uuid, or where bridge_uuid = xml_uuid."""
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
        WHERE (%s IS NOT NULL AND c.xml_cdr_uuid = %s)
           OR (%s IS NOT NULL AND c.bridge_uuid = %s)
        ORDER BY c.start_stamp DESC
        LIMIT 1;
    """
    params = (bridge_uuid, bridge_uuid, xml_uuid, xml_uuid)
    with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
        cur.execute(sql, params)
        return cur.fetchone()

def derive_external_number(row) -> tuple[str | None, str]:
    """Return (external_10, source_hint) based on direction and fields in the given row."""
    direction = (row["direction"] or "").lower()
    if direction == "outbound":
        return normalize_10_digits(row["destination_number"]), "dest_from_outbound"
    else:
        # inbound/local: prefer caller_id_number
        return normalize_10_digits(row["caller_id_number"]), "cid_from_inbound"

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
        answered = bool(row["answer_stamp"]) or status == "answered"
        direction = (row["direction"] or "").lower()

        # First attempt on the direct match
        external_10, src_hint = derive_external_number(row)

        # If result is missing OR equals the agent's extension (bad), try peer leg.
        ext_digits = digits_only(ext or "")
        if (not external_10) or (external_10 == ext_digits[-10:] if len(ext_digits) >= 10 else False):
            # Determine peer using either bridge_uuid or the row's xml_cdr_uuid
            peer = None
            if row["bridge_uuid"]:
                peer = get_record_by_xml_or_bridge(conn, xml_uuid=None, bridge_uuid=row["bridge_uuid"])
            if not peer:
                peer = get_record_by_xml_or_bridge(conn, xml_uuid=row["xml_cdr_uuid"], bridge_uuid=None)
            if peer:
                external_10_peer, src_hint_peer = derive_external_number(peer)
                # Prefer a good peer value
                if external_10_peer and external_10_peer != (ext_digits[-10:] if len(ext_digits) >= 10 else None):
                    external_10, src_hint = external_10_peer, f"peer:{src_hint_peer}"

        # Determine "who"
        if answered and ext:
            who = f"{ext} {ext_name}".strip()
        elif answered:
            who = (row["caller_id_name"] or "Unknown").strip()
        else:
            who = status if status else "Unknown status"

        # Output
        print(f"answered_by: {who}")
        print(f"direction: {direction or 'unknown'}")
        if external_10:
            print(f"external_10_digit: {external_10}")
        else:
            # Provide raw numbers from both primary and (if looked up) peer rows for debugging
            raw_primary = row["destination_number"] if direction == "outbound" else row["caller_id_number"]
            print(f"external_number_raw: {raw_primary or 'unknown'}")
        # Include a hint for debugging/telemetry
        print(f"source_hint: {src_hint}")
    finally:
        try:
            conn.close()
        except Exception:
            pass

if __name__ == "__main__":
    main()
