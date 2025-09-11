#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
ai_copy_s3.py  —  end-to-end scanner → rename plan → S3 upload → Step Functions

Usage examples:
  # fresh run (reads .env), writes plan + uploads, triggers Step Functions
  ./ai_copy_s3.py --domain leemyles.blueuc.com

  # dry run (no uploads / no Step Functions)
  ./ai_copy_s3.py --domain leemyles.blueuc.com --dry-run

  # resume from previous state for that domain (ignores .env knobs where applicable)
  ./ai_copy_s3.py --domain leemyles.blueuc.com --resume

  # test with just ONE file (first eligible never previously one-file-tested)
  ./ai_copy_s3.py --domain leemyles.blueuc.com --one-file-test

  # scan ONLY an explicit date range (inclusive), ignore resume/seed window
  ./ai_copy_s3.py --domain leemyles.blueuc.com --date-range 2023-01-01:2023-01-31

  # force uploads to bucket root even if S3_KEY_PREFIX is set / saved
  ./ai_copy_s3.py --domain leemyles.blueuc.com --no-prefix

───────────────────────────────────────────────────────────────────────────────
CLI OPTIONS (documented)

  --domain <name>        (required) FusionPBX domain, e.g. leemyles.blueuc.com
  --dry-run              Do everything except upload to S3 and start Step Functions
  --resume               Resume using saved state for this domain. This includes
                        using the saved config snapshot (S3_KEY_PREFIX, etc.).
  --state <path>         Override state JSON path (default ./out/state_<domain>.json)
  --plan  <path>         Override rename plan JSON output path
  --one-file-test        Upload ONLY the first eligible file found this run. Ignores
                        resume timing; scans the last INITIAL_SEED_DAYS window unless
                        --date-range is provided.
                        • Skips files already uploaded (in any mode)
                        • Skips files previously uploaded in one-file-test mode
                          (tracked in state.one_file_test_history)
                        • Stops scanning immediately after the first upload
                          (or the first "would upload" in --dry-run)
  --date-range A:B       Limit scanning to an explicit inclusive date range where
                        A and B are YYYY-MM-DD. Example: 2024-01-01:2024-01-31.
                        Overrides the initial seed window and --resume timing.
                        When provided, the script DOES NOT update last_run_time_utc
                        in the state file (so future runs are unaffected).
  --no-prefix            Ignore S3_KEY_PREFIX from env/saved state and upload to
                        the bucket root for this run.

───────────────────────────────────────────────────────────────────────────────
CONFIGURATION (via .env or env.py or environment variables)

  DB_HOST (default 127.0.0.1)
  DB_PORT (default 5432)
  DB_NAME (default fusionpbx)
  DB_USER (default postgres)
  DB_PASS (default "")

  FREESWITCH_RECORDING_PATH (default /usr/local/freeswitch/recordings)
  AUDIO_EXTS (default ".wav,.mp3")
  MIN_FILE_LENGTH_SECONDS (default 15)
  RECORD_RETENTION_DAYS (default 30)   # prune upload history entries older than this
  INITIAL_SEED_DAYS (default 5)        # first run scan window when no state exists
  UUID_REGEX (default RFC-4122-style 36-char regex)

  AGENT_UPLOAD_FILTER_ARRAY (default empty)  # comma-separated allowlist of agent
                                            # extensions. Empty means allow all.

  S3_BUCKET_NAME (required for uploads)
  S3_REGION_NAME (default us-east-1)
  S3_KEY_PREFIX (default empty)  # optional path prefix in the bucket

  STEP_FUNCTION_ARN    (optional)  # if set, triggered when there are uploads
  STEP_FUNCTION_REGION (default S3_REGION_NAME)

  COMPUTE_MD5 (default False)  # if True, compute and send Content-MD5 on upload

  PLAN_OUT_DIR  (default ./out) # where rename plan JSONs go
  STATE_OUT_DIR (default ./out) # where state JSONs go

Notes:
• --resume honors the previously saved config snapshot in state_<domain>.json.
• --date-range A:B ignores both the initial seed window and resume timing.
• --no-prefix affects only the current run and is NOT saved to state.
• Deterministic scanning order: dates ascending, folders ascending, files ascending.
"""

import os
import re
import sys
import json
import time
import hashlib
import argparse
import base64
from typing import Any, Dict, List, Optional, Tuple
from datetime import datetime, date, timedelta, timezone
from decimal import Decimal

# -------------------- Optional .env loader --------------------
ENV_LOADED = False
try:
    from dotenv import load_dotenv
    load_dotenv()
    ENV_LOADED = True
except Exception:
    pass

# Fallback env.py support (define variables in env.py as module-level names)
try:
    import env as env_mod  # optional file
except Exception:
    env_mod = None

def env_str(name: str, default: Optional[str] = None) -> Optional[str]:
    if os.getenv(name) is not None:
        return os.getenv(name)
    if env_mod and hasattr(env_mod, name):
        v = getattr(env_mod, name)
        if v is not None:
            return str(v)
    return default

def env_int(name: str, default: int) -> int:
    v = env_str(name, None)
    if v is None or str(v).strip() == "":
        return default
    try:
        return int(str(v).strip())
    except Exception:
        return default

def env_bool(name: str, default: bool) -> bool:
    v = env_str(name, None)
    if v is None:
        return default
    s = str(v).strip().lower()
    return s in ("1", "true", "t", "yes", "y", "on")

# -------------------- Dependencies --------------------
try:
    import boto3
except ImportError:
    print("Missing dependency: boto3  (pip install boto3)")
    sys.exit(1)

try:
    import psycopg2
    import psycopg2.extras
except ImportError:
    print("Missing dependency: psycopg2-binary  (pip install psycopg2-binary)")
    sys.exit(1)

try:
    from mutagen.mp3 import MP3
    from mutagen.wave import WAVE
except Exception:
    print("Missing dependency: mutagen  (pip install mutagen)")
    sys.exit(1)

# -------------------- Config from .env / env.py --------------------
# DB
DB_HOST = env_str("DB_HOST", "127.0.0.1")
DB_PORT = env_int("DB_PORT", 5432)
DB_NAME = env_str("DB_NAME", "fusionpbx")
DB_USER = env_str("DB_USER", "postgres")
DB_PASS = env_str("DB_PASS", "")

# FS paths & scanning
FREESWITCH_RECORDING_PATH = env_str("FREESWITCH_RECORDING_PATH", "/usr/local/freeswitch/recordings")
AUDIO_EXTS = [s.strip().lower() for s in (env_str("AUDIO_EXTS", ".wav,.mp3") or "").split(",") if s.strip()]
MIN_FILE_LENGTH_SECONDS = env_int("MIN_FILE_LENGTH_SECONDS", 15)
RECORD_RETENTION_DAYS = env_int("RECORD_RETENTION_DAYS", 30)    # for pruning upload history
INITIAL_SEED_DAYS = env_int("INITIAL_SEED_DAYS", 5)             # first run window
UUID_REGEX = env_str("UUID_REGEX", r"[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}")

# Agent filtering (optional, empty = allow all)
AGENT_UPLOAD_FILTER_ARRAY = [s.strip() for s in (env_str("AGENT_UPLOAD_FILTER_ARRAY", "") or "").split(",") if s.strip()]

# S3
S3_BUCKET_NAME = env_str("S3_BUCKET_NAME", "")
S3_REGION_NAME = env_str("S3_REGION_NAME", "us-east-1")
S3_KEY_PREFIX = env_str("S3_KEY_PREFIX", "")  # optional

# Step Functions
STEP_FUNCTION_ARN = env_str("STEP_FUNCTION_ARN", "")
STEP_FUNCTION_REGION = env_str("STEP_FUNCTION_REGION", S3_REGION_NAME)

# Misc
COMPUTE_MD5 = env_bool("COMPUTE_MD5", False)  # if True, compute Content-MD5 for uploads (slower)
PLAN_OUT_DIR = env_str("PLAN_OUT_DIR", "./out")  # where rename plan JSONs go
STATE_OUT_DIR = env_str("STATE_OUT_DIR", "./out") # where state JSONs go

# -------------------- Helpers --------------------
def connect_db():
    dsn = f"host={DB_HOST} port={DB_PORT} dbname={DB_NAME} user={DB_USER} password={DB_PASS}"
    return psycopg2.connect(dsn)

def s3_client(region: str):
    return boto3.client("s3", region_name=region, config=boto3.session.Config(signature_version='s3v4'))

def step_client(region: str):
    return boto3.client("stepfunctions", region_name=region)

def make_json_safe(obj):
    if isinstance(obj, dict):
        return {k: make_json_safe(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [make_json_safe(v) for v in obj]
    if isinstance(obj, tuple):
        return [make_json_safe(v) for v in obj]
    if isinstance(obj, set):
        return [make_json_safe(v) for v in list(obj)]
    if isinstance(obj, Decimal):
        try:
            if obj == obj.to_integral_value():
                return int(obj)
        except Exception:
            pass
        return float(obj)
    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    return obj

def digits_only(s: Optional[str]) -> str:
    return "".join(ch for ch in (s or "") if ch.isdigit())

def clean_number_keep_10_or_11(num: Optional[str]) -> Optional[str]:
    d = digits_only(num)
    if len(d) in (10, 11):
        return d
    if len(d) > 11:
        last11 = d[-11:]
        if last11.startswith("1"):
            return last11
        return d[-10:]
    return None

def normalize_filename_piece(s: str) -> str:
    return re.sub(r'[^a-zA-Z0-9_.-]', '', s or '')

def fmt_filename_datetime(dt: Optional[datetime]) -> str:
    if not dt:
        return "UNKNOWN"
    try:
        iso = dt.astimezone(timezone.utc).isoformat(timespec="seconds")
    except Exception:
        iso = dt.isoformat(timespec="seconds")
    iso = iso.replace("+00:00", "")
    if "T" in iso:
        d, t = iso.split("T", 1)
        t = t.split(".", 1)[0]
        t = t.replace(":", "-")
        return f"{d}T{t}"
    return iso.replace(":", "-")

def build_precise_s3_name(domain: str, cust_digits: Optional[str], uuid: str, agent_ext: Optional[str], dt: Optional[datetime], ext: str) -> str:
    dom = normalize_filename_piece(domain)
    cust = cust_digits if (cust_digits and len(cust_digits) in (10, 11) and cust_digits.isdigit()) else "UNKNOWN"
    agent = agent_ext if agent_ext else "UNKNOWN"
    when = fmt_filename_datetime(dt)
    file_ext = ext if ext.startswith(".") else f".{ext}" if ext else ""
    return f"{dom}_CUST_{cust}_GUID_{uuid}_AGENT_{agent}_DATETIME_{when}{file_ext}"

def md5_of_file(path: str) -> str:
    h = hashlib.md5()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()

# -------------------- DB Queries --------------------
def get_columns(cur, table_name: str) -> set:
    cur.execute("""
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema IN ('public')
          AND table_name = %s
    """, (table_name,))
    cols = set()
    for row in cur.fetchall():
        if isinstance(row, dict):
            cols.add(row.get("column_name"))
        else:
            cols.add(row[0])
    return {c for c in cols if c}

def resolve_domain_uuid(cur, domain: str) -> Optional[str]:
    cur.execute("SELECT domain_uuid FROM v_domains WHERE domain_name = %s LIMIT 1", (domain,))
    row = cur.fetchone()
    if not row: return None
    return row["domain_uuid"] if isinstance(row, dict) else row[0]

def fetch_valid_extensions(cur, domain_uuid: str) -> List[str]:
    cur.execute("""
        SELECT extension, number_alias, enabled
        FROM v_extensions
        WHERE domain_uuid = %s
    """, (domain_uuid,))
    out = []
    for r in cur.fetchall():
        ext = (r["extension"] if isinstance(r, dict) else r[0]) or ""
        ena = (r["enabled"] if isinstance(r, dict) else r[2])
        ena_s = str(ena).lower()
        if ena in (True, 1) or ena_s in ("true", "t", "1", "yes", "y"):
            if ext and ext.isdigit():
                out.append(ext)
    return sorted(set(out))

def make_select_clause(cols_present: set) -> str:
    # add 'extension' first – FusionPBX populates this with the answered user/extension shown in the UI
    candidates = [
        "extension", "extension_uuid",
        "caller_id_name","caller_id_number","destination_number","direction",
        "start_stamp","end_stamp","billsec","answer_stamp","hangup_cause",
        "accountcode","domain_uuid","record_name","record_path",
        "last_app","last_arg","presence_id","bridge_uuid","bleg_uuid",
        "call_flow","context","user_context","caller_destination",
        "variable_bridge_id","variable_sip_to_user","variable_sip_from_user",
        "variables","json","raw_json","call_json"
    ]
    actual = [c for c in candidates if c in cols_present]
    return ", ".join(actual) if actual else "*"


def fetch_cdr_by_uuid(cur, uuid: str, cols_present: set, uuid_col: Optional[str]):
    select_clause = make_select_clause(cols_present)

    # Prefer xml_cdr_uuid if present, then uuid, then call_uuid
    for cand in ["xml_cdr_uuid", "uuid", "call_uuid"]:
        if uuid_col == cand:
            sql = f"SELECT {select_clause} FROM v_xml_cdr WHERE {cand} = %s LIMIT 1"
            params = (uuid,)
            cur.execute(sql, params)
            row = cur.fetchone()
            if row:
                return (" ".join(sql.split()), list(params), row, "uuid_col")

    # Fallback: try record_name/record_path LIKE
    like_cols = [c for c in ("record_name", "record_path") if c in cols_present]
    if like_cols:
        conds = " OR ".join([f"{c} ILIKE %s" for c in like_cols])
        sql = f"SELECT {select_clause} FROM v_xml_cdr WHERE {conds} ORDER BY start_stamp DESC NULLS LAST LIMIT 1"
        needle = f"%{uuid}%"
        params = tuple(needle for _ in like_cols)
        cur.execute(sql, params)
        row = cur.fetchone()
        if row:
            return (" ".join(sql.split()), list(params), row, "fallback_like")

    # Not found
    if uuid_col:
        sql = f"SELECT {select_clause} FROM v_xml_cdr WHERE {uuid_col} = %s LIMIT 1"
        params = (uuid,)
    else:
        sql = f"SELECT {select_clause} FROM v_xml_cdr /* no uuid-like columns; no fallback */ LIMIT 0"
        params = ()
    return (" ".join(sql.split()), list(params), None, "not_found")

# -------------------- Agent & CUST detection --------------------
def stringify_cdr_row(cdr: Dict[str, Any]) -> Tuple[str, List[Dict[str, str]]]:
    parts, locs = [], []
    for k, v in cdr.items():
        if v is None:
            continue
        if isinstance(v, (datetime, date)):
            s = v.isoformat()
        elif isinstance(v, (int, float, Decimal)):
            s = str(v)
        else:
            s = str(v)
        if not s:
            continue
        parts.append(f"{k}={s}")
        locs.append({"field": k, "snippet": s[:500]})
    return "\n".join(parts), locs

def find_agent_anywhere(cdr: Dict[str, Any], valid_exts: set) -> Tuple[Optional[str], List[Dict[str, str]], str]:
    blob, locs = stringify_cdr_row(cdr)
    # Use ALPHANUMERIC boundaries so we don't match inside UUIDs/SIP URIs
    for ext in sorted(valid_exts, key=len, reverse=True):
        pat = re.compile(rf"(?<![0-9A-Za-z]){re.escape(ext)}(?![0-9A-Za-z])")
        if pat.search(blob):
            hits: List[Dict[str, str]] = []
            source_field = "any"
            for L in locs:
                if pat.search(L["snippet"]):
                    hits.append({"field": L["field"], "context": L["snippet"][:200]})
                    source_field = L["field"]
                    if len(hits) >= 3:
                        break
            return ext, hits, source_field
    return None, [], ""

def fast_pick_agent_from_seed_row(cdr: Dict[str, Any], valid_exts: set) -> Tuple[Optional[str], str]:
    """
    Minimal, fast agent detector that mirrors FusionPBX XML CDR UI:
    1) Use v_xml_cdr.extension if it exists and is a valid extension.
    2) Fallbacks that are still single-field and cheap.
    Returns (agent_ext, source_field)
    """
    def as_str(x): return ("" if x is None else str(x)).strip()

    # 1) primary: 'extension' column from v_xml_cdr (what the UI shows)
    ext_col = as_str(cdr.get("extension"))
    if ext_col and ext_col in valid_exts:
        return ext_col, "extension"

    # 2) accountcode sometimes set to the answering extension
    acc = as_str(cdr.get("accountcode"))
    if acc and acc in valid_exts:
        return acc, "accountcode"

    # 3) presence_id like "103@domain" -> 103
    pres = as_str(cdr.get("presence_id"))
    if pres:
        m = re.match(r"(\d+)(?:@|$)", pres)
        if m and m.group(1) in valid_exts:
            return m.group(1), "presence_id"

    # 4) SIP users captured on the leg
    sip_to = as_str(cdr.get("variable_sip_to_user"))
    if sip_to and sip_to in valid_exts:
        return sip_to, "variable_sip_to_user"
    sip_from = as_str(cdr.get("variable_sip_from_user"))
    if sip_from and sip_from in valid_exts:
        return sip_from, "variable_sip_from_user"

    # 5) Directional single-field fallback (cheap, no full-text scans)
    direction = as_str(cdr.get("direction")).lower()
    cid = as_str(cdr.get("caller_id_number"))
    dst = as_str(cdr.get("destination_number"))

    if direction in ("inbound", "local"):
        if dst in valid_exts:
            return dst, "destination_number"
        if cid in valid_exts:
            return cid, "caller_id_number"
    elif direction == "outbound":
        if cid in valid_exts:
            return cid, "caller_id_number"
        if dst in valid_exts:
            return dst, "destination_number"

    # no fast match
    return None, ""



def is_uuidish(val: Optional[str]) -> bool:
    try:
        s = str(val or "").strip()
        if not s:
            return False
        return re.fullmatch(UUID_REGEX, s) is not None
    except Exception:
        return False
      
def collect_extension_hits(cdr: Dict[str, Any], valid_exts: set) -> Dict[str, List[str]]:
    """
    field_name -> sorted list of valid extensions found in that field's value.
    Uses ALPHANUMERIC boundaries so '103' won't match inside hex UUIDs, etc.
    """
    hits: Dict[str, set] = {}
    for k, v in cdr.items():
        if v is None:
            continue
        s = str(v)
        if not s:
            continue
        field_hits = set()
        for ext in valid_exts:
            pat = re.compile(rf"(?<![0-9A-Za-z]){re.escape(ext)}(?![0-9A-Za-z])")
            if pat.search(s):
                field_hits.add(ext)
        if field_hits:
            hits[k] = field_hits
    return {k: sorted(list(v), key=lambda x: (len(x), x)) for k, v in hits.items()}


  
def fetch_related_cdr_rows(cur,
                           seed_row: Dict[str, Any],
                           cols_present: set,
                           uuid_col: Optional[str]) -> List[Dict[str, Any]]:
    """
    Given one CDR row, collect likely-related rows by chasing bridge UUIDs.
    Returns a list of DISTINCT rows including the seed.
    """
    related: List[Dict[str, Any]] = []
    seen_keys: set = set()

    def add_row(r: Dict[str, Any]):
        # Build a stable identity key from available UUID-like columns so we don't duplicate
        key_vals = []
        for c in ("xml_cdr_uuid", "uuid", "call_uuid", "bridge_uuid", "bleg_uuid", "variable_bridge_id"):
            if c in r and r.get(c):
                key_vals.append(str(r.get(c)))
        key = "|".join(key_vals) or str(id(r))
        if key not in seen_keys:
            related.append(r)
            seen_keys.add(key)

    # Always include the seed
    add_row(seed_row)

    # Collect candidate bridged UUIDs from the seed
    bridge_keys = []
    for k in ("bridge_uuid", "bleg_uuid", "variable_bridge_id"):
        v = seed_row.get(k)
        if is_uuidish(v):
            bridge_keys.append((k, str(v)))

    if not bridge_keys:
        return related

    # Build SELECT for the same subset we already use
    select_clause = make_select_clause(cols_present)

    # For each bridged UUID, try to fetch a row directly via the configured uuid column,
    # and also try each of the common CDR uuid-ish columns.
    for _, buid in bridge_keys:
        found = False
        for cand in ["xml_cdr_uuid", "uuid", "call_uuid", "bridge_uuid", "bleg_uuid", "variable_bridge_id"]:
            if cand not in cols_present:
                continue
            sql = f"SELECT {select_clause} FROM v_xml_cdr WHERE {cand} = %s ORDER BY start_stamp DESC NULLS LAST LIMIT 1"
            cur.execute(sql, (buid,))
            r = cur.fetchone()
            if r:
                add_row(r)
                found = True
                break

        # Last ditch: look for filename match if columns exist
        if not found:
            like_cols = [c for c in ("record_name", "record_path") if c in cols_present]
            if like_cols:
                conds = " OR ".join([f"{c} ILIKE %s" for c in like_cols])
                sql = f"SELECT {select_clause} FROM v_xml_cdr WHERE {conds} ORDER BY start_stamp DESC NULLS LAST LIMIT 1"
                needle = f"%{buid}%"
                params = tuple(needle for _ in like_cols)
                cur.execute(sql, params)
                r = cur.fetchone()
                if r:
                    add_row(r)

    return related

def pick_answered_agent_from_rows(rows: List[Dict[str, Any]],
                                  valid_exts: set) -> Tuple[Optional[str], str, Dict[str, Any]]:
    """
    From related rows (a/b legs), choose the answered leg and extract the agent extension.
    Returns (agent_ext, source_field, chosen_row). Prints detailed DEBUG lines.
    """
    def answered_key(r: Dict[str, Any]) -> Tuple[int, int]:
        bill = int(r.get("billsec") or 0)
        ans_present = 1 if r.get("answer_stamp") else 0
        return (ans_present, 1 if bill > 0 else 0)

    rows_sorted = sorted(rows, key=answered_key, reverse=True)

    for idx, r in enumerate(rows_sorted, 1):
        direction = str(r.get("direction") or "").lower()
        cid = str(r.get("caller_id_number") or "")
        dst = str(r.get("destination_number") or "")
        print(f"[DEBUG] CANDIDATE LEG #{idx}  direction={direction}  billsec={r.get('billsec')}  answer_stamp={r.get('answer_stamp')}  cid={cid}  dst={dst}")

        hits = collect_extension_hits(r, valid_exts)
        if hits:
            print("[DEBUG]   extension hits per field:")
            preferred = [
                "presence_id", "variable_sip_to_user", "variable_sip_from_user",
                "destination_number", "caller_id_number",
                "caller_destination", "last_arg", "context", "user_context",
                "variables", "json", "raw_json", "call_json"
            ]
            printed = set()
            for f in preferred:
                if f in hits:
                    print(f"    - {f}: {', '.join(hits[f])}")
                    printed.add(f)
            for f in sorted(hits.keys()):
                if f not in printed:
                    print(f"    - {f}: {', '.join(hits[f])}")
        else:
            print("[DEBUG]   no single-field hits")

        pres = str(r.get("presence_id") or "")
        if "@" in pres:
            ext = pres.split("@", 1)[0]
            if ext in valid_exts:
                print(f"[DEBUG]   picked agent via presence_id: {ext}")
                return ext, "presence_id", r

        to_user = str(r.get("variable_sip_to_user") or "")
        from_user = str(r.get("variable_sip_from_user") or "")
        if direction == "inbound":
            if to_user in valid_exts:
                print(f"[DEBUG]   picked agent via variable_sip_to_user (inbound): {to_user}")
                return to_user, "variable_sip_to_user", r
            if dst in valid_exts:
                print(f"[DEBUG]   picked agent via destination_number (inbound): {dst}")
                return dst, "destination_number", r
        elif direction == "outbound":
            if from_user in valid_exts:
                print(f"[DEBUG]   picked agent via variable_sip_from_user (outbound): {from_user}")
                return from_user, "variable_sip_from_user", r
            if cid in valid_exts:
                print(f"[DEBUG]   picked agent via caller_id_number (outbound): {cid}")
                return cid, "caller_id_number", r

        for f in ("variable_sip_to_user", "variable_sip_from_user", "destination_number", "caller_id_number"):
            v = str(r.get(f) or "")
            if v in valid_exts:
                print(f"[DEBUG]   picked agent via {f}: {v}")
                return v, f, r

        print("[DEBUG]   did not identify agent on this leg; trying next")

    print("[DEBUG] No agent extension matched on any related rows.")
    return None, "", rows_sorted[0] if rows_sorted else ({},)


def prefer_external_number(caller_id_number: Optional[str], destination_number: Optional[str], agent_ext: Optional[str]) -> Tuple[str, str]:
    cid = digits_only(caller_id_number)
    dst = digits_only(destination_number)
    if agent_ext and cid == agent_ext and dst and dst != agent_ext:
        return agent_ext, dst
    if agent_ext and dst == agent_ext and cid and cid != agent_ext:
        return agent_ext, cid
    cand = None
    if len(cid) >= 10 and cid != agent_ext:
        cand = cid
    if len(dst) >= 10 and dst != agent_ext:
        if cand is None or len(dst) > len(cand):
            cand = dst
    if cand is None:
        cand = dst if agent_ext and cid == agent_ext else cid
    return agent_ext or "", cand or ""

CUST_FIELD_PRIORITY = [
    "caller_id_number",
    "destination_number",
    "variable_sip_from_user",
    "variable_sip_to_user",
    "caller_destination",
    "presence_id",
    "context",
    "last_arg",
    "variables",
    "json",
    "raw_json",
    "call_json",
]

def find_cust_digits_anywhere(cdr: Dict[str, Any], agent_ext: Optional[str], valid_exts: set) -> Tuple[Optional[str], Optional[str]]:
    """
    Return (cust_digits, source_field) by scanning many CDR string fields for a 10/11 digit number
    that is NOT the agent extension and NOT another extension if possible.
    """
    # pass 1: in priority fields, strict 10/11 digits
    for field in CUST_FIELD_PRIORITY:
        if field not in cdr or cdr[field] is None:
            continue
        text = str(cdr[field])
        for m in re.finditer(r"(?<!\d)(\d{10,11})(?!\d)", text):
            digits = m.group(1)
            if agent_ext and digits == agent_ext:
                continue
            if digits in valid_exts:
                continue
            return digits, field

    # pass 2: fallback to prefer_external_number on caller/destination
    agent, cust_raw = prefer_external_number(
        str(cdr.get("caller_id_number") or ""),
        str(cdr.get("destination_number") or ""),
        agent_ext
    )
    cust = clean_number_keep_10_or_11(cust_raw)
    if cust:
        # figure a source hint
        src = "caller_id_number" if digits_only(str(cdr.get("caller_id_number"))) == cust_raw else "destination_number"
        return cust, src

    return None, None

def pick_best_datetime(cdr: Dict[str, Any], original_mtime_iso: Optional[str]) -> Optional[datetime]:
    for key in ("start_stamp", "answer_stamp", "end_stamp"):
        if key in cdr and cdr[key]:
            v = cdr[key]
            if isinstance(v, datetime):
                return v
            try:
                return datetime.fromisoformat(str(v).replace("Z", "+00:00"))
            except Exception:
                pass
    if original_mtime_iso:
        try:
            return datetime.fromisoformat(original_mtime_iso.replace("Z", "+00:00"))
        except Exception:
            return None
    return None

# -------------------- Duration --------------------
def get_audio_duration(file_path: str) -> float:
    ext = os.path.splitext(file_path)[1].lower()
    try:
        if ext == ".mp3":
            return MP3(file_path).info.length
        elif ext == ".wav":
            return WAVE(file_path).info.length
    except Exception:
        return 0.0
    return 0.0

# -------------------- CLI --------------------

def parse_date_range(s: str) -> Tuple[date, date]:
    m = re.match(r"^\s*(\d{4}-\d{2}-\d{2})\s*:\s*(\d{4}-\d{2}-\d{2})\s*$", s)
    if not m:
        raise ValueError("--date-range must be in the form YYYY-MM-DD:YYYY-MM-DD")
    start = date.fromisoformat(m.group(1))
    end = date.fromisoformat(m.group(2))
    if end < start:
        start, end = end, start
    return start, end

def parse_args():
    ap = argparse.ArgumentParser(description="End-to-end: scan → rename plan → S3 upload → Step Functions")
    ap.add_argument("--domain", required=True, help="PBX domain, e.g. leemyles.blueuc.com")
    ap.add_argument("--dry-run", action="store_true", help="Do not upload or trigger Step Functions")
    ap.add_argument("--resume", action="store_true", help="Resume using saved state for this domain")
    ap.add_argument("--state", help="Path to state JSON (default ./out/state_<domain>.json)")
    ap.add_argument("--plan", help="Path to write rename plan JSON (default ./out/rename_plan_<domain>.json)")
    ap.add_argument("--one-file-test", action="store_true",
                    help="Upload only the first eligible file not previously used for one-file tests; stop immediately after")
    ap.add_argument("--date-range", help="YYYY-MM-DD:YYYY-MM-DD inclusive; scan only this range; ignore resume/seed timing; does not update last_run_time_utc")
    ap.add_argument("--no-prefix", action="store_true", help="Ignore S3_KEY_PREFIX and upload to bucket root")
    ap.add_argument("--deep-agent", action="store_true",
                    help="Optional: if FAST agent pick fails, chase bridged legs to find the answered leg (slower).")
    return ap.parse_args()


# -------------------- State I/O --------------------

def default_state_path(domain: str) -> str:
    os.makedirs(STATE_OUT_DIR, exist_ok=True)
    return os.path.join(STATE_OUT_DIR, f"state_{domain}.json")

def default_plan_path(domain: str) -> str:
    os.makedirs(PLAN_OUT_DIR, exist_ok=True)
    return os.path.join(PLAN_OUT_DIR, f"rename_plan_{domain}.json")

def load_state(path: str) -> Dict[str, Any]:
    if not os.path.exists(path):
        return {}
    try:
        with open(path, "r") as f:
            return json.load(f)
    except Exception:
        return {}

def save_state(path: str, state: Dict[str, Any]):
    data = make_json_safe(state)
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w") as f:
        json.dump(data, f, indent=2)

# -------------------- Main --------------------

def main():
    args = parse_args()
    domain = args.domain.strip()
    state_path = args.state or default_state_path(domain)
    plan_path = args.plan or default_plan_path(domain)
    dry_run = args.dry_run
    deep_agent = args.deep_agent  # <— only chase b-legs if explicitly asked

    # Load or init state
    state = load_state(state_path)
    if "one_file_test_history" not in state:
        state["one_file_test_history"] = []  # absolute paths uploaded during --one-file-test

    # Effective config (resume prefers saved config snapshot)
    cfg = {
        "AUDIO_EXTS": AUDIO_EXTS,
        "MIN_FILE_LENGTH_SECONDS": MIN_FILE_LENGTH_SECONDS,
        "FREESWITCH_RECORDING_PATH": FREESWITCH_RECORDING_PATH,
        "RECORD_RETENTION_DAYS": RECORD_RETENTION_DAYS,
        "INITIAL_SEED_DAYS": INITIAL_SEED_DAYS,
        "AGENT_UPLOAD_FILTER_ARRAY": AGENT_UPLOAD_FILTER_ARRAY,
        "S3_BUCKET_NAME": S3_BUCKET_NAME,
        "S3_REGION_NAME": S3_REGION_NAME,
        "S3_KEY_PREFIX": S3_KEY_PREFIX,
        "STEP_FUNCTION_ARN": STEP_FUNCTION_ARN,
        "STEP_FUNCTION_REGION": STEP_FUNCTION_REGION,
        "COMPUTE_MD5": COMPUTE_MD5,
        "UUID_REGEX": UUID_REGEX,
    }
    if args.resume and "config_snapshot" in state:
        print("[INFO] --resume: using prior config snapshot")
        cfg = state["config_snapshot"]

    # Init upload history
    if "uploaded_files" not in state:
        state["uploaded_files"] = {}
    uploaded_files = state["uploaded_files"]

    # Retention pruning (by uploaded_at)
    cutoff = datetime.now(timezone.utc) - timedelta(days=int(cfg["RECORD_RETENTION_DAYS"]))
    to_del = [k for k, v in uploaded_files.items()
              if "uploaded_at" in v and datetime.fromisoformat(v["uploaded_at"].replace("Z","+00:00")) < cutoff]
    for k in to_del:
        uploaded_files.pop(k, None)
    if to_del:
        print(f"[INFO] pruned {len(to_del)} old upload entries older than {cfg['RECORD_RETENTION_DAYS']} days")

    # Determine time window
    should_update_last_run = not (args.date_range or args.one_file_test)
    if args.date_range:
        try:
            start_date, end_date = parse_date_range(args.date_range)
        except Exception as e:
            print(f"[ERROR] {e}")
            sys.exit(2)
        print(f"[INFO] Using explicit date range {start_date} to {end_date} (inclusive). Ignoring --resume/seed timing.")
    elif args.one_file_test:
        start_date = (datetime.now(timezone.utc) - timedelta(days=int(cfg["INITIAL_SEED_DAYS"])) ).date()
        end_date = datetime.now(timezone.utc).date()
        print(f"[INFO] --one-file-test: scanning last {cfg['INITIAL_SEED_DAYS']} day(s); ignoring --resume timing.")
    else:
        last_run = None
        if "last_run_time_utc" in state:
            try:
                last_run = datetime.fromisoformat(state["last_run_time_utc"].replace("Z","+00:00"))
            except Exception:
                last_run = None

        if last_run is None:
            print(f"[INFO] First run or no valid last run time. Initial seed window: {cfg['INITIAL_SEED_DAYS']} day(s).")
            start_date = (datetime.now(timezone.utc) - timedelta(days=int(cfg["INITIAL_SEED_DAYS"])) ).date()
        else:
            print(f"[INFO] Resuming since last run at {state['last_run_time_utc']}")
            start_date = last_run.date()
        end_date = datetime.now(timezone.utc).date()

    # DB connect & prep
    print(f"[INFO] Connecting DB {DB_HOST}:{DB_PORT}/{DB_NAME} as {DB_USER} ...")
    conn = connect_db()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    domain_uuid = resolve_domain_uuid(cur, domain)
    if not domain_uuid:
        print(f"[ERROR] Could not resolve domain_uuid for domain={domain}")
        sys.exit(3)

    valid_ext_list = fetch_valid_extensions(cur, domain_uuid)
    valid_exts = set(valid_ext_list)
    print(f"[INFO] Found {len(valid_exts)} valid extensions for {domain}")

    cols_present = get_columns(cur, "v_xml_cdr")
    uuid_col = next((c for c in ["xml_cdr_uuid","uuid","call_uuid"] if c in cols_present), None)
    print(f"[INFO] v_xml_cdr columns: {len(cols_present)} detected. Using UUID column: {uuid_col or 'none (fallback search)'}")

    # Scan filesystem by date folders
    root_domain_dir = os.path.join(cfg["FREESWITCH_RECORDING_PATH"], domain, "archive")
    if not os.path.isdir(root_domain_dir):
        print(f"[ERROR] Missing domain archive directory: {root_domain_dir}")
        sys.exit(4)

    # Stats + plan items
    items: List[Dict[str, Any]] = []
    uploaded_this_run: List[Dict[str, Any]] = []
    stats = {
        "scanned_days": 0,
        "files_considered": 0,
        "files_duration_ok": 0,
        "cdr_found": 0,
        "no_cdr": 0,
        "mapped_agent_cust": 0,
        "no_agent_match": 0,
        "uploads_done": 0,
        "uploads_skipped_unchanged": 0,
        "uploads_skipped_agent_filter": 0,
        "strategy_counts": {"uuid_col": 0, "fallback_like": 0, "not_found": 0}
    }

    # Build date list inclusive (ascending, deterministic)
    dates: List[date] = []
    d = start_date
    while d <= end_date:
        dates.append(d)
        d += timedelta(days=1)
    stats["scanned_days"] = len(dates)

    # Walk by days; FreeSWITCH month is 'Aug', 'Sep'...:
    month_names = ["Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"]

    # UUID filename regex
    uuid_re = re.compile(UUID_REGEX)

    # Control for one-file-test early stop
    stop_scanning = False

    # Iterate day folders
    for day_date in dates:
        if stop_scanning:
            break
        year_str = f"{day_date.year:04d}"
        month_str = month_names[day_date.month - 1]
        day_str = f"{day_date.day:02d}"

        folder = os.path.join(root_domain_dir, year_str, month_str, day_str)
        if not os.path.isdir(folder):
            continue

        print(f"[SCAN] {folder}")
        for root, dirs, files in os.walk(folder):
            dirs.sort()
            files.sort()
            for fn in files:
                if stop_scanning:
                    break
                ext = os.path.splitext(fn)[1].lower()
                if ext not in cfg["AUDIO_EXTS"]:
                    continue
                stats["files_considered"] += 1

                abs_path = os.path.abspath(os.path.join(root, fn))
                rel_path = os.path.relpath(abs_path, cfg["FREESWITCH_RECORDING_PATH"])

                # Quickly require UUIDish base name
                base = os.path.splitext(fn)[0]
                if not uuid_re.fullmatch(base):
                    # Not a UUID style file — skip
                    continue
                uuid = base

                # Duration check
                duration = get_audio_duration(abs_path)
                if duration < float(cfg["MIN_FILE_LENGTH_SECONDS"]):
                    continue
                stats["files_duration_ok"] += 1

                # Skip if this file was already one-file-tested or uploaded before
                if args.one_file_test:
                    if abs_path in state.get("one_file_test_history", []):
                        continue
                if abs_path in uploaded_files:
                    # already uploaded in a prior run (never upload same file twice)
                    entry_already = {
                        "uuid": uuid,
                        "original": {
                            "filename": fn,
                            "absolute_path": abs_path,
                            "relative_path": rel_path,
                            "extension": ext,
                            "duration_seconds": round(float(duration), 2),
                            "bytes": os.path.getsize(abs_path),
                            "mtime_utc": datetime.utcfromtimestamp(os.path.getmtime(abs_path)).replace(tzinfo=timezone.utc).isoformat().replace("+00:00","Z"),
                            "year": year_str, "month": month_str, "day": day_str
                        },
                        "status": "already_uploaded",
                        "reason": "File previously uploaded (dedupe by absolute path)."
                    }
                    stats["uploads_skipped_unchanged"] += 1
                    items.append(entry_already)
                    continue

                # CDR fetch
                sql, params, cdr_row, strategy = fetch_cdr_by_uuid(cur, uuid, cols_present, uuid_col)
                stats["strategy_counts"][strategy] = stats["strategy_counts"].get(strategy, 0) + 1

                entry = {
                    "uuid": uuid,
                    "original": {
                        "filename": fn,
                        "absolute_path": abs_path,
                        "relative_path": rel_path,
                        "extension": ext,
                        "duration_seconds": round(float(duration), 2),
                        "bytes": os.path.getsize(abs_path),
                        "mtime_utc": datetime.utcfromtimestamp(os.path.getmtime(abs_path)).replace(tzinfo=timezone.utc).isoformat().replace("+00:00","Z"),
                        "year": year_str, "month": month_str, "day": day_str
                    },
                    "sql": {"strategy": strategy, "query": sql, "params": params},
                    "cdr": None,
                    "decision": None,
                    "proposed": None,
                    "status": None,
                    "reason": None
                }

                if not cdr_row:
                    stats["no_cdr"] += 1
                    entry["status"] = "no_cdr"
                    entry["reason"] = "No CDR row found for this UUID (uuid_col or fallback)."
                    items.append(entry)
                    continue

                stats["cdr_found"] += 1

                # CDR subset out
                cdr_subset_fields = [
                    "extension",  # <-- added
                    "caller_id_name","caller_id_number","destination_number","direction",
                    "start_stamp","end_stamp","billsec","answer_stamp","hangup_cause",
                    "accountcode","domain_uuid","record_name","record_path",
                    "last_app","last_arg","presence_id","bridge_uuid","bleg_uuid",
                    "call_flow","context","user_context","caller_destination",
                    "variable_bridge_id","variable_sip_to_user","variable_sip_from_user",
                    "variables","json","raw_json","call_json"
                ]

                cdr_out = {}
                for k in cdr_subset_fields:
                    if k in cdr_row:
                        v = cdr_row.get(k)
                        if isinstance(v, (datetime, date)):
                            v = v.isoformat()
                        cdr_out[k] = v
                entry["cdr"] = cdr_out

                                # Agent detection (FAST, single-row, no heavy scans)
                # ------------------------------------------------
                # Debug: show the core fields first (mirrors UI)
                dbg_direction = str(cdr_row.get("direction") or "")
                dbg_cid = str(cdr_row.get("caller_id_number") or "")
                dbg_dst = str(cdr_row.get("destination_number") or "")
                print(f"\n[DEBUG] v_xml_cdr lookup strategy: {strategy}")
                print(f"[DEBUG] Table: v_xml_cdr (seed a-leg) | UUID: {uuid}")
                print(f"[DEBUG] Direction: {dbg_direction} | caller_id_number: {dbg_cid} | destination_number: {dbg_dst}")

                # Candidate single-field matches
                cand_fields = [
                    ("extension", cdr_row.get("extension")),
                    ("accountcode", cdr_row.get("accountcode")),
                    ("presence_id", cdr_row.get("presence_id")),
                    ("variable_sip_to_user", cdr_row.get("variable_sip_to_user")),
                    ("variable_sip_from_user", cdr_row.get("variable_sip_from_user")),
                ]
                per_field_hits = []
                for fname, fval in cand_fields:
                    if fval is None:
                        continue
                    sval = str(fval).strip()
                    # presence_id comes like '103@domain'
                    if fname == "presence_id":
                        m = re.match(r"(\d+)(?:@|$)", sval)
                        if m and m.group(1) in valid_exts:
                            per_field_hits.append((fname, m.group(1)))
                    else:
                        if sval in valid_exts:
                            per_field_hits.append((fname, sval))

                if per_field_hits:
                    print("[DEBUG] Single-field extension candidates in seed row:")
                    for fname, hit in per_field_hits:
                        print(f"    - {fname}: {hit}")
                else:
                    print("[DEBUG] No single-field extension match present in seed row.")

                # actual pick (FAST)
                agent_ext, agent_src = fast_pick_agent_from_seed_row(cdr_row, valid_exts)
                hits = []  # keep for JSON structure compatibility

                if not agent_ext:
                    entry["decision"] = {
                        "rule": "no_agent_match",
                        "note": "no extension found in standard CDR fields (extension/accountcode/presence_id/sip users/caller-dst by direction)",
                        "agent": None,
                        "cust": None,
                        "match_locations": hits
                    }
                    entry["status"] = "no_agent_match"
                    entry["reason"] = "no single-field agent match"
                    stats["no_agent_match"] += 1
                    print("[DEBUG] AGENT: <none> (no_agent_match)")
                    items.append(entry)
                    continue

                decision_rule = "agent_from_single_field"
                decision_note = f"Picked {agent_ext} via {agent_src}"
                print(f"[DEBUG] AGENT CHOSEN: {agent_ext}  (via {agent_src})")


                # CUST detection
                cust_digits, cust_source_field = find_cust_digits_anywhere(cdr_row, agent_ext, valid_exts)
                if not cust_digits:
                    # fallback final cleanup
                    _, cust_raw = prefer_external_number(
                        str(cdr_row.get("caller_id_number") or ""),
                        str(cdr_row.get("destination_number") or ""),
                        agent_ext
                    )
                    cust_digits = clean_number_keep_10_or_11(cust_raw)
                    cust_source_field = "fallback_prefer_external"

                dt_best = pick_best_datetime(cdr_row, entry["original"]["mtime_utc"])
                s3_name = build_precise_s3_name(domain, cust_digits, uuid, agent_ext, dt_best, ext)

                # Components + provenance for JSON
                proposed = {
                    "s3_upload_filename": s3_name,
                    "s3_components": {
                        "domain": domain,
                        "cust_digits": cust_digits or "UNKNOWN",
                        "cust_source_field": cust_source_field or "UNKNOWN",
                        "uuid": uuid,
                        "agent": agent_ext or "UNKNOWN",
                        "agent_source_field": agent_src or "UNKNOWN",
                        "datetime_iso": fmt_filename_datetime(dt_best),
                        "extension": ext
                    },
                    "new_basename": f"{cust_digits or 'UNKNOWN'}-{agent_ext or 'UNKNOWN'}-{uuid}{ext}",
                    "new_absolute_path": os.path.join(os.path.dirname(entry["original"]["absolute_path"]), f"{cust_digits or 'UNKNOWN'}-{agent_ext or 'UNKNOWN'}-{uuid}{ext}")
                }

                # -------------------- DEBUG: filenames & S3 key preview --------------------
                print(f"[DEBUG] FILENAME: original_basename='{fn}'  |  new_basename='{proposed['new_basename']}'")
                prefix = "" if args.no_prefix else (cfg["S3_KEY_PREFIX"] or "")
                s3_key_preview = s3_name if not prefix else f"{prefix.rstrip('/')}/{s3_name}"
                print(f"[DEBUG] S3 KEY: {s3_key_preview}")
                # --------------------------------------------------------------------------

                # Allow agent filter (optional)
                if AGENT_UPLOAD_FILTER_ARRAY and agent_ext not in AGENT_UPLOAD_FILTER_ARRAY:
                    entry["decision"] = {
                        "rule": decision_rule + " (agent_filtered)",
                        "note": decision_note + f" — filtered out by AGENT_UPLOAD_FILTER_ARRAY {AGENT_UPLOAD_FILTER_ARRAY}",
                        "agent": agent_ext, "cust_digits_10_or_11": cust_digits,
                        "match_locations": []
                    }
                    entry["proposed"] = proposed
                    entry["status"] = "agent_filtered"
                    entry["reason"] = "agent not in allowed list"
                    stats["uploads_skipped_agent_filter"] += 1
                    items.append(entry)
                    continue

                # Final decision
                entry["decision"] = {
                    "rule": decision_rule,
                    "note": decision_note,
                    "agent": agent_ext,
                    "agent_source_field": agent_src,
                    "cust_digits_10_or_11": cust_digits,
                    "cust_source_field": cust_source_field,
                    "match_locations": []
                }
                entry["proposed"] = proposed
                entry["status"] = "ok"
                stats["mapped_agent_cust"] += 1

                # ---- Upload or skip based on state (never re-upload same absolute path)
                # Flatten S3 key (keep prefix if configured)
                prefix = "" if args.no_prefix else (cfg["S3_KEY_PREFIX"] or "")
                s3_key = s3_name if not prefix else f"{prefix.rstrip('/')}/{s3_name}"

                # Dedupe by absolute path across ALL modes
                k = entry["original"]["absolute_path"]
                if k in uploaded_files:
                    entry["status"] = "already_uploaded"
                    entry["reason"] = "File previously uploaded (dedupe by absolute path)."
                    stats["uploads_skipped_unchanged"] += 1
                    items.append(entry)
                    continue

                sz_now = entry["original"]["bytes"]
                did_upload = False
                would_upload = False

                if dry_run:
                    print(f"[DRY-RUN] Would upload -> s3://{S3_BUCKET_NAME}/{s3_key}")
                    would_upload = True
                else:
                    if not S3_BUCKET_NAME:
                        print("[ERROR] S3_BUCKET_NAME not set. Aborting uploads.")
                        cur.close(); conn.close(); sys.exit(5)
                    extra = {}
                    if COMPUTE_MD5:
                        hexd = md5_of_file(k)
                        extra["ContentMD5"] = base64.b64encode(bytes.fromhex(hexd)).decode("ascii")
                    s3 = s3_client(S3_REGION_NAME)
                    s3.upload_file(k, S3_BUCKET_NAME, s3_key, ExtraArgs={kv: vv for kv, vv in extra.items() if vv})
                    stats["uploads_done"] += 1
                    did_upload = True
                    # Track in state (path-based dedupe)
                    uploaded_files[k] = {
                        "uploaded_at": datetime.now(timezone.utc).isoformat().replace("+00:00","Z"),
                        "file_size": int(sz_now),
                        "s3_bucket": S3_BUCKET_NAME,
                        "s3_key": s3_key,
                        "uuid": uuid
                    }
                    uploaded_this_run.append({
                        "uuid": uuid,
                        "local_path": k,
                        "s3_bucket": S3_BUCKET_NAME,
                        "s3_key": s3_key,
                        "agent": agent_ext or "UNKNOWN",
                        "cust": cust_digits or "UNKNOWN",
                        "datetime": proposed["s3_components"]["datetime_iso"],
                    })

                items.append(entry)

                # Early-stop logic for --one-file-test
                if args.one_file_test and (did_upload or would_upload):
                    if did_upload:
                        state["one_file_test_history"].append(k)
                    stop_scanning = True

        # end os.walk
    # end day loop

    # Done DB
    cur.close()
    conn.close()

    # Write plan JSON
    run_meta = {
        "generated_utc": datetime.now(timezone.utc).isoformat().replace("+00:00","Z"),
        "domain": domain,
        "scanned_days": stats["scanned_days"],
        "min_seconds": cfg["MIN_FILE_LENGTH_SECONDS"],
        "audio_exts": cfg["AUDIO_EXTS"]
    }
    plan_doc = {"run": run_meta, "stats": stats, "items": items}
    plan_safe = make_json_safe(plan_doc)
    os.makedirs(os.path.dirname(plan_path), exist_ok=True)
    with open(plan_path, "w") as f:
        json.dump(plan_safe, f, indent=2)
    print(f"[INFO] Wrote rename plan -> {plan_path}")

    # Update & save state
    if should_update_last_run:
        state["last_run_time_utc"] = datetime.now(timezone.utc).isoformat().replace("+00:00","Z")
    state["config_snapshot"] = cfg
    state["uploaded_files"] = uploaded_files
    state["last_plan"] = {"path": os.path.abspath(plan_path), "run": run_meta, "stats": stats}
    save_state(state_path, state)
    print(f"[INFO] Saved state -> {state_path}")

    # Step Functions trigger
    if STEP_FUNCTION_ARN and not dry_run and uploaded_this_run:
        print(f"[INFO] Triggering Step Function: {STEP_FUNCTION_ARN} with {len(uploaded_this_run)} file(s)")
        sf = step_client(STEP_FUNCTION_REGION)
        payload = {
            "domain": domain,
            "run_generated_utc": run_meta["generated_utc"],
            "count": len(uploaded_this_run),
            "uploads": uploaded_this_run,
        }
        try:
            resp = sf.start_execution(stateMachineArn=STEP_FUNCTION_ARN, input=json.dumps(payload))
            exec_arn = resp.get("executionArn")
            print(f"[INFO] Step Function started: {exec_arn}")
            hist = state.get("step_function_executions", [])
            hist.append({
                "when": datetime.now(timezone.utc).isoformat().replace("+00:00","Z"),
                "executionArn": exec_arn,
                "count": len(uploaded_this_run)
            })
            state["step_function_executions"] = hist
            save_state(state_path, state)
        except Exception as e:
            print(f"[ERROR] Step Function start failed: {e}")
    elif STEP_FUNCTION_ARN and dry_run and uploaded_this_run:
        print(f"[DRY-RUN] Would trigger Step Function {STEP_FUNCTION_ARN} with {len(uploaded_this_run)} file(s)")
    else:
        if not STEP_FUNCTION_ARN:
            print("[INFO] STEP_FUNCTION_ARN not set; skipping Step Functions trigger.")
        else:
            print("[INFO] No new uploads this run; Step Functions not triggered.")

    # Summary
    print("\n===== SUMMARY =====")
    print(json.dumps(make_json_safe(stats), indent=2))
    printable = sum(1 for it in items if it.get("status") == "ok")
    print(f"Files with a valid AGENT/CUST mapping (would be renamed): {printable}")



if __name__ == "__main__":
    main()
