#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
ai_copy_s3.py — end-to-end scanner → UUID CDR lookup (FusionPBX-style) → rename plan → S3 upload → Step Functions
(Agent + Cust restored via *exact* UUID search; no deep leg chasing; minimal bloat)

Usage examples:
  ./ai_copy_s3.py --domain leemyles.blueuc.com
  ./ai_copy_s3.py --domain leemyles.blueuc.com --dry-run
  ./ai_copy_s3.py --domain leemyles.blueuc.com --resume
  ./ai_copy_s3.py --domain leemyles.blueuc.com --one-file-test
  ./ai_copy_s3.py --domain leemyles.blueuc.com --date-range 2023-01-01:2023-01-31
  ./ai_copy_s3.py --domain leemyles.blueuc.com --no-prefix

Key behavior:
• CDR lookup is EXACT on v_xml_cdr.xml_cdr_uuid (same as FusionPBX xml_cdr app’s UUID search).
• AGENT is taken from v_xml_cdr.extension_uuid → v_extensions.extension (if present).
• CUST number is chosen simply from direction:
    - inbound  => destination_number
    - outbound => caller_id_number
  Then normalized to a 10/11-digit string (keeps leading '1' if present). If not available, falls back to UNKNOWN.
• Optional AGENT regex filter from .env: AGENT_UPLOAD_FILTER_REGEX (Python regex). If set and AGENT does not match, skip upload.
• Also supports legacy AGENT_UPLOAD_FILTER_ARRAY (comma-separated allowlist). If both are set, BOTH must pass.

CONFIGURATION (via .env or env.py or environment variables)

  # DB
  DB_HOST (default 127.0.0.1)
  DB_PORT (default 5432)
  DB_NAME (default fusionpbx)
  DB_USER (default postgres)
  DB_PASS (default "")

  # FS paths & scanning
  FREESWITCH_RECORDING_PATH (default /usr/local/freeswitch/recordings)
  AUDIO_EXTS (default ".wav,.mp3")
  MIN_FILE_LENGTH_SECONDS (default 15)
  RECORD_RETENTION_DAYS (default 30)
  INITIAL_SEED_DAYS (default 5)
  UUID_REGEX (default RFC-4122-style 36-char lowercase regex)

  # S3
  S3_BUCKET_NAME (required for uploads)
  S3_REGION_NAME (default us-east-1)
  S3_KEY_PREFIX (default empty)

  # Step Functions
  STEP_FUNCTION_ARN    (optional)
  STEP_FUNCTION_REGION (default S3_REGION_NAME)

  # Misc
  COMPUTE_MD5 (default False)
  PLAN_OUT_DIR (default ./out)
  STATE_OUT_DIR (default ./out)

  # Filters
  AGENT_UPLOAD_FILTER_REGEX (optional Python regex; e.g., "^(1?0?(?:10[1-9]|11[0-9]))$")
  AGENT_UPLOAD_FILTER_ARRAY (optional CSV allowlist; empty = allow all)

Notes:
• Deterministic scanning: dates ascending, folders ascending, files ascending.
• No bridge chasing or full-text scans. Exact UUID match only.
"""

import os
import re
import sys
import json
import argparse
import base64
import hashlib
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

# -------------------- Config --------------------
# DB
DB_HOST = env_str("DB_HOST", "127.0.0.1")
DB_PORT = env_int("DB_PORT", 5432)
DB_NAME = env_str("DB_NAME", "fusionpbx")
DB_USER = env_str("DB_USER", "postgres")
DB_PASS = env_str("DB_PASS", "")

# FS scanning
FREESWITCH_RECORDING_PATH = env_str("FREESWITCH_RECORDING_PATH", "/usr/local/freeswitch/recordings")
AUDIO_EXTS = [s.strip().lower() for s in (env_str("AUDIO_EXTS", ".wav,.mp3") or "").split(",") if s.strip()]
MIN_FILE_LENGTH_SECONDS = env_int("MIN_FILE_LENGTH_SECONDS", 15)
RECORD_RETENTION_DAYS = env_int("RECORD_RETENTION_DAYS", 30)
INITIAL_SEED_DAYS = env_int("INITIAL_SEED_DAYS", 5)
UUID_REGEX = env_str("UUID_REGEX", r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$")

# S3
S3_BUCKET_NAME = env_str("S3_BUCKET_NAME", "")
S3_REGION_NAME = env_str("S3_REGION_NAME", "us-east-1")
S3_KEY_PREFIX = env_str("S3_KEY_PREFIX", "")

# Step Functions
STEP_FUNCTION_ARN = env_str("STEP_FUNCTION_ARN", "")
STEP_FUNCTION_REGION = env_str("STEP_FUNCTION_REGION", S3_REGION_NAME)

# Misc
COMPUTE_MD5 = env_bool("COMPUTE_MD5", False)
PLAN_OUT_DIR = env_str("PLAN_OUT_DIR", "./out")
STATE_OUT_DIR = env_str("STATE_OUT_DIR", "./out")

# Filters
AGENT_UPLOAD_FILTER_REGEX = env_str("AGENT_UPLOAD_FILTER_REGEX", "")
AGENT_UPLOAD_FILTER_ARRAY = [s.strip() for s in (env_str("AGENT_UPLOAD_FILTER_ARRAY", "") or "").split(",") if s.strip()]

# -------------------- Helpers --------------------
def connect_db():
    dsn = f"host={DB_HOST} port={DB_PORT} dbname={DB_NAME} user={DB_USER} password={DB_PASS}"
    return psycopg2.connect(dsn, connect_timeout=5, application_name="ai_copy_s3.py")

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

def md5_of_file(path: str) -> str:
    h = hashlib.md5()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()

def build_precise_s3_name(domain: str, cust_digits: Optional[str], uuid: str, agent_ext: Optional[str], dt: Optional[datetime], ext: str) -> str:
    """
    Filename format (unchanged from original expectations, with AGENT restored):
      <domain>_CUST_<digits or UNKNOWN>_GUID_<uuid>_AGENT_<agent or UNKNOWN>_DATETIME_<YYYY-MM-DDTHH-MM-SS><ext>
    """
    dom = normalize_filename_piece(domain)
    cust = cust_digits if (cust_digits and cust_digits.isdigit() and len(cust_digits) in (10,11)) else "UNKNOWN"
    agent = (agent_ext or "UNKNOWN")
    when = fmt_filename_datetime(dt)
    file_ext = ext if ext.startswith(".") else f".{ext}" if ext else ""
    return f"{dom}_CUST_{cust}_GUID_{uuid}_AGENT_{agent}_DATETIME_{when}{file_ext}"

# -------------------- CDR Lookup (exact UUID match, FusionPBX-style) --------------------
def get_table_columns(cur, table: str) -> set:
    cur.execute("""SELECT column_name FROM information_schema.columns WHERE table_schema IN ('public') AND table_name = %s""", (table,))
    return {r[0] if not isinstance(r, dict) else r.get("column_name") for r in cur.fetchall()}

def fetch_cdr_by_uuid(cur, uuid: str) -> Optional[Dict[str, Any]]:
    cols_cdr = get_table_columns(cur, "v_xml_cdr")
    cols_ext = get_table_columns(cur, "v_extensions")

    # Base fields we rely on
    want_cdr = [
        "xml_cdr_uuid","direction","status","answer_stamp","start_stamp","end_stamp",
        "caller_id_name","caller_id_number","destination_number","extension_uuid"
    ]
    # Optional fields if present
    opt_cdr = ["variable_sip_to_user","variable_sip_from_user"]

    cdr_fields = [f"c.{c}" for c in want_cdr if c in cols_cdr]
    cdr_fields += [f"c.{c}" for c in opt_cdr if c in cols_cdr]

    ext_fields = []
    if "extension" in cols_ext:
        ext_fields.append("e.extension")
    if "effective_caller_id_name" in cols_ext:
        ext_fields.append("e.effective_caller_id_name AS extension_name")

    select_list = ",\n        ".join(cdr_fields + ext_fields)
    sql = f"""
        SELECT
            {select_list}
        FROM v_xml_cdr AS c
        LEFT JOIN v_extensions AS e ON e.extension_uuid = c.extension_uuid
        WHERE c.xml_cdr_uuid = %s
        ORDER BY c.start_stamp DESC
        LIMIT 1;
    """
    cur.execute(sql, (uuid,))
    row = cur.fetchone()
    return dict(row) if row else None

def extract_agent(cdr: Dict[str, Any]) -> Optional[str]:
    # Prefer joined extension
    ext = (cdr.get("extension") or "")
    ext = str(ext).strip() if ext is not None else ""
    if ext:
        return ext
    # Very light fallbacks: SIP to/from user if they look like digits
    to_user = str(cdr.get("variable_sip_to_user") or "").strip()
    from_user = str(cdr.get("variable_sip_from_user") or "").strip()
    if to_user.isdigit():
        return to_user
    if from_user.isdigit():
        return from_user
    return None

def extract_cust(cdr: Dict[str, Any]) -> Optional[str]:
    direction = str(cdr.get("direction") or "").lower()
    cid = str(cdr.get("caller_id_number") or "")
    dst = str(cdr.get("destination_number") or "")
    # Simple rule per user request
    if direction == "inbound":
        prime = dst
    elif direction == "outbound":
        prime = cid
    else:
        # default: prefer the longer 10/11 digit candidate
        prime = dst if len(digits_only(dst)) >= len(digits_only(cid)) else cid
    return clean_number_keep_10_or_11(prime)

def extract_best_time(cdr: Dict[str, Any], fallback_iso_mtime: Optional[str]) -> Optional[datetime]:
    for key in ("start_stamp", "answer_stamp", "end_stamp"):
        v = cdr.get(key)
        if not v:
            continue
        if isinstance(v, datetime):
            return v
        try:
            return datetime.fromisoformat(str(v).replace("Z", "+00:00"))
        except Exception:
            pass
    if fallback_iso_mtime:
        try:
            return datetime.fromisoformat(fallback_iso_mtime.replace("Z", "+00:00"))
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

# -------------------- CLI & State --------------------
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
    ap = argparse.ArgumentParser(description="Scan → UUID CDR lookup → rename plan → S3 upload (with AGENT + CUST from exact UUID)")
    ap.add_argument("--domain", required=True, help="PBX domain, e.g. leemyles.blueuc.com")
    ap.add_argument("--dry-run", action="store_true", help="Do not upload or trigger Step Functions")
    ap.add_argument("--resume", action="store_true", help="Resume using saved state for this domain")
    ap.add_argument("--state", help="Path to state JSON (default ./out/state_<domain>.json)")
    ap.add_argument("--plan", help="Path to write rename plan JSON (default ./out/rename_plan_<domain>.json)")
    ap.add_argument("--one-file-test", action="store_true",
                    help="Upload only the first eligible file not previously used for one-file tests; stop immediately after")
    ap.add_argument("--date-range", help="YYYY-MM-DD:YYYY-MM-DD inclusive; scan only this range; ignore resume/seed timing; does not update last_run_time_utc")
    ap.add_argument("--no-prefix", action="store_true", help="Ignore S3_KEY_PREFIX and upload to bucket root")
    return ap.parse_args()

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
        "S3_BUCKET_NAME": S3_BUCKET_NAME,
        "S3_REGION_NAME": S3_REGION_NAME,
        "S3_KEY_PREFIX": S3_KEY_PREFIX,
        "STEP_FUNCTION_ARN": STEP_FUNCTION_ARN,
        "STEP_FUNCTION_REGION": STEP_FUNCTION_REGION,
        "COMPUTE_MD5": COMPUTE_MD5,
        "UUID_REGEX": UUID_REGEX,
        "AGENT_UPLOAD_FILTER_REGEX": AGENT_UPLOAD_FILTER_REGEX,
        "AGENT_UPLOAD_FILTER_ARRAY": AGENT_UPLOAD_FILTER_ARRAY,
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

    # DB connect
    print(f"[INFO] Connecting DB {DB_HOST}:{DB_PORT}/{DB_NAME} as {DB_USER} ...")
    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASS,
            connect_timeout=5,
            application_name="ai_copy_s3.py",
        )
    except Exception as e:
        print(f"[ERROR] could not connect to database: {e}", file=sys.stderr)
        sys.exit(3)
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    # Build date list inclusive (ascending, deterministic)
    dates: List[date] = []
    d = start_date
    while d <= end_date:
        dates.append(d)
        d += timedelta(days=1)

    stats = {
        "scanned_days": len(dates),
        "files_considered": 0,
        "files_duration_ok": 0,
        "cdr_found": 0,
        "no_cdr": 0,
        "mapped_agent_cust": 0,
        "uploads_done": 0,
        "uploads_skipped_unchanged": 0,
        "uploads_skipped_agent_filter": 0
    }

    items: List[Dict[str, Any]] = []
    uploaded_this_run: List[Dict[str, Any]] = []

    # Month names per FreeSWITCH layout
    month_names = ["Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"]
    uuid_re = re.compile(UUID_REGEX)

    root_domain_dir = os.path.join(cfg["FREESWITCH_RECORDING_PATH"], domain, "archive")
    if not os.path.isdir(root_domain_dir):
        print(f"[ERROR] Missing domain archive directory: {root_domain_dir}")
        cur.close(); conn.close()
        sys.exit(4)

    # AGENT regex pre-compile
    agent_regex = None
    if cfg["AGENT_UPLOAD_FILTER_REGEX"]:
        try:
            agent_regex = re.compile(cfg["AGENT_UPLOAD_FILTER_REGEX"])
        except re.error as e:
            print(f"[ERROR] Invalid AGENT_UPLOAD_FILTER_REGEX: {e}")
            cur.close(); conn.close()
            sys.exit(5)

    stop_scanning = False

    for day in dates:
        if stop_scanning:
            break
        y = f"{day.year:04d}"
        m = month_names[day.month - 1]
        d2 = f"{day.day:02d}"
        folder = os.path.join(root_domain_dir, y, m, d2)
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

                base = os.path.splitext(fn)[0]
                if not uuid_re.fullmatch(base):
                    continue
                uuid = base

                # Duration check
                try:
                    duration = 0.0
                    if ext == ".mp3":
                        duration = MP3(abs_path).info.length
                    elif ext == ".wav":
                        duration = WAVE(abs_path).info.length
                except Exception:
                    duration = 0.0
                if duration < float(cfg["MIN_FILE_LENGTH_SECONDS"]):
                    continue
                stats["files_duration_ok"] += 1

                # Skip already uploaded
                if args.one_file_test and abs_path in state.get("one_file_test_history", []):
                    continue
                if abs_path in uploaded_files:
                    items.append({
                        "uuid": uuid,
                        "original": {
                            "filename": fn,
                            "absolute_path": abs_path,
                            "relative_path": rel_path,
                            "extension": ext,
                            "duration_seconds": round(float(duration), 2),
                            "bytes": os.path.getsize(abs_path),
                            "mtime_utc": datetime.utcfromtimestamp(os.path.getmtime(abs_path)).replace(tzinfo=timezone.utc).isoformat().replace("+00:00","Z"),
                            "year": y, "month": m, "day": d2
                        },
                        "status": "already_uploaded",
                        "reason": "File previously uploaded (dedupe by absolute path)."
                    })
                    stats["uploads_skipped_unchanged"] += 1
                    continue

                # -------- Exact CDR UUID lookup
                cdr = fetch_cdr_by_uuid(cur, uuid)
                if not cdr:
                    stats["no_cdr"] += 1
                    items.append({
                        "uuid": uuid,
                        "original": {
                            "filename": fn,
                            "absolute_path": abs_path,
                            "relative_path": rel_path,
                            "extension": ext,
                            "duration_seconds": round(float(duration), 2),
                            "bytes": os.path.getsize(abs_path),
                            "mtime_utc": datetime.utcfromtimestamp(os.path.getmtime(abs_path)).replace(tzinfo=timezone.utc).isoformat().replace("+00:00","Z"),
                            "year": y, "month": m, "day": d2
                        },
                        "status": "no_cdr",
                        "reason": "No CDR row found for this UUID (exact xml_cdr_uuid match)."
                    })
                    continue

                stats["cdr_found"] += 1

                # Extract AGENT + CUST + datetime
                agent_ext = extract_agent(cdr)
                cust_digits = extract_cust(cdr)
                dt_best = extract_best_time(cdr, datetime.utcfromtimestamp(os.path.getmtime(abs_path)).replace(tzinfo=timezone.utc).isoformat().replace("+00:00","Z"))

                # Apply agent filters if set
                if agent_ext:
                    if agent_regex and not agent_regex.fullmatch(agent_ext):
                        items.append({
                            "uuid": uuid,
                            "original": {
                                "filename": fn,
                                "absolute_path": abs_path,
                                "relative_path": rel_path,
                                "extension": ext,
                                "duration_seconds": round(float(duration), 2),
                                "bytes": os.path.getsize(abs_path),
                                "mtime_utc": datetime.utcfromtimestamp(os.path.getmtime(abs_path)).replace(tzinfo=timezone.utc).isoformat().replace("+00:00","Z"),
                                "year": y, "month": m, "day": d2
                            },
                            "cdr": cdr,
                            "status": "agent_filtered",
                            "reason": f"Agent '{agent_ext}' does not match AGENT_UPLOAD_FILTER_REGEX"
                        })
                        stats["uploads_skipped_agent_filter"] += 1
                        continue
                    if cfg["AGENT_UPLOAD_FILTER_ARRAY"] and agent_ext not in cfg["AGENT_UPLOAD_FILTER_ARRAY"]:
                        items.append({
                            "uuid": uuid,
                            "original": {
                                "filename": fn,
                                "absolute_path": abs_path,
                                "relative_path": rel_path,
                                "extension": ext,
                                "duration_seconds": round(float(duration), 2),
                                "bytes": os.path.getsize(abs_path),
                                "mtime_utc": datetime.utcfromtimestamp(os.path.getmtime(abs_path)).replace(tzinfo=timezone.utc).isoformat().replace("+00:00","Z"),
                                "year": y, "month": m, "day": d2
                            },
                            "cdr": cdr,
                            "status": "agent_filtered",
                            "reason": f"Agent '{agent_ext}' not in AGENT_UPLOAD_FILTER_ARRAY"
                        })
                        stats["uploads_skipped_agent_filter"] += 1
                        continue

                # Filename & proposal
                s3_name = build_precise_s3_name(domain, cust_digits, uuid, agent_ext, dt_best, ext)
                proposed = {
                    "s3_upload_filename": s3_name,
                    "s3_components": {
                        "domain": domain,
                        "cust_digits": cust_digits or "UNKNOWN",
                        "uuid": uuid,
                        "agent": agent_ext or "UNKNOWN",
                        "datetime_iso": fmt_filename_datetime(dt_best),
                        "extension": ext
                    },
                    "new_basename": f"{(cust_digits or 'UNKNOWN')}-{(agent_ext or 'UNKNOWN')}-{uuid}{ext}",
                    "new_absolute_path": os.path.join(os.path.dirname(abs_path), f"{(cust_digits or 'UNKNOWN')}-{(agent_ext or 'UNKNOWN')}-{uuid}{ext}")
                }

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
                        "year": y, "month": m, "day": d2
                    },
                    "cdr": {
                        "direction": cdr.get("direction"),
                        "status": cdr.get("status"),
                        "answer_stamp": str(cdr.get("answer_stamp")) if cdr.get("answer_stamp") else None,
                        "start_stamp": str(cdr.get("start_stamp")) if cdr.get("start_stamp") else None,
                        "end_stamp": str(cdr.get("end_stamp")) if cdr.get("end_stamp") else None,
                        "caller_id_name": cdr.get("caller_id_name"),
                        "caller_id_number": cdr.get("caller_id_number"),
                        "destination_number": cdr.get("destination_number"),
                        "extension": agent_ext,
                        "extension_name": cdr.get("extension_name")
                    },
                    "decision": {
                        "rule": "uuid_exact_match",
                        "agent": agent_ext,
                        "cust_digits_10_or_11": cust_digits
                    },
                    "proposed": proposed,
                    "status": "ok"
                }
                stats["mapped_agent_cust"] += 1

                # ---- Upload or dry-run
                prefix = "" if args.no_prefix else (cfg["S3_KEY_PREFIX"] or "")
                s3_key = s3_name if not prefix else f"{prefix.rstrip('/')}/{s3_name}"

                if abs_path in uploaded_files:
                    entry["status"] = "already_uploaded"
                    entry["reason"] = "File previously uploaded (dedupe by absolute path)."
                    stats["uploads_skipped_unchanged"] += 1
                    items.append(entry)
                    continue

                did_upload = False
                would_upload = False

                if dry_run:
                    print(f"[DRY-RUN] Would upload -> s3://{S3_BUCKET_NAME}/{s3_key}")
                    would_upload = True
                else:
                    if not S3_BUCKET_NAME:
                        print("[ERROR] S3_BUCKET_NAME not set. Aborting uploads.")
                        cur.close(); conn.close(); sys.exit(6)
                    extra_args = {}
                    if COMPUTE_MD5:
                        hexd = md5_of_file(abs_path)
                        extra_args["ContentMD5"] = base64.b64encode(bytes.fromhex(hexd)).decode("ascii")
                    s3 = s3_client(S3_REGION_NAME)
                    s3.upload_file(abs_path, S3_BUCKET_NAME, s3_key, ExtraArgs=extra_args if extra_args else None)
                    stats["uploads_done"] += 1
                    did_upload = True
                    uploaded_files[abs_path] = {
                        "uploaded_at": datetime.now(timezone.utc).isoformat().replace("+00:00","Z"),
                        "file_size": int(os.path.getsize(abs_path)),
                        "s3_bucket": S3_BUCKET_NAME,
                        "s3_key": s3_key,
                        "uuid": uuid
                    }
                    uploaded_this_run.append({
                        "uuid": uuid,
                        "local_path": abs_path,
                        "s3_bucket": S3_BUCKET_NAME,
                        "s3_key": s3_key,
                        "agent": agent_ext or "UNKNOWN",
                        "cust": cust_digits or "UNKNOWN",
                        "datetime": proposed["s3_components"]["datetime_iso"],
                    })

                items.append(entry)

                if args.one_file_test and (did_upload or would_upload):
                    if did_upload:
                        state["one_file_test_history"].append(abs_path)
                    stop_scanning = True

    # Done DB
    cur.close()
    conn.close()

    # Write plan JSON
    run_meta = {
        "generated_utc": datetime.now(timezone.utc).isoformat().replace("+00:00","Z"),
        "domain": domain,
        "scanned_days": stats["scanned_days"],
        "min_seconds": MIN_FILE_LENGTH_SECONDS,
        "audio_exts": AUDIO_EXTS
    }
    plan_doc = {"run": run_meta, "stats": stats, "items": items}
    os.makedirs(os.path.dirname(plan_path), exist_ok=True)
    with open(plan_path, "w") as f:
        json.dump(make_json_safe(plan_doc), f, indent=2)
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
    print("
===== SUMMARY =====")
    print(json.dumps(make_json_safe(stats), indent=2))
    printable = sum(1 for it in items if it.get("status") == "ok")
    print(f"Files with AGENT/CUST mapping (OK): {printable}")
print("\n===== UUID/CUST/AGENT =====")
if uploaded_this_run:
    for u in uploaded_this_run:
        print(f"{u.get('uuid')} | CUST={u.get('cust','UNKNOWN')} | AGENT={u.get('agent','UNKNOWN')} | S3_KEY={u.get('s3_key')}")
else:
    # Fallback: list mappings from items marked ok
    for it in items:
        if it.get("status") == "ok":
            comp = (it.get("proposed") or {}).get("s3_components") or {}
            print(f"{comp.get('uuid')} | CUST={comp.get('cust_digits','UNKNOWN')} | AGENT={comp.get('agent','UNKNOWN')} | S3_KEY={(it.get('proposed') or {}).get('s3_upload_filename')}")

    for it in items:
        if it.get("status") == "ok":
            uuid = it.get("uuid")
            agent = (it.get("decision") or {}).get("agent") or "UNKNOWN"
            cust = (it.get("decision") or {}).get("cust_digits_10_or_11") or "UNKNOWN"
            print(f"  UUID={uuid}  AGENT={agent}  CUST={cust}")

if __name__ == "__main__":
    main()
