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
  --one-file-test        Upload ONLY the first eligible file found this run.
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
    print("
===== SUMMARY =====")
    print(json.dumps(make_json_safe(stats), indent=2))
    printable = sum(1 for it in items if it.get("status") == "ok")
    print(f"Files with a valid AGENT/CUST mapping (would be renamed): {printable}")

if __name__ == "__main__":
    main()
