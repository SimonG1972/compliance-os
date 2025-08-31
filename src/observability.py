# src/observability.py
from __future__ import annotations

import os
import sys
import time
import logging
import datetime as _dt
import threading

# Enable verbose logging if COMPLIANCE_VERBOSE is truthy (not "", "0", "false")
_VERB = os.getenv("COMPLIANCE_VERBOSE", "").strip().lower()
VERBOSE = _VERB not in ("", "0", "false", "no")

# Heartbeat interval (seconds) if set
_HEART = os.getenv("COMPLIANCE_HEARTBEAT", "").strip()

def _iso_now() -> str:
    return _dt.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")

def _setup_logging() -> None:
    if not VERBOSE:
        return
    # Simple console logger; safe to import anywhere
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)sZ [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S",
    )
    # Tame common libraries a bit (but still show info)
    for name in ("urllib3", "requests", "sqlalchemy.engine"):
        logging.getLogger(name).setLevel(logging.INFO)

def _patch_requests_logging() -> None:
    """If 'requests' is present, wrap Session.request to print start/stop lines."""
    if not VERBOSE:
        return
    try:
        import requests  # type: ignore
        _orig = requests.Session.request

        def _wrapped(self, method, url, *args, **kwargs):
            start = time.time()
            print(f"{_iso_now()} [http] -> {method} {url}", file=sys.stderr, flush=True)
            try:
                resp = _orig(self, method, url, *args, **kwargs)
                ms = int((time.time() - start) * 1000)
                print(f"{_iso_now()} [http] <- {resp.status_code} {url} ({ms} ms)", file=sys.stderr, flush=True)
                return resp
            except Exception as e:
                ms = int((time.time() - start) * 1000)
                print(f"{_iso_now()} [http] !! {method} {url} ERROR {e} ({ms} ms)", file=sys.stderr, flush=True)
                raise

        requests.Session.request = _wrapped  # type: ignore[attr-defined]
    except Exception:
        # Never break the CLI if anything goes wrong here
        pass

def _start_heartbeat():
    if not _HEART:
        return
    try:
        interval = int(float(_HEART))
        if interval <= 0:
            return
    except Exception:
        return

    def _loop():
        while True:
            try:
                print(f"{_iso_now()} ♥ heartbeat: CLI is running", file=sys.stderr, flush=True)
                time.sleep(interval)
            except Exception:
                # Never crash the process due to logging
                return

    t = threading.Thread(target=_loop, daemon=True)
    t.start()


# Initialize on import (safe side-effects)
_setup_logging()
_patch_requests_logging()
_start_heartbeat()
