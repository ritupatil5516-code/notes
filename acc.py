# core/data/accounts.py
from __future__ import annotations
import json, re
from functools import lru_cache
from pathlib import Path
from typing import Dict, Any, Tuple

from core.config.settings import ACCOUNT_DATA_DIR

# Filenames we expect inside each account_id folder
ACCOUNT_FILENAMES = {
    "transactions": "transactions.json",
    "payments": "payments.json",
    "statements": "statements.json",
    "account_summary": "account_summary.json",
}

_id_ok = re.compile(r"^[A-Za-z0-9_\-]+$")  # sanitize folder name

def _sanitize_account_id(account_id: str) -> str:
    account_id = account_id.strip()
    if not _id_ok.match(account_id):
        raise ValueError(f"Invalid account_id: {account_id!r}")
    return account_id

def account_dir(account_id: str) -> Path:
    """Return the folder Path for this account_id (does not create it)."""
    return ACCOUNT_DATA_DIR / _sanitize_account_id(account_id)

def get_account_paths(account_id: str) -> Dict[str, Path]:
    """
    Return absolute Paths for all four json files for this account_id,
    without reading them. Raises if the directory doesn’t exist.
    """
    base = account_dir(account_id)
    if not base.exists() or not base.is_dir():
        raise FileNotFoundError(f"Account folder not found: {base}")
    return {k: base / fname for k, fname in ACCOUNT_FILENAMES.items()}

def _load_json(p: Path) -> Any:
    with p.open("r", encoding="utf-8") as f:
        return json.load(f)

@lru_cache(maxsize=256)
def load_account_bundle(account_id: str) -> Dict[str, Any]:
    """
    Load and return the four JSON payloads for this account_id.
    Returns dict with keys: transactions, payments, statements, account_summary.
    If a file is missing, raises FileNotFoundError with a clear message.
    """
    paths = get_account_paths(account_id)
    missing = [k for k, p in paths.items() if not p.exists()]
    if missing:
        details = ", ".join(f"{k}→{paths[k]}" for k in missing)
        raise FileNotFoundError(f"Missing required file(s) for {account_id}: {details}")

    return {
        "transactions": _load_json(paths["transactions"]),
        "payments": _load_json(paths["payments"]),
        "statements": _load_json(paths["statements"]),
        "account_summary": _load_json(paths["account_summary"]),
    }

def try_load_account_bundle(account_id: str) -> Tuple[Dict[str, Any], Dict[str, Path]]:
    """
    Non-raising variant. Returns (bundle, paths).
    bundle entries are {} or [] when missing.
    """
    paths = {}
    try:
        paths = get_account_paths(account_id)
    except FileNotFoundError:
        return {"transactions": [], "payments": [], "statements": [], "account_summary": {}}, {}

    bundle = {}
    for key, path in paths.items():
        if path.exists():
            bundle[key] = _load_json(path)
        else:
            bundle[key] = [] if key in ("transactions", "payments", "statements") else {}
    return bundle, paths
