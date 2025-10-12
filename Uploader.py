from __future__ import annotations

import json
import os
import shutil
import time
from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import Any, Dict, List, Optional, Tuple

from fastapi import FastAPI, HTTPException, Path as ApiPath
from pydantic import BaseModel, Field, constr

# -------------------------------------------------------------------
# Config
# -------------------------------------------------------------------
# Base directory where each account has its own folder of JSON files.
# Override with env var ACCOUNT_DATA_DIR if you want a custom path.
ACCOUNT_DATA_DIR = Path(os.getenv("ACCOUNT_DATA_DIR", "./data/customer_data")).resolve()

# Filenames per domain
ACCOUNT_FILENAMES: Dict[str, str] = {
    "transactions": "transactions.json",
    "payments": "payments.json",
    "statements": "statements.json",
    "account_summary": "account_summary.json",
}

VALID_ACCOUNT = constr(pattern=r"^[A-Za-z0-9_\-]+$")

# -------------------------------------------------------------------
# Models
# -------------------------------------------------------------------
class UploadBundle(BaseModel):
    accountId: VALID_ACCOUNT = Field(..., description="Folder name to write files under")
    transactions: Optional[List[Dict[str, Any]]] = None
    payments: Optional[List[Dict[str, Any]]] = None
    statements: Optional[List[Dict[str, Any]]] = None
    account_summary: Optional[Dict[str, Any]] = None

    # behavior toggles
    backup_existing: bool = Field(
        default=True,
        description="Create a timestamped .bak before overwriting existing files",
    )
    indent: int = Field(default=2, ge=0, le=8, description="JSON pretty-print indent")


class SingleUpload(BaseModel):
    accountId: VALID_ACCOUNT
    data: Any
    backup_existing: bool = True
    indent: int = 2


# -------------------------------------------------------------------
# Helpers
# -------------------------------------------------------------------
def _account_dir(account_id: str) -> Path:
    p = ACCOUNT_DATA_DIR / account_id
    p.mkdir(parents=True, exist_ok=True)
    return p


def _target_path(account_id: str, domain: str) -> Path:
    if domain not in ACCOUNT_FILENAMES:
        raise HTTPException(status_code=400, detail=f"Unsupported domain '{domain}'. Allowed: {sorted(ACCOUNT_FILENAMES)}")
    return _account_dir(account_id) / ACCOUNT_FILENAMES[domain]


def _atomic_write_json(target: Path, payload: Any, *, indent: int = 2, backup_existing: bool = True) -> Tuple[str, int]:
    """
    Write JSON atomically: temp file -> replace.
    Optionally create a timestamped backup of the existing file.
    Returns (status, byte_count).
    """
    target.parent.mkdir(parents=True, exist_ok=True)

    if backup_existing and target.exists():
        ts = time.strftime("%Y%m%d-%H%M%S")
        backup = target.with_suffix(target.suffix + f".{ts}.bak")
        shutil.copy2(target, backup)

    # Write temp and then replace
    with NamedTemporaryFile("w", encoding="utf-8", delete=False, dir=str(target.parent)) as tf:
        json.dump(payload, tf, ensure_ascii=False, indent=indent)
        tf.flush()
        os.fsync(tf.fileno())
        temp_name = tf.name

    # Replace atomically on POSIX; Windows also OK with replace()
    os.replace(temp_name, target)

    size = target.stat().st_size if target.exists() else 0
    return ("updated" if target.exists() else "created", size)


def _collect_domains(bundle: UploadBundle) -> Dict[str, Any]:
    data: Dict[str, Any] = {}
    for key in ACCOUNT_FILENAMES.keys():
        val = getattr(bundle, key)
        if val is not None:
            data[key] = val
    if not data:
        raise HTTPException(status_code=400, detail="No domain data supplied. Provide at least one of: "
                                                    + ", ".join(ACCOUNT_FILENAMES.keys()))
    return data


# -------------------------------------------------------------------
# FastAPI app
# -------------------------------------------------------------------
app = FastAPI(title="Account JSON Uploader", version="1.0.0")


@app.post("/api/context/v1/upload", summary="Upload multiple JSON domains for an account")
def upload_bundle(bundle: UploadBundle):
    domains = _collect_domains(bundle)
    account_dir = _account_dir(bundle.accountId)

    results = {}
    for domain, payload in domains.items():
        target = _target_path(bundle.accountId, domain)
        status, size = _atomic_write_json(
            target,
            payload,
            indent=bundle.indent,
            backup_existing=bundle.backup_existing,
        )
        # Nicety: counts for array-like data
        count = len(payload) if isinstance(payload, list) else (len(payload) if isinstance(payload, dict) else None)
        results[domain] = {
            "file": str(target),
            "status": status,
            "size_bytes": size,
            "count": count,
        }

    return {
        "accountId": bundle.accountId,
        "account_dir": str(account_dir),
        "written": results,
    }


@app.post(
    "/api/context/v1/upload/{domain}",
    summary="Upload a single JSON domain (transactions/payments/statements/account_summary) for an account",
)
def upload_single(
    domain: str = ApiPath(..., description=f"One of: {', '.join(ACCOUNT_FILENAMES.keys())}"),
    payload: SingleUpload = ...,
):
    target = _target_path(payload.accountId, domain)
    status, size = _atomic_write_json(
        target,
        payload.data,
        indent=payload.indent,
        backup_existing=payload.backup_existing,
    )
    count = len(payload.data) if isinstance(payload.data, list) else (len(payload.data) if isinstance(payload.data, dict) else None)
    return {
        "accountId": payload.accountId,
        "domain": domain,
        "file": str(target),
        "status": status,
        "size_bytes": size,
        "count": count,
    }


@app.get("/api/context/v1/where/{accountId}", summary="Resolve account folder + expected files")
def where(accountId: VALID_ACCOUNT):
    base = _account_dir(accountId)
    files = {d: str((base / f).resolve()) for d, f in ACCOUNT_FILENAMES.items()}
    return {"accountId": accountId, "account_dir": str(base), "files": files}
