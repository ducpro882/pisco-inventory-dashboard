#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
update_inventory_v2.py
=====================
JOB NỀN (BACKGROUND JOB) – KHÔNG NẰM TRONG STREAMLIT

Mục tiêu (chuẩn Data / BI / ERP):
1. (Optional) Chạy crawler
2. Đọc file inventory_latest.xlsx
3. Chuẩn hoá dữ liệu
4. detect_total_row() + AUDIT LOG
5. Xuất inventory_latest.parquet (cho Streamlit đọc)
6. Lưu snapshot + audit vào SQLite để so sánh theo thời gian

⚠️ File này PHẢI chạy bằng:
- Cron (Linux) hoặc
- Task Scheduler (Windows)
KHÔNG chạy bên trong Streamlit
"""

from __future__ import annotations

import os
import time
import sys
import json
import sqlite3
import subprocess
from datetime import datetime
from pathlib import Path
from typing import Optional

import pandas as pd

from detect_total_row import split_total_row

# ===================== CONFIG =====================
BASE_DIR = Path(__file__).resolve().parent
EXPORT_DIR = Path("/data/exports")
EXPORT_DIR.mkdir(exist_ok=True)

INPUT_XLSX = Path(os.getenv("INVENTORY_INPUT_XLSX", EXPORT_DIR / "inventory_latest.xlsx"))
PARQUET_LATEST = EXPORT_DIR / "inventory_latest.parquet"
LOCK_FILE = EXPORT_DIR / "update.lock"
STATUS_FILE = EXPORT_DIR / "update_status.json"
DB_PATH = Path("/data/pisco_inventory.db")
CRAWLER_PATH = os.getenv("INVENTORY_CRAWLER", "").strip()
LOG_FILE = EXPORT_DIR / "update.log"

QTY_COLS = [
    "total_qty",
    "mindman_qty",
    "pisco_jp_qty",
    "pisco_kr_qty",
    "pisco_tw_qty",
    "totra_qty",
]

SAFE_COLS = [
    "total_safe_qty",
    "mindman_safe_qty",
    "pisco_jp_safe_qty",
    "pisco_kr_safe_qty",
    "pisco_tw_safe_qty",
    "totra_safe_qty",
]

# ===================== DB =====================
def write_status(state: str, message: str = "", progress: int | None = None):
    payload = {
        "ts": datetime.now().isoformat(timespec="seconds"),
        "state": state,         # "idle" | "running" | "ok" | "error"
        "message": message,
        "progress": progress
    }
    try:
        STATUS_FILE.write_text(__import__("json").dumps(payload, ensure_ascii=False), encoding="utf-8")
    except Exception:
        pass

def acquire_lock(ttl_seconds: int = 60 * 30) -> bool:
    """
    Trả True nếu lấy được lock.
    Nếu lock tồn tại quá TTL -> coi như lock chết, tự giải phóng.
    """
    try:
        if LOCK_FILE.exists():
            age = time.time() - LOCK_FILE.stat().st_mtime
            if age < ttl_seconds:
                return False
            # lock quá cũ -> giải phóng
            try:
                LOCK_FILE.unlink()
            except Exception:
                return False

        LOCK_FILE.write_text(str(os.getpid()), encoding="utf-8")
        return True
    except Exception:
        return False

def release_lock():
    try:
        if LOCK_FILE.exists():
            LOCK_FILE.unlink()
    except Exception:
        pass


def ensure_db(conn: sqlite3.Connection):
    conn.execute("""
    CREATE TABLE IF NOT EXISTS inventory_snapshot (
        snapshot_id TEXT PRIMARY KEY,
        snapshot_at TEXT,
        row_count INTEGER,
        total_qty INTEGER,
        status TEXT,
        created_at TEXT
    )
    """)

    conn.execute("""
    CREATE TABLE IF NOT EXISTS detect_total_audit (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        snapshot_id TEXT,
        detected_at TEXT,
        found INTEGER,
        confidence REAL,
        score REAL,
        row_index INTEGER,
        reasons TEXT,
        total_qty_excel INTEGER,
        total_qty_clean INTEGER
    )
    """)

    # MIGRATION: nếu inventory_data có cột id (schema cũ) -> rebuild
    migrate_inventory_data_if_needed(conn)

    conn.execute("""
    CREATE TABLE IF NOT EXISTS inventory_data (
        snapshot_id TEXT,
        snapshot_at TEXT,
        product_code TEXT,
        product_name TEXT,
        spec TEXT,
        total_qty INTEGER,
        total_safe_qty INTEGER,
        shortage_qty INTEGER,
        mindman_qty INTEGER,
        mindman_safe_qty INTEGER,
        pisco_jp_qty INTEGER,
        pisco_jp_safe_qty INTEGER,
        pisco_kr_qty INTEGER,
        pisco_kr_safe_qty INTEGER,
        pisco_tw_qty INTEGER,
        pisco_tw_safe_qty INTEGER,
        totra_qty INTEGER,
        totra_safe_qty INTEGER
    )
    """)

    conn.execute("CREATE INDEX IF NOT EXISTS idx_inv_data_snapshot ON inventory_data(snapshot_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_audit_snapshot ON detect_total_audit(snapshot_id)")

def migrate_inventory_data_if_needed(conn: sqlite3.Connection):
    """
    Nếu inventory_data đang có cột 'id' (schema cũ) -> rebuild lại table.
    """
    cur = conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='inventory_data'")
    if not cur.fetchone():
        return  # chưa có table -> không cần migrate

    cols = [r[1] for r in conn.execute("PRAGMA table_info(inventory_data)").fetchall()]  # r[1] = column name
    if "id" not in cols:
        return  # schema ok

    # Rebuild table
    conn.execute("ALTER TABLE inventory_data RENAME TO inventory_data_old")

    conn.execute("""
    CREATE TABLE inventory_data (
        snapshot_id TEXT,
        snapshot_at TEXT,
        product_code TEXT,
        product_name TEXT,
        spec TEXT,
        total_qty INTEGER,
        total_safe_qty INTEGER,
        shortage_qty INTEGER,
        mindman_qty INTEGER,
        mindman_safe_qty INTEGER,
        pisco_jp_qty INTEGER,
        pisco_jp_safe_qty INTEGER,
        pisco_kr_qty INTEGER,
        pisco_kr_safe_qty INTEGER,
        pisco_tw_qty INTEGER,
        pisco_tw_safe_qty INTEGER,
        totra_qty INTEGER,
        totra_safe_qty INTEGER
    )
    """)

    # Copy dữ liệu cũ sang (bỏ id)
    common = [
        "snapshot_id","snapshot_at","product_code","product_name","spec",
        "total_qty","total_safe_qty","shortage_qty",
        "mindman_qty","mindman_safe_qty",
        "pisco_jp_qty","pisco_jp_safe_qty",
        "pisco_kr_qty","pisco_kr_safe_qty",
        "pisco_tw_qty","pisco_tw_safe_qty",
        "totra_qty","totra_safe_qty"
    ]

    # Chỉ copy những cột thực sự tồn tại trong table cũ
    old_cols = set(cols)
    use_cols = [c for c in common if c in old_cols]

    conn.execute(
        f"INSERT INTO inventory_data ({','.join(use_cols)}) "
        f"SELECT {','.join(use_cols)} FROM inventory_data_old"
    )

    conn.execute("DROP TABLE inventory_data_old")
    conn.commit()


def log(msg):
    ts = datetime.now().strftime("%H:%M:%S")
    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write(f"[{ts}] {msg}\n")

# ===================== NORMALIZE =====================
def normalize(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()

    for c in ["product_code", "product_name", "spec", "snapshot_id", "snapshot_at"]:
        if c not in out.columns:
            out[c] = ""
        out[c] = out[c].astype(str).fillna("").str.strip()

    for c in QTY_COLS + SAFE_COLS + ["shortage_qty"]:
        if c not in out.columns:
            out[c] = 0
        out[c] = pd.to_numeric(out[c], errors="coerce").fillna(0).astype("int64")

    out["shortage_qty"] = out["total_qty"] - out["total_safe_qty"]
    return out


# ===================== CRAWLER HOOK =====================
def run_crawler_if_any():
    if not CRAWLER_PATH:
        return
    subprocess.run([sys.executable, CRAWLER_PATH], check=True)

def run_pis2_crawler():
    """
    Chạy crawler pis2.py để tạo inventory_latest.xlsx
    """
    log("START pis2 crawler")

    proc = subprocess.run(
        [sys.executable, "pis2.py"],
        capture_output=True,
        text=True
    )

    if proc.returncode != 0:
        log("pis2 crawler FAILED")
        log(proc.stderr)
        raise RuntimeError("pis2 crawler failed")

    log("pis2 crawler FINISHED")

# ===================== MAIN =====================
def main():
    print("DB_PATH =", DB_PATH.resolve())

    # 0. Check ENV (Render)
    required = ["ECOMPANY", "EID", "EPASS"]
    missing = [k for k in required if not os.getenv(k)]
    if missing:
        write_status("error", f"Missing env vars: {missing}")
        raise RuntimeError(f"Missing env vars: {missing}")

    # 1. Acquire lock
    if not acquire_lock():
        write_status("running", "Another update is already running")
        return

    write_status("running", "Update started", progress=0)
    log("UPDATE STARTED")

    try:
        # 2. Run crawler (optional)
        write_status("running", "Running crawler", progress=10)
        run_crawler_if_any()

        write_status("running", "Running pis2 crawler", progress=20)
        run_pis2_crawler()

        # 3. Check Excel
        if not INPUT_XLSX.exists():
            raise FileNotFoundError(f"pis2 không tạo ra file: {INPUT_XLSX}")

        # 4. Read + normalize
        write_status("running", "Reading & normalizing data", progress=40)
        raw = pd.read_excel(INPUT_XLSX)
        raw = normalize(raw)

        # 5. Detect total row
        write_status("running", "Detecting total row", progress=60)
        clean_df, total_row, meta = split_total_row(
            raw,
            qty_cols=QTY_COLS,
            code_col="product_code",
            name_col="product_name",
            spec_col="spec",
            tolerance=0,
            min_confidence=0.80,
            last_k_rows=8,
        )

        # 6. Snapshot identity
        snapshot_at = datetime.now().isoformat(timespec="seconds")
        snapshot_id = datetime.now().strftime("%Y%m%d%H%M%S")

        # 7. Write Parquet
        write_status("running", "Writing parquet", progress=80)
        clean_df.to_parquet(PARQUET_LATEST, index=False)

        # 8. Write DB
        write_status("running", "Writing database", progress=90)
        conn = sqlite3.connect(DB_PATH)
        try:
            ensure_db(conn)

            total_clean = int(clean_df["total_qty"].sum())
            created_at = datetime.now().isoformat(timespec="seconds")

            if not meta["found"]:
                status = "SUCCESS"
            elif meta["confidence"] >= 0.9:
                status = "SUCCESS"
            elif meta["confidence"] >= 0.8:
                status = "WARNING"
            else:
                status = "FAILED"

            conn.execute(
                """
                INSERT OR REPLACE INTO inventory_snapshot
                (snapshot_id, snapshot_at, row_count, total_qty, status, created_at)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    snapshot_id,
                    snapshot_at,
                    len(clean_df),
                    total_clean,
                    status,
                    created_at,
                )
            )

            conn.execute("DELETE FROM inventory_data WHERE snapshot_id = ?", (snapshot_id,))
            # ===== ENSURE SCHEMA MATCH (DROP COLUMNS NOT IN DB) =====
            allowed_cols = [
                "snapshot_id", "snapshot_at",
                "product_code", "product_name", "spec",
                "total_qty", "total_safe_qty", "shortage_qty",
                "mindman_qty", "mindman_safe_qty",
                "pisco_jp_qty", "pisco_jp_safe_qty",
                "pisco_kr_qty", "pisco_kr_safe_qty",
                "pisco_tw_qty", "pisco_tw_safe_qty",
                "totra_qty", "totra_safe_qty",
            ]

            df_to_db = (
                clean_df
                .assign(snapshot_id=snapshot_id, snapshot_at=snapshot_at)
                .loc[:, lambda d: [c for c in allowed_cols if c in d.columns]]
            )
            print("DF columns =", list(clean_df.columns))
            df_to_db.to_sql(
                "inventory_data",
                conn,
                if_exists="append",
                index=False,
            )


            conn.execute(
                """
                INSERT INTO detect_total_audit (
                    snapshot_id, detected_at, found,
                    confidence, score, row_index, reasons,
                    total_qty_excel, total_qty_clean
                ) VALUES (?,?,?,?,?,?,?,?,?)
                """,
                (
                    snapshot_id,
                    created_at,
                    int(meta["found"]),
                    meta["confidence"],
                    meta["score"],
                    meta["row_index"],
                    json.dumps(meta["reasons"], ensure_ascii=False),
                    int(total_row["total_qty"]) if total_row is not None else None,
                    total_clean,
                ),
            )

            conn.commit()
        finally:
            conn.close()

        write_status("ok", "Update finished", progress=100)
        log(f"UPDATE FINISHED | snapshot={snapshot_id}")

    except Exception as e:
        write_status("error", f"Update failed: {e}")
        log(f"UPDATE FAILED: {e}")
        raise
    finally:
        release_lock()

    

if __name__ == "__main__":
    main()
