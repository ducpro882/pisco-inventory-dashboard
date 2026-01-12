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
EXPORT_DIR = Path(os.getenv("INVENTORY_EXPORT_DIR", "exports"))
EXPORT_DIR.mkdir(exist_ok=True)

INPUT_XLSX = Path(os.getenv("INVENTORY_INPUT_XLSX", EXPORT_DIR / "inventory_latest.xlsx"))
PARQUET_LATEST = EXPORT_DIR / "inventory_latest.parquet"
LOG_FILE = EXPORT_DIR / "update_status.log"
DB_PATH = Path(os.getenv("INVENTORY_DB", "pisco_inventory.db"))
CRAWLER_PATH = os.getenv("INVENTORY_CRAWLER", "").strip()

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

    conn.execute("CREATE INDEX IF NOT EXISTS idx_inv_data_snapshot ON inventory_data(snapshot_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_audit_snapshot ON detect_total_audit(snapshot_id)")

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
    # 1. Run crawler (optional)
    run_crawler_if_any()

    run_pis2_crawler()

    # 2. kiểm tra lại Excel
    if not INPUT_XLSX.exists():
        raise FileNotFoundError(f"pis2 không tạo ra file: {INPUT_XLSX}")

    # 3. đọc dữ liệu
    raw = pd.read_excel(INPUT_XLSX)

    # 2. Read + normalize
    raw = pd.read_excel(INPUT_XLSX)
    raw = normalize(raw)

    # 3. Detect total row + audit
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

    # 4. Snapshot identity
    snapshot_at = datetime.now().isoformat(timespec="seconds")
    snapshot_id = datetime.now().strftime("%Y%m%d%H%M%S")

    # 5. Write Parquet (latest)
    clean_df.to_parquet(PARQUET_LATEST, index=False)

    # 6. Write DB (snapshot + audit)
    conn = sqlite3.connect(DB_PATH)
    try:
        ensure_db(conn)

        total_clean = int(clean_df["total_qty"].sum())
        row_count = int(len(clean_df))
        created_at = datetime.now().isoformat(timespec="seconds")

        # xác định status
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
                int(clean_df["total_qty"].sum()),
                status,
                datetime.now().isoformat(timespec="seconds"),
            )
        )


        conn.execute("DELETE FROM inventory_data WHERE snapshot_id = ?", (snapshot_id,))

        clean_df.assign(snapshot_id=snapshot_id, snapshot_at=snapshot_at).to_sql(
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

    print("OK – Inventory updated | snapshot:", snapshot_id)

    

if __name__ == "__main__":
    main()
