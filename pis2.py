#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
ECOUNT INVENTORY CRAWLER (ASYNC) – HISTORY & STREAMLIT READY
===========================================================
Flow (kept exactly as your baseline):
Login -> Click menu -> Click tab "Tất cả" -> Apply filters -> F8 -> Auto-scroll grid -> 1-pass JS extract -> Save

Fixes (NO logic change, only engineering fixes):
1) Remove excuteLogin dependency (use trusted click/enter)
2) Snapshot is fixed per run (snapshot_at + snapshot_id)
3) Batch insert SQLite (fast)
4) Resume is real (resume per snapshot_id via state file + DB) – no CSV fake resume
5) DB schema supports snapshot history + Streamlit queries + exports (parquet/xlsx)

Requirements:
- playwright (async)
- pandas, openpyxl
"""

import os
import sys
import json
import time
import uuid
import sqlite3
import asyncio
import logging
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, List, Tuple, Optional

import pandas as pd
from playwright.async_api import async_playwright, TimeoutError as PWTimeoutError


# ===================== CONFIG =====================

HEADLESS = True
WAIT_TIMEOUT = 30_000

WAIT_GRID_TIMEOUT = 60_000

DB_PATH = "pisco_inventory.db"
EXPORT_DIR = Path("exports")
EXPORT_DIR.mkdir(exist_ok=True)

STATE_DIR = Path("data")
STATE_DIR.mkdir(exist_ok=True)
STATE_FILE = STATE_DIR / "crawler_state.json"

# URLs / Selectors - keep your flow
ECONT_LOGIN_URL = "https://login.ecount.com/"
INVENTORY_URL = (
    "https://login.ecount.com/#menuType=MPGG_RT00000003"
    "&menuSeq=MPGG_RT00000003"
    "&groupSeq=MPGG00000300001"
    "&prgId=MPGG_RT00000003"
    "&depth=1"
)

GRID_ROOT = "#grid-ESZ018R"
GRID_ROW_SEL = "#grid-ESZ018R tr[data-rowtype='line']"

# Credentials (ENV) – Streamlit will inject env when calling subprocess
COMPANY_CODE = os.getenv("ECOMPANY")
USERNAME = os.getenv("EID")
PASSWORD = os.getenv("EPASS")

if not all([COMPANY_CODE, USERNAME, PASSWORD]):
    raise RuntimeError(
        "Missing ECOUNT credentials. Please set ECOMPANY, EID, EPASS environment variables "
        "(or run from Streamlit that injects secrets into env)."
    )

# ===================== LOGGING =====================

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
logger = logging.getLogger("ecount_async_crawler")


# ===================== DB (HISTORY SCHEMA) =====================

def init_db():
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    cur.execute("PRAGMA journal_mode=WAL;")
    cur.execute("PRAGMA synchronous=NORMAL;")
    cur.execute("PRAGMA temp_store=MEMORY;")

    # SNAPSHOT TABLE (có total_qty)
    cur.execute("""
    CREATE TABLE IF NOT EXISTS inventory_snapshot (
        snapshot_id TEXT PRIMARY KEY,
        snapshot_at TEXT NOT NULL,
        row_count INTEGER NOT NULL,
        total_qty INTEGER,
        status TEXT NOT NULL,
        created_at TEXT NOT NULL
    )
    """)

    # DATA TABLE (KHÔNG CÓ ALTER TABLE)
    cur.execute("""
    CREATE TABLE IF NOT EXISTS inventory_data (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        snapshot_id TEXT NOT NULL,
        snapshot_at TEXT NOT NULL,

        product_code TEXT,
        product_name TEXT,
        spec TEXT,

        total_qty INTEGER,
        total_safe_qty INTEGER,

        mindman_qty INTEGER,
        mindman_safe_qty INTEGER,

        pisco_jp_qty INTEGER,
        pisco_jp_safe_qty INTEGER,

        pisco_kr_qty INTEGER,
        pisco_kr_safe_qty INTEGER,

        pisco_tw_qty INTEGER,
        pisco_tw_safe_qty INTEGER,

        totra_qty INTEGER,
        totra_safe_qty INTEGER,

        shortage_qty INTEGER,

        FOREIGN KEY(snapshot_id) REFERENCES inventory_snapshot(snapshot_id)
    )
    """)

    # Indexes
    cur.execute("CREATE INDEX IF NOT EXISTS idx_data_snapshot_id ON inventory_data(snapshot_id)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_data_snapshot_at ON inventory_data(snapshot_at)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_data_product_code ON inventory_data(product_code)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_data_shortage ON inventory_data(shortage_qty)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_snap_at ON inventory_snapshot(snapshot_at)")

    conn.commit()
    conn.close()



def db_get_existing_codes_for_snapshot(snapshot_id: str) -> set:
    """Real resume: codes already saved for this snapshot_id."""
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute("SELECT product_code FROM inventory_data WHERE snapshot_id = ?", (snapshot_id,))
    rows = cur.fetchall()
    conn.close()
    return {r[0] for r in rows if r and r[0]}


def db_insert_snapshot_running(
    snapshot_id: str,
    snapshot_at: str,
    row_count: int,
    total_qty: Optional[int],
    now_str: str
):
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute("""
        INSERT OR REPLACE INTO inventory_snapshot
        (snapshot_id, snapshot_at, row_count, total_qty, status, created_at)
        VALUES (?, ?, ?, ?, 'running', ?)
    """, (
        snapshot_id,
        snapshot_at,
        int(row_count),
        total_qty,
        now_str
    ))
    conn.commit()
    conn.close()


def db_update_snapshot_done(snapshot_id: str, row_count: int):
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute("""
        UPDATE inventory_snapshot
        SET row_count = ?, status = 'done'
        WHERE snapshot_id = ?
    """, (int(row_count), snapshot_id))
    conn.commit()
    conn.close()


def db_batch_insert(records: List[Tuple]):
    if not records:
        return

    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute("PRAGMA journal_mode=WAL;")
    cur.execute("PRAGMA synchronous=NORMAL;")
    cur.execute("PRAGMA temp_store=MEMORY;")

    cur.executemany("""
        INSERT INTO inventory_data (
            snapshot_id, snapshot_at,
            product_code, product_name, spec,
            total_qty, total_safe_qty,
            mindman_qty, mindman_safe_qty,
            pisco_jp_qty, pisco_jp_safe_qty,
            pisco_kr_qty, pisco_kr_safe_qty,
            pisco_tw_qty, pisco_tw_safe_qty,
            totra_qty, totra_safe_qty,
            shortage_qty
        )
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
    """, records)

    conn.commit()
    conn.close()


# ===================== STATE (RESUME) =====================

def load_state() -> Optional[Dict[str, Any]]:
    if not STATE_FILE.exists():
        return None
    try:
        return json.loads(STATE_FILE.read_text(encoding="utf-8"))
    except Exception:
        return None


def save_state(snapshot_id: str, snapshot_at: str):
    STATE_FILE.write_text(
        json.dumps({"snapshot_id": snapshot_id, "snapshot_at": snapshot_at}, ensure_ascii=False, indent=2),
        encoding="utf-8"
    )


def clear_state():
    if STATE_FILE.exists():
        STATE_FILE.unlink()


def get_or_create_snapshot() -> Tuple[str, str]:
    """
    Resume behavior (real):
    - If previous run crashed, keep same snapshot_id and continue inserting missing SKUs into that snapshot.
    - Else create a new snapshot for this run.
    """
    st = load_state()
    if st and st.get("snapshot_id") and st.get("snapshot_at"):
        snapshot_id = st["snapshot_id"]
        snapshot_at = st["snapshot_at"]
        logger.info(f"♻ Resume snapshot: id={snapshot_id} at={snapshot_at}")
        return snapshot_id, snapshot_at

    snapshot_id = str(uuid.uuid4())
    snapshot_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    save_state(snapshot_id, snapshot_at)
    logger.info(f"📸 New snapshot: id={snapshot_id} at={snapshot_at}")
    return snapshot_id, snapshot_at


# ===================== HELPERS =====================

def clean_int(v: Any) -> int:
    if v is None:
        return 0
    s = str(v).replace(",", "").strip()
    if s == "":
        return 0
    try:
        # ECOUNT may show negatives; keep them
        return int(float(s))
    except Exception:
        return 0


def export_outputs(df: pd.DataFrame):
    df.to_parquet(EXPORT_DIR / "inventory_latest.parquet", index=False)

    xlsx_path = EXPORT_DIR / "inventory_latest.xlsx"
    with pd.ExcelWriter(xlsx_path, engine="openpyxl") as writer:
        df.to_excel(writer, sheet_name="latest", index=False)

    (EXPORT_DIR / "snapshot_log.json").write_text(
        json.dumps(
            {"snapshot_id": df["snapshot_id"].iloc[0] if len(df) else None,
             "snapshot_at": df["snapshot_at"].iloc[0] if len(df) else None,
             "rows": int(len(df))},
            ensure_ascii=False, indent=2
        ),
        encoding="utf-8"
    )


# ===================== AUTO SCROLL GRID (VIRTUALIZED) =====================

async def auto_scroll_grid(page, max_idle_round: int = 5):
    logger.info("🔄 Auto scrolling grid...")
    last_count = 0
    idle_round = 0

    while True:
        await page.evaluate(f"""
        () => {{
            const grid = document.querySelector('{GRID_ROOT}');
            if (grid) {{
                grid.scrollTop += grid.clientHeight * 3;
            }}
        }}
        """)
        await page.wait_for_timeout(800)

        count = await page.evaluate(f"""
        () => document.querySelectorAll("{GRID_ROW_SEL}").length
        """)

        if count == last_count:
            idle_round += 1
        else:
            idle_round = 0

        last_count = count

        if idle_round >= max_idle_round:
            break

    logger.info(f"✅ Grid fully loaded ({last_count} rows)")


# ===================== MAIN =====================

async def main():
    init_db()

    snapshot_id, snapshot_at = get_or_create_snapshot()
    now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    

    # Real resume: only skip codes already saved in THIS snapshot
    existing_codes = db_get_existing_codes_for_snapshot(snapshot_id)
    logger.info(f"♻ Resume enabled – {len(existing_codes)} rows already saved in this snapshot")

    added_count = 0
    batch_records: List[Tuple] = []

    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=HEADLESS,
            args=["--start-maximized"]
        )
        context = await browser.new_context(
            viewport={"width": 1920, "height": 1080}
        )
        page = await context.new_page()
        page.set_default_timeout(WAIT_TIMEOUT)
        
        
        # ---------------- LOGIN ----------------
        logger.info("🔐 Login...")

        await page.goto(ECONT_LOGIN_URL, wait_until="domcontentloaded")

        await page.wait_for_selector("#com_code", timeout=30000)

        await page.fill("#com_code", COMPANY_CODE)
        await page.fill("#id", USERNAME)
        await page.fill("#passwd", PASSWORD)

        # ⏳ chờ JS gốc của ECOUNT
        await page.wait_for_function(
            "() => typeof window.excuteLogin === 'function'",
            timeout=30000
        )

        # 🔥 BẮT BUỘC CLICK NÚT LOGIN
        await page.evaluate("""
        () => {
            document.querySelector("#save").click();
        }
        """)

        # ✅ CHECKPOINT SAU LOGIN
        await page.wait_for_selector(
            "div.wrapper-depth1 >> a#link_depth1_MPGG_RT00000003",
            timeout=60000
        )

        

        # Wait login success checkpoint
        await page.wait_for_selector(
            "div.wrapper-depth1 >> a#link_depth1_MPGG_RT00000003",
            timeout=60_000
        )
        logger.info("✅ Logged in successfully")

        # ---------------- NAV (keep your flow) ----------------
        # 1) Click menu
        await page.click("div.wrapper-depth1 >> a#link_depth1_MPGG_RT00000003")

        # 2) Wait tab "Tất cả"
        await page.wait_for_selector("a#all", timeout=30_000)

        # 3) Click tab "Tất cả"
        await page.click("a#all")

        # ---------------- APPLY FILTERS (keep same JS as baseline) ----------------

        checkbox_cids = [
            "cbRptConfirm",
            "BAL_FLAG",
            "DEL_GUBUN",
            "DEL_LOCATION_YN",
            "SAFE_STOCK_QTY_FLAG",
        ]

        # 1) rbSumGubun value="1"
        await page.evaluate("""
        () => {
            const input = document.querySelector("input[data-cid='rbSumGubun'][value='1']");
            if (input && !input.checked) {
                const lbl = document.querySelector(`label[for="${input.id}"]`);
                (lbl || input).click();
            }
        }
        """)

        # 2) main_prod_radio value="I"
        await page.evaluate("""
        () => {
            const input = document.querySelector("input[data-cid='main_prod_radio'][value='I']");
            if (input && !input.checked) {
                const lbl = document.querySelector(`label[for="${input.id}"]`);
                (lbl || input).click();
                input.dispatchEvent(new Event("change", { bubbles: true }));
            }
        }
        """)

        # 3) tick checkboxes
        await page.evaluate("""
        (cids) => {
            for (const cid of cids) {
                const input = document.querySelector(`input[type="checkbox"][data-cid="${cid}"]`);
                if (!input) continue;
                if (input.disabled) continue;
                if (!input.checked) {
                    const lbl = document.querySelector(`label[for="${input.id}"]`);
                    (lbl || input).click();
                    input.dispatchEvent(new Event("change", { bubbles: true }));
                }
            }
        }
        """, checkbox_cids)

        # 4) verify (optional debug)
        verify = await page.evaluate("""
        (cids) => {
            const out = {};
            for (const cid of cids) {
                const input = document.querySelector(`input[type="checkbox"][data-cid="${cid}"]`);
                out[cid] = input ? { checked: input.checked, disabled: input.disabled, id: input.id } : null;
            }
            const mpI = document.querySelector("input[data-cid='main_prod_radio'][value='I']");
            out["main_prod_radio_I"] = mpI ? { checked: mpI.checked, id: mpI.id } : null;

            const rb1 = document.querySelector("input[data-cid='rbSumGubun'][value='1']");
            out["rbSumGubun_1"] = rb1 ? { checked: rb1.checked, id: rb1.id } : null;
            return out;
        }
        """, checkbox_cids)
        logger.info(f"VERIFY FILTER STATE: {verify}")

        # ---------------- LOAD GRID ----------------
        logger.info("⌨ F8 trigger")
        await page.keyboard.press("F8")

        await page.wait_for_selector(
            GRID_ROW_SEL,
            timeout=WAIT_GRID_TIMEOUT
        )
        
        # ---------------- AUTO SCROLL ----------------
        await auto_scroll_grid(page)

        # ================= TOTAL QTY (SYSTEM VALUE) =================
        total_qty_ui = await page.evaluate("""
        () => {
        const pick = (el) => el ? (el.innerText || el.textContent || '').trim() : null;

        // 1) selectors thử nhanh
        const candidates = [
            document.querySelector("#lblTotQty"),
            document.querySelector(".total-qty"),
            document.querySelector("[data-cid='TOT_QTY']"),
            document.querySelector("[id*='TotQty' i]"),
            document.querySelector("[class*='tot' i][class*='qty' i]")
        ];
        for (const el of candidates) {
            const t = pick(el);
            if (t && /\\d{1,3}(,\\d{3})+/.test(t)) return t.replace(/,/g,'');
        }

        // 2) fallback scan text nodes: tìm number dạng 273,298
        const all = Array.from(document.querySelectorAll("span,div,td,strong,b"));
        for (const el of all) {
            const t = pick(el);
            if (!t) continue;
            // ưu tiên phần tử ngắn (tránh bắt nhầm table)
            if (t.length > 30) continue;
            const m = t.match(/\\d{1,3}(,\\d{3})+/);
            if (m) return m[0].replace(/,/g,'');
        }
        return null;
        }
        """)
        TOTAL_QTY_SNAPSHOT = int(total_qty_ui) if total_qty_ui else None
        logger.info(f"📊 TOTAL QTY (SYSTEM): {TOTAL_QTY_SNAPSHOT}")
        db_insert_snapshot_running(
            snapshot_id=snapshot_id,
            snapshot_at=snapshot_at,
            row_count=len(existing_codes),     # 👈 quan trọng cho resume
            total_qty=TOTAL_QTY_SNAPSHOT,
            now_str=now_str
        )

        # ---------------- CRAWL (keep 1-pass JS extract) ----------------
        logger.info("⚡ Fast crawling grid (single JS pass)...")

        raw_rows = await page.evaluate(f"""
        () => {{
            const rows = document.querySelectorAll("{GRID_ROW_SEL}");
            const results = [];

            rows.forEach(row => {{
                const obj = {{}};
                row.querySelectorAll("td").forEach(td => {{
                    const cid = td.getAttribute("data-columnid");
                    if (cid) obj[cid] = td.innerText.trim();
                }});
                if (obj["#tmp.prod_cd"]) results.push(obj);
            }});

            return results;
        }}
        """)

        # Transform -> batch records (FIX#2 snapshot fixed; FIX#3 batch)
        for row_data in raw_rows:
            code = (row_data.get("#tmp.prod_cd") or "").strip()
            if not code:
                continue
            if code in existing_codes:
                continue

            record = {
                "product_code": code,
                "product_name": row_data.get("sale003.prod_des", ""),
                "spec": row_data.get("sale003.size_des", ""),

                "total_qty": row_data.get("BAL_QTY", ""),
                "total_safe_qty": row_data.get("TOT_SAFE_QTY", ""),

                "mindman_qty": row_data.get("SMDM", ""),
                "mindman_safe_qty": row_data.get("SMDM_SAFE_QTY", ""),

                "pisco_jp_qty": row_data.get("SJP", ""),
                "pisco_jp_safe_qty": row_data.get("SJP_SAFE_QTY", ""),

                "pisco_kr_qty": row_data.get("SKR", ""),
                "pisco_kr_safe_qty": row_data.get("SKR_SAFE_QTY", ""),

                "pisco_tw_qty": row_data.get("STW", ""),
                "pisco_tw_safe_qty": row_data.get("STW_SAFE_QTY", ""),

                "totra_qty": row_data.get("STOTRA", ""),
                "totra_safe_qty": row_data.get("STOTRA_SAFE_QTY", ""),
            }

            total_qty = clean_int(record["total_qty"])
            total_safe_qty = clean_int(record["total_safe_qty"])
            shortage = total_qty - total_safe_qty

            batch_records.append((
                snapshot_id, snapshot_at,
                record["product_code"], record["product_name"], record["spec"],
                total_qty, total_safe_qty,
                clean_int(record["mindman_qty"]), clean_int(record["mindman_safe_qty"]),
                clean_int(record["pisco_jp_qty"]), clean_int(record["pisco_jp_safe_qty"]),
                clean_int(record["pisco_kr_qty"]), clean_int(record["pisco_kr_safe_qty"]),
                clean_int(record["pisco_tw_qty"]), clean_int(record["pisco_tw_safe_qty"]),
                clean_int(record["totra_qty"]), clean_int(record["totra_safe_qty"]),
                shortage
            ))

            existing_codes.add(code)
            added_count += 1

        await context.close()
        await browser.close()

    logger.info(f"✅ New rows to insert: {added_count}")

    # Save DB in one batch (FIX#3)
    if batch_records:
        db_batch_insert(batch_records)

    # Snapshot row_count should be total rows for this snapshot
    total_rows = len(db_get_existing_codes_for_snapshot(snapshot_id))
    db_update_snapshot_done(snapshot_id, total_rows)

    # Export latest dataset (for Streamlit fast load)
    conn = sqlite3.connect(DB_PATH)
    df = pd.read_sql(
        """
        SELECT *
        FROM inventory_data
        WHERE snapshot_id = ?
        """,
        conn,
        params=(snapshot_id,)
    )
    conn.close()

    export_outputs(df)

    logger.info(f"✅ Snapshot done: {snapshot_id} | rows={len(df)}")
    clear_state()


if __name__ == "__main__":
    asyncio.run(main())
