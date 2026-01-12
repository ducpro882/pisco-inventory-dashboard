#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
app_vn_exec_detect_total.py
==========================
Dashboard Tồn kho (Executive – Tiếng Việt)

NÂNG CẤP (v3 - UX/Logic Search thông minh):
✔ Search “Google-like”: tự nhận diện SKU vs từ khóa, hỗ trợ nhiều token, xếp hạng kết quả theo điểm phù hợp
✔ Ưu tiên khớp mã SKU chính xác / bắt đầu / chứa; sau đó mới đến tên/spec
✔ Query có thể là: "PIS-ABC123", "ABC123", "van khí", "abc 123 pisco"
✔ Nếu chỉ có 1 kết quả -> hiển thị Summary Card trước bảng
✔ Có “Search presets” và “lọc nhiễu” (min qty, chỉ hiển thị SKU có tồn > 0 theo kho chọn)
✔ Tối ưu UX sidebar: ít nhưng đúng việc
✔ Ẩn thông số kỹ thuật gây nhiễu (snapshot_id, confidence...) ra khỏi UI chính; chỉ để Audit

Ghi chú:
- Ưu tiên đọc DB theo latest snapshot (inventory_snapshot). Nếu không có thì fallback từ inventory_data.
"""

from __future__ import annotations
import json
import re
import time
import sys
import subprocess
import sqlite3
from datetime import datetime
from pathlib import Path
from io import BytesIO

import pandas as pd
import streamlit as st

from detect_total_row import split_total_row

# ===================== CONFIG =====================
st.set_page_config(
    page_title="Dashboard PISCO – Executive",
    layout="wide",
    initial_sidebar_state="expanded"
)

st.markdown("""
<style>

/* ===== KPI TILE – STREAMLIT SAFE ===== */
div[data-testid="stMarkdown"] .kpi-tile {
    background: #FFFFFF !important;
    border-radius: 14px !important;
    padding: 16px 12px !important;
    text-align: center !important;
    border: 1px solid #E5E7EB !important;
    box-shadow: 0 2px 6px rgba(15,23,42,0.08) !important;
}

div[data-testid="stMarkdown"] .kpi-tile.primary {
    background: linear-gradient(135deg, #1F4FD8, #1C46C7) !important;
    color: #FFFFFF !important;
    border: none !important;
}

div[data-testid="stMarkdown"] .kpi-tile .label {
    font-size: 12px !important;
    font-weight: 700 !important;
    color: #64748B !important;
}

div[data-testid="stMarkdown"] .kpi-tile.primary .label {
    color: #E0E7FF !important;
}

div[data-testid="stMarkdown"] .kpi-tile .value {
    font-size: 26px !important;
    font-weight: 800 !important;
    margin-top: 6px !important;
}

</style>
""", unsafe_allow_html=True)



BASE_DIR = Path(__file__).resolve().parent
EXPORT_DIR = Path("/data/exports")
EXPORT_DIR.mkdir(exist_ok=True)

DB_PATH = Path("/data/pisco_inventory.db")
PARQUET_LATEST = EXPORT_DIR / "inventory_latest.parquet"

STATUS_FILE = EXPORT_DIR / "update_status.json"

UPDATE_JOB = BASE_DIR / "update_inventory_v2.py"
 # False nếu chỉ muốn reload cache (không chạy crawler)

# UX mode (giữ đơn giản, có thể mở rộng thêm)
UX_MODE = "EXECUTIVE"  # EXECUTIVE | ANALYST

QTY_COLS = [
    "total_qty",
    "mindman_qty",
    "pisco_jp_qty",
    "pisco_kr_qty",
    "pisco_tw_qty",
    "totra_qty",
]

WAREHOUSE_LABEL = {
    "total_qty": "Tổng",
    "mindman_qty": "Mindman",
    "pisco_jp_qty": "NIHON PISCO",
    "pisco_kr_qty": "Pisco KR",
    "pisco_tw_qty": "Pisco TW",
    "totra_qty": "Totra",
}

DISPLAY_QTY_COLS = [
    "total_qty",
    "mindman_qty",
    "pisco_jp_qty",
    "pisco_kr_qty",
    "pisco_tw_qty",
    "totra_qty",
]

DISPLAY_COLS = [
    "product_code",
    "product_name",
    "spec",
] + DISPLAY_QTY_COLS

MEETING_MODE_DEFAULT = False  # True nếu muốn vào là ẩn sidebar luôn

# ===================== HELPERS =====================
def section(title: str, icon: str = "", desc: str | None = None):
    st.markdown(
        f"""
        <div style="
            border:1px solid #E6E6E6;
            border-radius:10px;
            padding:16px;
            margin-top:18px;
            margin-bottom:18px;
            background-color:#FFFFFF;
        ">
            <h4 style="margin-bottom:6px;">{icon} {title}</h4>
            {f"<div style='color:#666; font-size:13px; margin-bottom:12px;'>{desc}</div>" if desc else ""}
        </div>
        """,
        unsafe_allow_html=True
    )


def fmt_int(x) -> str:
    try:
        if pd.isna(x):
            return "-"
        return f"{int(x):,}"
    except Exception:
        return "-"

def fmt_pct(x) -> str:
    try:
        return f"{float(x):.2f}%"
    except Exception:
        return "-"

def normalize_sku_text(s: str) -> str:
    """
    Chuẩn hóa SKU để so khớp:
    - Uppercase
    - Bỏ tiền tố PIS-
    - Bỏ mọi ký tự không phải chữ + số
    """
    if not isinstance(s, str):
        return ""
    s = s.upper().strip()
    if s.startswith("PIS-"):
        s = s[4:]
    s = re.sub(r"[^A-Z0-9]", "", s)
    return s

def tokenize_query(q: str) -> list[str]:
    """
    Token hóa query để search thông minh:
    - Chuẩn hóa về uppercase
    - Tách theo space / dấu
    - Bỏ token quá ngắn (<=1) để giảm nhiễu
    """
    if not isinstance(q, str):
        return []
    q = q.strip()
    if not q:
        return []
    # giữ lại cả dạng raw tokens (có thể có dấu -) và norm tokens (A-Z0-9)
    raw = re.split(r"[\s,;|/\\]+", q.upper())
    raw = [t.strip() for t in raw if t.strip()]
    # giảm nhiễu: bỏ token 1 ký tự (trừ khi toàn query chỉ 1 token)
    if len(raw) > 1:
        raw = [t for t in raw if len(t) > 1]
    return raw

def looks_like_sku(q: str) -> bool:
    """
    Heuristic: query trông giống SKU nếu:
    - sau normalize còn >= 4 ký tự
    - và có chữ + số hoặc độ dài lớn
    """
    n = normalize_sku_text(q)
    if len(n) < 4:
        return False
    has_alpha = bool(re.search(r"[A-Z]", n))
    has_digit = bool(re.search(r"\d", n))
    if has_alpha and has_digit:
        return True
    # SKU toàn số dài hoặc toàn chữ dài vẫn có thể là SKU
    return len(n) >= 8

def normalize(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()

    for c in ["product_code", "product_name", "spec", "snapshot_id", "snapshot_at"]:
        if c not in out.columns:
            out[c] = ""
        out[c] = out[c].astype(str).fillna("").str.strip()

    for c in QTY_COLS + ["total_safe_qty", "shortage_qty"]:
        if c not in out.columns:
            out[c] = 0
        out[c] = pd.to_numeric(out[c], errors="coerce").fillna(0).astype("int64")

    
    out["_snapshot_at_dt"] = pd.to_datetime(out["snapshot_at"], errors="coerce")
    return out



def db_table_exists(conn: sqlite3.Connection, table_name: str) -> bool:
    q = "SELECT name FROM sqlite_master WHERE type='table' AND name=?"
    cur = conn.execute(q, (table_name,))
    return cur.fetchone() is not None

def get_latest_snapshot_id(conn: sqlite3.Connection) -> tuple[str | None, str | None, str | None]:
    """
    Returns: (snapshot_id, snapshot_at, status)
    """
    if db_table_exists(conn, "inventory_snapshot"):
        try:
            row = conn.execute(
                """
                SELECT snapshot_id, snapshot_at, status
                FROM inventory_snapshot
                ORDER BY created_at DESC
                LIMIT 1
                """
            ).fetchone()
            if row:
                return row[0], row[1], row[2]
        except Exception:
            pass
    return None, None, None

def list_snapshots(conn: sqlite3.Connection) -> pd.DataFrame:
    """
    Return snapshots list with columns: snapshot_id, snapshot_at, status, created_at
    Prefers inventory_snapshot; fallback to distinct from inventory_data.
    """
    if db_table_exists(conn, "inventory_snapshot"):
        df = pd.read_sql(
            """
            SELECT snapshot_id, snapshot_at, status, created_at
            FROM inventory_snapshot
            ORDER BY created_at DESC
            """,
            conn
        )
        if not df.empty:
            return df

    if db_table_exists(conn, "inventory_data"):
        df = pd.read_sql(
            """
            SELECT snapshot_id, snapshot_at
            FROM inventory_data
            GROUP BY snapshot_id, snapshot_at
            ORDER BY snapshot_at DESC
            """,
            conn
        )
        if not df.empty:
            df["status"] = None
            df["created_at"] = None
            return df

    return pd.DataFrame(columns=["snapshot_id", "snapshot_at", "status", "created_at"])

def load_latest_data_from_db() -> tuple[pd.DataFrame | None, dict]:
    """
    Load latest snapshot dataset.
    meta: {snapshot_id, snapshot_at, status, source}
    """
    if not DB_PATH.exists():
        return None, {"source": "none"}

    conn = sqlite3.connect(DB_PATH)
    try:
        sid, sat, status = get_latest_snapshot_id(conn)

        if sid:
            df = pd.read_sql(
                "SELECT * FROM inventory_data WHERE snapshot_id = ?",
                conn,
                params=(sid,)
            )
            return df, {
                "snapshot_id": sid,
                "snapshot_at": sat,
                "status": status,
                "source": "db:inventory_snapshot"
            }

        # fallback: no snapshot table -> pick max snapshot_at from inventory_data
        df = pd.read_sql("SELECT * FROM inventory_data", conn)
        if df.empty:
            return None, {"source": "db:empty"}

        df = normalize(df)
        max_dt = df["_snapshot_at_dt"].max()
        if pd.isna(max_dt):
            df = df.sort_values("snapshot_at", ascending=False)
            sat = df["snapshot_at"].iloc[0]
        else:
            df = df[df["_snapshot_at_dt"] == max_dt]
            sat = df["snapshot_at"].iloc[0] if "snapshot_at" in df.columns and len(df) else str(max_dt)

        df = df.drop(columns=["_snapshot_at_dt"], errors="ignore")
        return df, {
            "snapshot_id": df["snapshot_id"].iloc[0] if "snapshot_id" in df.columns and len(df) else None,
            "snapshot_at": sat,
            "status": None,
            "source": "db:fallback_inventory_data"
        }
    finally:
        conn.close()

def load_data() -> tuple[pd.DataFrame | None, dict]:
    """
    Priority: DB latest snapshot -> Parquet latest -> None
    """
    df, meta = load_latest_data_from_db()
    if df is not None and not df.empty:
        return df, meta

    if PARQUET_LATEST.exists():
        try:
            return pd.read_parquet(PARQUET_LATEST), {"source": "parquet"}
        except Exception:
            pass

    return None, {"source": "none"}

def kpi_top_share(df: pd.DataFrame, qty_col: str, top_n: int) -> float:
    total = df[qty_col].sum()
    if total <= 0:
        return 0.0
    top_sum = df.sort_values(qty_col, ascending=False).head(top_n)[qty_col].sum()
    return float(top_sum / total * 100)

def kpi_sku_for_80(df: pd.DataFrame, qty_col: str) -> int:
    tmp = df.sort_values(qty_col, ascending=False).copy()
    total = tmp[qty_col].sum()
    if total <= 0:
        return 0
    tmp["cum"] = tmp[qty_col].cumsum() / total
    return int((tmp["cum"] <= 0.80).sum())

def build_compare(now_df: pd.DataFrame, old_df: pd.DataFrame, qty_col: str) -> pd.DataFrame:
    now = now_df[["product_code", "product_name", "spec", qty_col]].copy()
    old = old_df[["product_code", qty_col]].copy()

    now = now.rename(columns={qty_col: "qty_now"})
    old = old.rename(columns={qty_col: "qty_old"})

    merged = now.merge(old, on="product_code", how="left")
    merged["qty_old"] = pd.to_numeric(merged["qty_old"], errors="coerce").fillna(0).astype("int64")
    merged["qty_now"] = pd.to_numeric(merged["qty_now"], errors="coerce").fillna(0).astype("int64")
    merged["delta"] = merged["qty_now"] - merged["qty_old"]
    # thêm % change để lọc nhiễu
    merged["delta_pct"] = merged.apply(
        lambda r: (r["delta"] / r["qty_old"] * 100) if r["qty_old"] > 0 else (100.0 if r["qty_now"] > 0 else 0.0),
        axis=1
    )
    return merged

def to_excel_bytes(sheets: dict[str, pd.DataFrame]) -> bytes:
    buf = BytesIO()
    with pd.ExcelWriter(buf, engine="xlsxwriter") as writer:
        for name, df in sheets.items():
            df.to_excel(writer, index=False, sheet_name=name[:31])
    return buf.getvalue()

def read_update_status() -> dict | None:
    if not STATUS_FILE.exists():
        return None
    try:
        return json.loads(STATUS_FILE.read_text(encoding="utf-8"))
    except Exception:
        return None

# ===================== SMART SEARCH (CORE) =====================
def prepare_search_index(df: pd.DataFrame) -> pd.DataFrame:
    """
    Tạo cột index phục vụ search nhanh và nhất quán.
    """
    out = df.copy()
    out["_norm_code"] = out["product_code"].apply(normalize_sku_text)
    out["_norm_name"] = out["product_name"].apply(normalize_sku_text)
    out["_norm_spec"] = out["spec"].apply(normalize_sku_text)

    # raw uppercase (giữ khoảng trắng) để search kiểu contains từ khóa mềm
    out["_up_name"] = out["product_name"].astype(str).fillna("").str.upper()
    out["_up_spec"] = out["spec"].astype(str).fillna("").str.upper()
    out["_up_code"] = out["product_code"].astype(str).fillna("").str.upper()
    return out

def score_row(
    norm_q: str,
    raw_tokens: list[str],
    row: pd.Series
) -> int:
    """
    Chấm điểm 1 dòng theo query.
    Ưu tiên SKU code, sau đó đến name/spec.
    """
    score = 0

    code_n = row.get("_norm_code", "")
    name_n = row.get("_norm_name", "")
    spec_n = row.get("_norm_spec", "")

    up_code = row.get("_up_code", "")
    up_name = row.get("_up_name", "")
    up_spec = row.get("_up_spec", "")

    # 1) Ưu tiên khớp CODE (SKU) - mạnh nhất
    if norm_q:
        if code_n == norm_q:
            score += 1000
        elif code_n.startswith(norm_q):
            score += 650
        elif norm_q in code_n:
            score += 450

    # 2) Token matching (giúp query dạng "abc 123 pisco" vẫn ra)
    #    - token match code mạnh hơn name/spec
    for t in raw_tokens:
        tn = normalize_sku_text(t)
        if not tn:
            continue

        if tn and tn == code_n:
            score += 220
        elif tn and code_n.startswith(tn):
            score += 160
        elif tn and tn in code_n:
            score += 120

        # name/spec normalized (mức vừa)
        if tn and tn in name_n:
            score += 55
        if tn and tn in spec_n:
            score += 45

        # raw keyword search (mềm, giúp tiếng Việt/đặc trưng)
        # dùng token raw uppercase để match trực tiếp
        if t and t in up_name:
            score += 18
        if t and t in up_spec:
            score += 12

    # 3) Bonus: nếu query trông giống SKU, mà code match mạnh -> đẩy lên
    if norm_q and looks_like_sku(norm_q):
        if code_n.startswith(norm_q):
            score += 80
        if code_n == norm_q:
            score += 120

    return int(score)

def smart_search(
    df_indexed: pd.DataFrame,
    query: str,
    limit: int = 300,
    min_score: int = 30
) -> pd.DataFrame:
    """
    Search thông minh:
    - Nếu query rỗng: return df trống (caller sẽ xử lý hiển thị all)
    - Nếu query giống SKU: ưu tiên lọc nhanh bằng code trước
    - Sau đó chấm điểm + sort theo score
    """
    q = (query or "").strip()
    if not q:
        return df_indexed.iloc[0:0].copy()

    raw_tokens = tokenize_query(q)
    norm_q = normalize_sku_text(q)

    # Fast filter (giảm dataset trước khi score) để chạy nhanh với data lớn
    # Rule:
    # - Nếu norm_q đủ dài -> lọc các dòng có code/name/spec chứa norm_q (normalized)
    # - Nếu không -> lọc theo raw token contains trong up_name/up_spec/up_code
    if norm_q and len(norm_q) >= 4:
        mask = (
            df_indexed["_norm_code"].str.contains(norm_q, na=False)
            | df_indexed["_norm_name"].str.contains(norm_q, na=False)
            | df_indexed["_norm_spec"].str.contains(norm_q, na=False)
        )
        pre = df_indexed[mask].copy()
    else:
        # dùng token raw uppercase
        if not raw_tokens:
            pre = df_indexed.copy()
        else:
            m = False
            for t in raw_tokens[:6]:
                t = t.strip().upper()
                if not t:
                    continue
                m = m | df_indexed["_up_code"].str.contains(t, na=False) \
                      | df_indexed["_up_name"].str.contains(t, na=False) \
                      | df_indexed["_up_spec"].str.contains(t, na=False)
            pre = df_indexed[m].copy() if isinstance(m, pd.Series) else df_indexed.copy()

    if pre.empty:
        return pre

    # Score
    pre["_score"] = pre.apply(lambda r: score_row(norm_q, raw_tokens, r), axis=1)
    pre = pre[pre["_score"] >= min_score].copy()
    if pre.empty:
        return pre

    # Sort: score desc, then selected inventory desc (if exists later), then code
    pre = pre.sort_values(["_score", "_norm_code"], ascending=[False, True])

    return pre.head(limit).copy()

def render_sku_summary_card(row: pd.Series, rename_map: dict) -> None:
    """
    Hiển thị 1 SKU dưới dạng card (Executive-friendly) khi search ra 1 dòng.
    """
    st.markdown("### ✅ Kết quả (SKU)")
    c1, c2 = st.columns([3, 2])

    with c1:
        st.markdown(f"**Mã SKU:** `{row.get('product_code','')}`")
        st.markdown(f"**Tên:** {row.get('product_name','')}")
        if row.get("spec", ""):
            st.markdown(f"**Quy cách:** {row.get('spec','')}")
    with c2:
        # mini KPI theo kho
        st.markdown("**Tồn kho theo địa điểm**")
        for k in QTY_COLS:
            st.write(f"- {rename_map.get(k, k)}: **{fmt_int(row.get(k, 0))}**")

# ===================== GLOBAL HEADER + UPDATE =====================
st.title("📊 Dashboard PISCO – Executive")

col_u1, col_u2 = st.columns([1, 4])

if "is_updating" not in st.session_state:
    st.session_state["is_updating"] = False

with col_u1:
    clicked = st.button(
        "⏳ Đang cập nhật..." if st.session_state["is_updating"] else "🔄 Cập nhật dữ liệu",
        use_container_width=True,
        disabled=st.session_state["is_updating"]
    )

    if clicked:
        st.session_state["is_updating"] = True

        # ghi trạng thái ngay để tránh click 2 lần
        STATUS_FILE.write_text(
            json.dumps({
                "ts": datetime.now().isoformat(timespec="seconds"),
                "state": "running",
                "message": "Khởi động job cập nhật",
                "progress": 0
            }),
            encoding="utf-8"
        )

        subprocess.Popen(
            [sys.executable, str(UPDATE_JOB)],
            cwd=str(BASE_DIR),
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL
        )

        st.rerun()

# ===== REALTIME STATUS (POLLING) =====
status = read_update_status()

# ===================== GLOBAL UPDATE GUARD =====================
if status and status.get("state") == "running":
    st.markdown("---")
    st.subheader("⏳ Hệ thống đang cập nhật dữ liệu")
    st.info(
        "Quá trình cập nhật đang chạy ở nền.\n\n"
        "🔒 Toàn bộ chức năng tạm thời bị khóa để đảm bảo dữ liệu nhất quán.\n\n"
        "Vui lòng chờ trong giây lát…"
    )

    prog = status.get("progress")
    if prog is not None:
        st.progress(int(prog))

    # Khoảng đệm UX
    st.caption("⏱ Trang sẽ tự động cập nhật khi hoàn tất")

    time.sleep(20)
    st.rerun()


if status:
    state = status.get("state")
    msg = status.get("message", "")
    prog = status.get("progress")
    ts = status.get("ts", "")

    from datetime import datetime

    with col_u2:
        try:
            dt = datetime.fromisoformat(ts)
            st.caption("🕒 **Cập nhật lần cuối**")
            st.markdown(
                f"""
                - **Ngày:** {dt.strftime('%d/%m/%Y')}
                - **Giờ:** {dt.strftime('%H:%M:%S')}
                """)
        except Exception:
            st.caption(f"🕒 Trạng thái lúc {ts}")
        if prog is not None:
            st.progress(int(prog))

        if state == "running":
            st.info(f"⏳ Đang cập nhật: {msg}")
            if prog is not None:
                st.progress(int(prog))
            time.sleep(20)
            st.rerun()

        elif state == "ok":
            st.success(f"✅ Hoàn tất: {msg}")
            time.sleep(2)
            try:
                STATUS_FILE.unlink()
            except Exception:
                pass
            st.session_state["is_updating"] = False
            st.rerun()

        elif state == "error":
            st.error(f"❌ Lỗi: {msg}")
            try:
                STATUS_FILE.unlink()
            except Exception:
                pass
            st.session_state["is_updating"] = False


       
    

# ===================== SIDEBAR NAV =====================
if "meeting_mode" not in st.session_state:
    st.session_state["meeting_mode"] = MEETING_MODE_DEFAULT

if not st.session_state["meeting_mode"]:
    st.sidebar.header("📌 Chức năng")

    page = st.sidebar.radio(
        "Chọn trang",
        options=[
            "📊 Phân tích tồn kho",
            "🔍 Tra cứu dữ liệu",
        ],
        index=1,
        key="page_nav"
    )

    st.sidebar.divider()
    st.sidebar.header("⚙️ Tùy chọn")

    warehouse_options = list(WAREHOUSE_LABEL.keys())

    default_wh = (
        warehouse_options.index("pisco_jp_qty")
        if "pisco_jp_qty" in warehouse_options
        else 0
    )

    qty_col = st.sidebar.selectbox(
        "Chọn kho / địa điểm",
        options=warehouse_options,
        index=default_wh,
        format_func=lambda x: WAREHOUSE_LABEL[x],
        key="select_warehouse"
    )

    st.caption(f"🔎 Đang xem tồn kho: **{WAREHOUSE_LABEL[qty_col]}**")
    if page == "🔍 Tra cứu tồn kho chi tiết":
        st.sidebar.divider()
        st.sidebar.header("🧹 Lọc")
        filter_positive_only = st.sidebar.checkbox(
            f"Chỉ SKU có tồn > 0 ({WAREHOUSE_LABEL[qty_col]})",
            value=True
        )
        min_qty = st.sidebar.number_input(
            f"Ngưỡng tồn tối thiểu ({WAREHOUSE_LABEL[qty_col]})",
            min_value=0,
            value=0,
            step=10
        )
else:
    # Meeting mode defaults
    page = "🔍 Tra cứu tồn kho chi tiết"
    qty_col = "total_qty"
    

# ===== DEFAULT FILTER VALUES (ALWAYS DEFINED) =====
filter_positive_only = True
min_qty = 0

# ===================== LOAD + CLEAN DATA (ONE TIME) =====================
raw, meta_src = load_data()
if raw is None or raw.empty:
    st.warning("Chưa có dữ liệu. Hãy bấm “Cập nhật dữ liệu” để chạy job/crawler.")
    st.stop()

raw = normalize(raw)

clean_df, total_row, meta_total = split_total_row(
    raw,
    qty_cols=QTY_COLS,
    code_col="product_code",
    name_col="product_name",
    spec_col="spec",
    tolerance=0,
    min_confidence=0.80,
    last_k_rows=8,
)

snapshot_at_display = meta_src.get("snapshot_at") or (clean_df["snapshot_at"].iloc[0] if "snapshot_at" in clean_df.columns else "-")
status_display = meta_src.get("status")

# Header caption (Executive-friendly)
st.caption(
    f"📅 Dữ liệu ngày: {snapshot_at_display}"
    + (f" | 🟢 Trạng thái: {status_display}" if status_display else "")
    + f" | ⏱ Hiển thị lúc: {datetime.now().strftime('%d/%m/%Y %H:%M:%S')}"
)



# ===================== PAGE: ANALYSIS =====================
if page == "📊 Phân tích tồn kho":
    st.subheader("📌 Chỉ số tổng quan (Executive)")

    total_all = int(clean_df["total_qty"].sum())
    selected_total = int(clean_df[qty_col].sum())
    pct_share = (selected_total / total_all * 100) if total_all > 0 else 0.0
    top10_share = kpi_top_share(clean_df, qty_col, top_n=10)
    sku80 = kpi_sku_for_80(clean_df, qty_col)
    total_sku = clean_df["product_code"].nunique()

    # Executive KPI: giảm nhiễu
    if UX_MODE == "EXECUTIVE":
        k1, k2, k3 = st.columns(3)
        with k1:
            st.metric(f"Tổng tồn kho ({WAREHOUSE_LABEL[qty_col]})", fmt_int(selected_total))
            st.caption("Tổng số lượng tồn tại kho đang chọn")

        with k2:
            st.metric("SKU tạo 80% tồn kho", f"{sku80:,} / {total_sku:,}")
            st.caption("Số SKU tạo ra 80% tồn kho (nhóm cần tập trung quản lý)")

        with k3:
            st.metric(f"Tỷ trọng {WAREHOUSE_LABEL[qty_col]} / Tổng", fmt_pct(pct_share))
            st.caption("Kho này chiếm bao nhiêu % tổng tồn kho toàn hệ thống")
    else:
        k1, k2, k3, k4 = st.columns(4)
        with k1:
            st.metric(f"Tổng tồn kho ({WAREHOUSE_LABEL[qty_col]})", fmt_int(selected_total))
        with k2:
            st.metric(f"Tỷ trọng {WAREHOUSE_LABEL[qty_col]} / Tổng", fmt_pct(pct_share))
        with k3:
            st.metric("10 SKU lớn nhất chiếm", fmt_pct(top10_share))
        with k4:
            st.metric("SKU cần quản lý chính", f"{sku80:,} / {total_sku:,}")

    st.subheader("📋 Top 20 SKU tồn kho cao nhất")
    top20_df = (
        clean_df.sort_values(qty_col, ascending=False)
        .head(20)[["product_code", "product_name", "spec", qty_col]]
        .rename(columns={qty_col: f"Tồn ({WAREHOUSE_LABEL[qty_col]})"})
    )

    # Executive caption: giải thích “vì sao quan trọng”
    if selected_total > 0:
        top20_sum = int(clean_df.sort_values(qty_col, ascending=False).head(20)[qty_col].sum())
        share = (top20_sum / selected_total * 100) if selected_total else 0
        st.caption(f"20 SKU này chiếm khoảng {share:.1f}% tồn kho kho đang chọn. Đây là nhóm nên kiểm soát vòng quay và kế hoạch nhập/xả.")
    st.dataframe(top20_df, use_container_width=True, height=420)

    st.subheader("🔎 So sánh với quá khứ (theo snapshot)")
    if not DB_PATH.exists():
        st.info("Chưa có DB để so sánh snapshot.")
    else:
        conn = sqlite3.connect(DB_PATH)
        try:
            snaps = list_snapshots(conn)
            if snaps.empty or "snapshot_id" not in snaps.columns:
                st.info("Chưa có lịch sử snapshot.")
            else:
                snaps = snaps.copy()
                snaps["snapshot_at"] = snaps["snapshot_at"].astype(str)
                snaps["label"] = snaps["snapshot_at"].fillna("") + snaps["snapshot_id"].apply(lambda x: f"  (ID: {x})")

                current_sid = meta_src.get("snapshot_id") or (
                    clean_df["snapshot_id"].iloc[0] if "snapshot_id" in clean_df.columns and len(clean_df) else None
                )

                snap_options = snaps["label"].tolist()
                snap_map = dict(zip(snaps["label"], snaps["snapshot_id"]))
                snap_at_map = dict(zip(snaps["snapshot_id"], snaps["snapshot_at"]))

                # default chọn snapshot gần nhất trước đó (khác current)
                default_idx = 0
                if current_sid:
                    idxs = snaps.index[snaps["snapshot_id"] == current_sid].tolist()
                    if idxs:
                        i = idxs[0]
                        if i + 1 < len(snaps):
                            default_idx = i + 1

                compare_label = st.selectbox(
                    "Chọn snapshot quá khứ",
                    options=snap_options,
                    index=min(default_idx, len(snap_options) - 1),
                    key="compare_snapshot_select"
                )
                compare_sid = snap_map.get(compare_label)

                if compare_sid == current_sid:
                    st.warning("Bạn đang chọn snapshot hiện tại. Hãy chọn snapshot khác.")
                else:
                    old_df = pd.read_sql(
                        "SELECT * FROM inventory_data WHERE snapshot_id = ?",
                        conn,
                        params=(compare_sid,)
                    )
                    if old_df.empty:
                        st.info("Snapshot quá khứ không có dữ liệu.")
                    else:
                        old_df = normalize(old_df)
                        old_clean, _, _ = split_total_row(
                            old_df,
                            qty_cols=QTY_COLS,
                            code_col="product_code",
                            name_col="product_name",
                            spec_col="spec",
                            tolerance=0,
                            min_confidence=0.80,
                            last_k_rows=8,
                        )

                        cmp_df = build_compare(clean_df, old_clean, qty_col)

                        now_sum = int(clean_df[qty_col].sum())
                        old_sum = int(old_clean[qty_col].sum())
                        delta_sum = now_sum - old_sum

                        c1, c2, c3 = st.columns(3)
                        with c1: st.metric("Tồn hiện tại", fmt_int(now_sum))
                        with c2: st.metric("Tồn quá khứ", fmt_int(old_sum))
                        with c3: st.metric("Chênh lệch", fmt_int(delta_sum))

                        # Lọc nhiễu biến động nhỏ (Executive)
                       

                        with st.expander("🧹 Lọc nhiễu biến động (Executive)", expanded=False):
                            delta_abs_min = st.number_input("Ngưỡng |delta| tối thiểu", min_value=0, value=50, step=10)
                            delta_pct_min = st.number_input("Ngưỡng |delta%| tối thiểu", min_value=0.0, value=10.0, step=1.0)
                        filtered_cmp = cmp_df[
                            (cmp_df["delta"].abs() >= int(delta_abs_min)) | (cmp_df["delta_pct"].abs() >= float(delta_pct_min))
                        ].copy()

                        inc = filtered_cmp.sort_values("delta", ascending=False).head(15)
                        dec = filtered_cmp.sort_values("delta", ascending=True).head(15)

                        left, right = st.columns(2)
                        with left:
                            st.markdown("**📈 Top tăng tồn kho**")
                            st.dataframe(
                                inc[["product_code", "product_name", "spec", "qty_old", "qty_now", "delta", "delta_pct"]],
                                use_container_width=True,
                                height=380
                            )
                        with right:
                            st.markdown("**📉 Top giảm tồn kho**")
                            st.dataframe(
                                dec[["product_code", "product_name", "spec", "qty_old", "qty_now", "delta", "delta_pct"]],
                                use_container_width=True,
                                height=380
                            )
        finally:
            conn.close()

    st.subheader("⬇️ Xuất báo cáo Excel")
    overview_df = pd.DataFrame([{
        "Snapshot": snapshot_at_display,
        "Kho đang chọn": WAREHOUSE_LABEL[qty_col],
        "Tồn kho kho chọn": int(clean_df[qty_col].sum()),
        "% kho / tổng": round((clean_df[qty_col].sum() / clean_df["total_qty"].sum() * 100) if clean_df["total_qty"].sum() else 0, 2),
        "Top 10 chiếm (%)": round(kpi_top_share(clean_df, qty_col, 10), 2),
        "SKU tạo 80% tồn kho": kpi_sku_for_80(clean_df, qty_col),
        "Nguồn": meta_src.get("source"),
    }])

    xlsx_bytes = to_excel_bytes({
        "Tong_quan": overview_df,
        "Top20_SKU": top20_df,
    })

    section(
        title="Xuất dữ liệu",
        icon="⬇️",
        desc="Tải file Excel đúng theo dữ liệu đang hiển thị trên màn hình"
    )

    st.download_button(
        label="📥 Tải Excel (Tổng quan + Top20)",
        data=xlsx_bytes,
        file_name=f"bao_cao_ton_kho_{WAREHOUSE_LABEL[qty_col]}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        use_container_width=True
    )

    st.subheader("⚖️ So sánh 2 kho song song")

    c1, c2 = st.columns(2)
    with c1:
        wh_left = st.selectbox(
            "Kho A",
            options=list(WAREHOUSE_LABEL.keys()),
            format_func=lambda x: WAREHOUSE_LABEL[x],
            key="compare_wh_left"
        )

    with c2:
        wh_right = st.selectbox(
            "Kho B",
            options=list(WAREHOUSE_LABEL.keys()),
            format_func=lambda x: WAREHOUSE_LABEL[x],
            index=1,
            key="compare_wh_right"
        )
    compare_df = clean_df[
        ["product_code", "product_name", "spec", wh_left, wh_right]
    ].copy()

    compare_df["Chênh lệch"] = compare_df[wh_left] - compare_df[wh_right]

    compare_df = compare_df.rename(columns={
        "product_code": "Mã SKU",
        "product_name": "Tên sản phẩm",
        "spec": "Quy cách",
        wh_left: WAREHOUSE_LABEL[wh_left],
        wh_right: WAREHOUSE_LABEL[wh_right],
    })

    compare_df = compare_df[
        (compare_df[WAREHOUSE_LABEL[wh_left]] +
        compare_df[WAREHOUSE_LABEL[wh_right]]) > 0
    ]

    st.dataframe(
        compare_df.sort_values("Chênh lệch", ascending=False),
        use_container_width=True,
        height=520
    )
# ===================== PAGE: CRAWLER DATA BROWSER =====================
else:
    # ===== MEETING MODE TOGGLE =====
    c1, c2 = st.columns([6, 1])
    with c1:
        st.subheader("🔍 Tra cứu tồn kho chi tiết")
    with c2:
        if st.button("🎤 Meeting Mode" if not st.session_state["meeting_mode"] else "❌ Thoát họp"):
            st.session_state["meeting_mode"] = not st.session_state["meeting_mode"]
            st.rerun()

    

    display_cols = ["product_code", "product_name", "spec"] + QTY_COLS
    browser_df = clean_df[display_cols].copy()

    rename_map = {
        "product_code": "Mã SKU",
        "product_name": "Tên sản phẩm",
        "spec": "Quy cách",
        "total_qty": "Tổng",
        "mindman_qty": "Mindman",
        "pisco_jp_qty": "NIHON PISCO",
        "pisco_kr_qty": "Pisco KR",
        "pisco_tw_qty": "Pisco TW",
        "totra_qty": "Totra",
    }

    # ===== SEARCH INPUT (MULTI-SKU) =====

    
    search_block = st.text_area(
        "",
        placeholder="Nhập mã hoặc tên để tìm kiếm\nCách nhau bằng dấu phẩy hoặc xuống hàng",
        height=90,
        label_visibility="collapsed"
    )

# ===== CONTROL: WHEN TO SHOW TABLE =====
    show_table = False

    # Có search text
    if search_block.strip():
        show_table = True
    # ===== FILTER =====
    if filter_positive_only:
        browser_df = browser_df[browser_df[qty_col] > 0].copy()
    if min_qty > 0:
        browser_df = browser_df[browser_df[qty_col] >= int(min_qty)].copy()

    indexed = prepare_search_index(browser_df)

    # ===== MULTI QUERY PARSE + LIMIT =====
    queries = []
    if search_block.strip():
        raw_lines = re.split(r"[\n,]+", search_block)
        queries = [q.strip() for q in raw_lines if q.strip()]
        if len(queries) > 50:
            st.info("Bạn dán nhiều hơn 50 dòng. Hệ thống sẽ xử lý 50 dòng đầu để đảm bảo tốc độ.")
            queries = queries[:50]

    if queries:
        frames = []
        for q in queries:
            r = smart_search(indexed, q, limit=300, min_score=30)
            frames.append(r)
        tmp = pd.concat(frames, ignore_index=True)

        # giữ SKU có score cao nhất
        if "_score" in tmp.columns:
            tmp = tmp.sort_values("_score", ascending=False)

        result = tmp.drop_duplicates(subset=["product_code"], keep="first").copy()
    else:
        result = indexed.copy()

    if result.empty:
        st.warning("❌ Không có dữ liệu phù hợp.")
        st.stop()

    # ===== SUM ROW (trên raw columns) =====
    sum_row = {c: int(result[c].sum()) for c in QTY_COLS}
    sum_row.update({"product_code": "TỔNG CỘNG", "product_name": "", "spec": ""})

    # ===== HIỂN THỊ: rename qty_col → tồn kho đang chọn =====
    result_show = result.drop(
        columns=["_norm_code", "_norm_name", "_norm_spec", "_up_code", "_up_name", "_up_spec"],
        errors="ignore"
    ).copy()

    ordered_cols = ["product_code", "product_name", "spec", qty_col] + [c for c in QTY_COLS if c != qty_col]
    result_show = result_show[ordered_cols]

    qty_col_display = f"Tồn ({WAREHOUSE_LABEL[qty_col]})"
    result_show = result_show.rename(columns={qty_col: qty_col_display}).rename(columns=rename_map)

    # ===== HIGHLIGHT LOGIC (PHẢI LÀM TRƯỚC KHI st.dataframe) =====
    # Overstock
    top_pct = 0.10
    abs_threshold = 1000

    top_n = max(1, int(len(result_show) * top_pct))
    overstock_count = (
        result_show[qty_col_display] >= abs_threshold
    ).sum()

    shortage_count = (
        clean_df["shortage_qty"] > 0
    ).sum()

  

 # ===== FILTER HEADER (NUMERIC) =====
    numeric_cols = [
        c for c in result_show.columns
        if c not in ["Mã SKU", "Tên sản phẩm", "Quy cách"]
    ]
   
    with st.expander("🔧 Lọc dữ liệu trong bảng", expanded=False):
        f1, f2, f3, f4 = st.columns([3, 2, 2, 2])

        with f1:
            filter_col = st.selectbox(
                "Cột",
                options=numeric_cols,
                key="tbl_filter_col"
            )

        with f2:
            filter_op = st.selectbox(
                "Điều kiện",
                options=[">", "<", ">=", "<=", "=", "Between"],
                key="tbl_filter_op"
            )

        with f3:
            filter_val1 = st.number_input(
                "Giá trị",
                value=0,
                key="tbl_filter_val1"
            )

        with f4:
            filter_val2 = st.number_input(
                "Đến",
                value=0,
                key="tbl_filter_val2"
            )

    # ===== APPLY FILTER =====
    filtered_show = result_show.copy()

    if filter_op == ">":
        filtered_show = filtered_show[filtered_show[filter_col] > filter_val1]

    elif filter_op == "<":
        filtered_show = filtered_show[filtered_show[filter_col] < filter_val1]

    elif filter_op == ">=":
        filtered_show = filtered_show[filtered_show[filter_col] >= filter_val1]

    elif filter_op == "<=":
        filtered_show = filtered_show[filtered_show[filter_col] <= filter_val1]

    elif filter_op == "=":
        filtered_show = filtered_show[filtered_show[filter_col] == filter_val1]

    elif filter_op == "Between":
        lo = min(filter_val1, filter_val2)
        hi = max(filter_val1, filter_val2)
        filtered_show = filtered_show[
            (filtered_show[filter_col] >= lo) &
            (filtered_show[filter_col] <= hi)
        ]
    
    # ===== SUMMARY (DYNAMIC - FOLLOW FILTERED DATA) =====
    if show_table:
        section(
            title="Kết quả tồn kho (theo tìm kiếm)",
            icon="📌",
            desc="Tổng số lượng tồn kho dựa trên kết quả tìm kiếm và bộ lọc"
        )

        # Xác định lại cột tồn kho đang chọn
        qty_col_display = f"Tồn ({WAREHOUSE_LABEL[qty_col]})"

        summary_map = {}

        # 1. Tổng tồn theo kho đang chọn
        if qty_col_display in filtered_show.columns:
            summary_map[qty_col_display] = int(filtered_show[qty_col_display].sum())

        # 2. Các kho còn lại
        for c in QTY_COLS:
            label = rename_map.get(c, c)
            if label in filtered_show.columns and label != qty_col_display:
                summary_map[label] = int(filtered_show[label].sum())

        # 3. Render summary bar
        cols = st.columns(len(summary_map))

        for i, (label, val) in enumerate(summary_map.items()):
            is_primary = (label == WAREHOUSE_LABEL[qty_col])
            tile_class = "kpi-tile primary" if is_primary else "kpi-tile"

            cols[i].markdown(
                f"""
                <div class="{tile_class}">
                    <div class="label">{label}</div>
                    <div class="value">{val:,}</div>
                </div>
                """,
                unsafe_allow_html=True
            )


    # ===== MAIN TABLE =====
    if show_table:
        st.caption(f"Hiển thị {len(filtered_show):,} SKU")

        left_info, right_info = st.columns([2.2, 1], gap="small")
        with left_info:
            st.success(f"✅ {len(filtered_show):,} SKU")
        with right_info:
            st.caption("Mẹo: click tên cột để sort nhanh")
        st.subheader("📋 Danh sách SKU phù hợp")
        st.caption("Danh sách chi tiết khớp với kết quả tổng hợp phía trên")
        st.dataframe(
            filtered_show,
            use_container_width=True,
            height=460
        )
    


    


    # ===== EXPORT (Excel = đúng như màn hình + thêm dòng tổng) =====
    export_df = filtered_show.copy()
    total_row_export = {c: "" for c in export_df.columns}
    total_row_export["Trạng thái"] = ""
    total_row_export["Mã SKU"] = "TỔNG CỘNG"
    total_row_export[qty_col_display] = int(result[qty_col].sum())

    # map các kho còn lại theo rename_map
    for c in QTY_COLS:
        label = rename_map.get(c, c)
        if label in export_df.columns and label != qty_col_display:
            total_row_export[label] = int(result[c].sum())

    export_df = pd.concat([export_df, pd.DataFrame([total_row_export])], ignore_index=True)

    xlsx_bytes = to_excel_bytes({"Crawler_Result": export_df})
    if show_table:
        section(
            title="Xuất dữ liệu",
            icon="⬇️",
            desc="Tải file Excel đúng theo dữ liệu đang hiển thị trên màn hình"
        )
        st.download_button(
            label="📥 Tải Excel (Đúng như màn hình)",
            data=xlsx_bytes,
            file_name=f"tra_cuu_crawler_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            use_container_width=True
        )
