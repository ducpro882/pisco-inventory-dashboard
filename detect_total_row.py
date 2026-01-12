#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
detect_total_row.py
===================
Module nhận diện và loại dòng "Tổng cộng" trong Excel/CSV theo tư duy Data Analysis Pro.

Vấn đề thực tế:
- Nhiều file Excel có 1 dòng cuối là tổng của toàn bộ SKU phía trên.
- Nếu SUM mà không loại dòng này => double-count (tổng bị gấp đôi).

Thiết kế:
- Không phụ thuộc chữ "Tổng cộng".
- Ưu tiên kiểm chứng toán học: dòng ứng viên phải ~ bằng tổng của các dòng còn lại.
- Scoring để tăng độ bền: keyword, vị trí cuối, text rỗng, đầy đủ số qty.

API chính:
- detect_total_row(df, qty_cols, code_col='product_code', name_col='product_name', spec_col='spec', tolerance=0)
- split_total_row(df, qty_cols, ...) -> (df_clean, total_row_series_or_None, meta_dict)

Khuyến nghị:
- Luôn chạy split_total_row trước mọi KPI / SUM / ABC / Concentration.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import List, Optional, Dict, Any, Tuple
import numpy as np
import pandas as pd


DEFAULT_KEYWORDS = {
    "tổng cộng", "tong cong", "tổng", "tong", "total", "grand total", "overall", "sum"
}

@dataclass
class TotalRowResult:
    found: bool
    row_index: Optional[int]
    confidence: float
    score: float
    reasons: List[str]


def _to_int_series(s: pd.Series) -> pd.Series:
    return pd.to_numeric(s, errors="coerce").fillna(0).astype("int64")


def detect_total_row(
    df: pd.DataFrame,
    qty_cols: List[str],
    code_col: str = "product_code",
    name_col: str = "product_name",
    spec_col: str = "spec",
    keywords: Optional[set] = None,
    tolerance: int = 0,
    min_confidence: float = 0.80,
    last_k_rows: int = 8,
) -> TotalRowResult:
    """
    Nhận diện dòng tổng bằng scoring + kiểm chứng toán học.

    Logic (tóm tắt):
    - Xét các dòng ứng viên ưu tiên ở cuối bảng (last_k_rows).
    - Tính sum_others = SUM(qty_cols) của toàn bộ bảng trừ dòng ứng viên.
    - Nếu row[qty_cols] ≈ sum_others (tolerance) => tín hiệu mạnh nhất.
    - Thêm điểm nếu:
        + keyword trong code/name/spec
        + name/spec rỗng
        + dòng nằm cuối
        + đầy đủ số qty

    Returns: TotalRowResult
    """
    if df is None or df.empty:
        return TotalRowResult(False, None, 0.0, 0.0, ["DataFrame rỗng."])

    keywords = keywords or DEFAULT_KEYWORDS

    # đảm bảo qty cols tồn tại
    qty_cols = [c for c in qty_cols if c in df.columns]
    if not qty_cols:
        return TotalRowResult(False, None, 0.0, 0.0, ["Không tìm thấy qty_cols trong DataFrame."])

    work = df.copy()

    # text normalize
    for c in [code_col, name_col, spec_col]:
        if c not in work.columns:
            work[c] = ""
        work[c] = work[c].astype(str).fillna("").str.strip()

    # numeric normalize
    for c in qty_cols:
        work[c] = _to_int_series(work[c])

    total_sum_all = work[qty_cols].sum(axis=0)

    # candidate indices: ưu tiên cuối file (vì "tổng cộng" thường ở cuối)
    n = len(work)
    start = max(0, n - int(last_k_rows))
    candidates = list(range(start, n))

    best = TotalRowResult(False, None, 0.0, 0.0, ["Không tìm thấy ứng viên phù hợp."])

    for idx in candidates:
        row = work.iloc[idx]
        reasons: List[str] = []
        score = 0.0

        # Keyword signal
        blob = " ".join([row[code_col], row[name_col], row[spec_col]]).lower()
        if any(k in blob for k in keywords):
            score += 3.0
            reasons.append("Có keyword (Tổng/Total/Grand Total/...).")

        # text empty (aggregate rows thường trống name/spec)
        if (row[name_col].strip() == "" or row[name_col].lower() == "nan") and (row[spec_col].strip() == "" or row[spec_col].lower() == "nan"):
            score += 2.0
            reasons.append("product_name & spec rỗng (mẫu điển hình dòng tổng).")

        # located near end
        if idx >= n - 2:
            score += 1.0
            reasons.append("Nằm ở cuối bảng.")

        # full numeric presence (không NaN)
        if np.isfinite(row[qty_cols].astype(float)).all():
            score += 1.0
            reasons.append("Đầy đủ số qty (không NaN).")

        # strongest: math equality across qty cols
        sum_others = total_sum_all - row[qty_cols]
        diffs = (row[qty_cols] - sum_others).abs()

        if int(diffs.max()) <= int(tolerance):
            score += 5.0
            reasons.append(f"Khớp toán học: row[qty] == SUM(others) (tolerance={tolerance}).")

        # confidence mapping: clamp(score/10)
        confidence = float(min(1.0, score / 10.0))

        # keep best
        if score > best.score:
            best = TotalRowResult(True, idx, confidence, score, reasons)

    # decision
    if best.confidence >= min_confidence:
        best.found = True
        return best

    # nếu chưa đủ confidence, coi như không chắc chắn
    return TotalRowResult(False, best.row_index, best.confidence, best.score, best.reasons)


def split_total_row(
    df: pd.DataFrame,
    qty_cols: List[str],
    code_col: str = "product_code",
    name_col: str = "product_name",
    spec_col: str = "spec",
    tolerance: int = 0,
    min_confidence: float = 0.80,
    last_k_rows: int = 8,
) -> Tuple[pd.DataFrame, Optional[pd.Series], Dict[str, Any]]:
    """
    Tách dòng tổng nếu phát hiện đủ tự tin.

    Returns:
    - df_clean: DataFrame đã loại dòng tổng
    - total_row: Series dòng tổng (nếu có)
    - meta: dict thông tin phát hiện (confidence, reasons...)
    """
    res = detect_total_row(
        df=df,
        qty_cols=qty_cols,
        code_col=code_col,
        name_col=name_col,
        spec_col=spec_col,
        tolerance=tolerance,
        min_confidence=min_confidence,
        last_k_rows=last_k_rows,
    )
    meta = {
        "found": res.found,
        "row_index": res.row_index,
        "confidence": res.confidence,
        "score": res.score,
        "reasons": res.reasons,
    }

    if not res.found or res.row_index is None:
        return df.copy(), None, meta

    total_row = df.iloc[res.row_index]
    df_clean = df.drop(df.index[res.row_index]).copy()
    return df_clean, total_row, meta
