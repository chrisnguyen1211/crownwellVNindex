#!/usr/bin/env python3
"""
EDA for screener CSVs using pandas.

Outputs:
- summary_columns.csv: dtype, missing, unique, min/max, sample
- anomalies.csv: flagged anomalies (EPS unit issues, current/quick ratio outliers, negative values)
- distribution_stats.csv: mean, std, percentiles for numeric columns
- report.html: compact HTML summary for quick viewing

Usage:
  python3 eda_analysis.py --input "/Users/nguyenhuycuong/Downloads/2025-10-01T05-52_export.csv" --outdir "/Users/nguyenhuycuong/Downloads/eda_report"
"""

from __future__ import annotations

import argparse
import os
import sys
from typing import List, Tuple

import pandas as pd
import numpy as np


def ensure_outdir(path: str) -> None:
    os.makedirs(path, exist_ok=True)


def read_csv(input_path: str) -> pd.DataFrame:
    try:
        return pd.read_csv(input_path)
    except Exception as exc:
        print(f"Failed to read CSV: {exc}", file=sys.stderr)
        raise


def summarize_columns(df: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for col in df.columns:
        s = df[col]
        dtype = str(s.dtype)
        missing = int(s.isna().sum())
        unique = int(s.nunique(dropna=True))
        sample = None
        try:
            sample = s.dropna().iloc[0]
        except Exception:
            sample = None
        stats = {
            "column": col,
            "dtype": dtype,
            "missing": missing,
            "unique": unique,
            "sample": sample,
        }
        if pd.api.types.is_numeric_dtype(s):
            stats.update({
                "min": float(np.nanmin(s.values)) if s.notna().any() else np.nan,
                "max": float(np.nanmax(s.values)) if s.notna().any() else np.nan,
                "mean": float(np.nanmean(s.values)) if s.notna().any() else np.nan,
            })
        rows.append(stats)
    return pd.DataFrame(rows)


def normalize_eps(eps_val: float) -> float:
    if pd.isna(eps_val):
        return np.nan
    try:
        v = float(eps_val)
    except Exception:
        return np.nan
    if v <= 0:
        return v
    # Heuristic: many VN sources return EPS in thousand VND
    return v * 1000.0 if v < 1000 else v


def detect_anomalies(df: pd.DataFrame) -> pd.DataFrame:
    anomalies = []

    # EPS anomalies
    if 'eps' in df.columns:
        eps = pd.to_numeric(df['eps'], errors='coerce')
        eps_norm = eps.apply(normalize_eps)
        mask_eps_unit = (eps.notna()) & (eps < 1000)
        mask_eps_extreme = eps_norm > 1_000_000
        for idx in df.index[mask_eps_unit | mask_eps_extreme]:
            anomalies.append({
                "row": int(idx),
                "column": "eps",
                "value": df.loc[idx, 'eps'],
                "issue": "eps_maybe_thousand_unit" if mask_eps_unit.loc[idx] else "eps_extreme_value",
                "suggested_normalized": float(eps_norm.loc[idx]) if pd.notna(eps_norm.loc[idx]) else np.nan,
            })

    # Liquidity ratios
    for col, name in [("current_ratio", "current_ratio"), ("quick_ratio", "quick_ratio")]:
        if col in df.columns:
            vals = pd.to_numeric(df[col], errors='coerce')
            mask_negative = vals < 0
            mask_extreme = vals > 10
            for idx in df.index[mask_negative | mask_extreme]:
                anomalies.append({
                    "row": int(idx),
                    "column": name,
                    "value": df.loc[idx, col],
                    "issue": "negative" if mask_negative.loc[idx] else "too_large(>10)",
                })

    # Negative or zero prices
    if 'price_vnd' in df.columns:
        pv = pd.to_numeric(df['price_vnd'], errors='coerce')
        mask_bad = (pv <= 0) | (pv.isna())
        for idx in df.index[mask_bad]:
            anomalies.append({
                "row": int(idx),
                "column": "price_vnd",
                "value": df.loc[idx, 'price_vnd'],
                "issue": "missing_or_nonpositive_price",
            })

    return pd.DataFrame(anomalies)


def distribution_stats(df: pd.DataFrame) -> pd.DataFrame:
    num_df = df.select_dtypes(include=[np.number]).copy()
    if num_df.empty:
        return pd.DataFrame()
    desc = num_df.describe(percentiles=[0.05, 0.25, 0.5, 0.75, 0.95]).T
    desc = desc.rename(columns={
        "50%": "p50",
        "25%": "p25",
        "75%": "p75",
        "5%": "p05",
        "95%": "p95",
    })
    return desc.reset_index().rename(columns={"index": "column"})


def build_html_report(summary_df: pd.DataFrame, anomalies_df: pd.DataFrame, dist_df: pd.DataFrame, outdir: str) -> str:
    html_parts: List[str] = []
    html_parts.append("<h2>EDA Summary</h2>")
    html_parts.append("<h3>Columns</h3>")
    html_parts.append(summary_df.to_html(index=False))
    html_parts.append("<h3>Anomalies</h3>")
    if anomalies_df is not None and not anomalies_df.empty:
        html_parts.append(anomalies_df.to_html(index=False))
    else:
        html_parts.append("<p>No anomalies detected.</p>")
    html_parts.append("<h3>Distribution Stats</h3>")
    if dist_df is not None and not dist_df.empty:
        html_parts.append(dist_df.to_html(index=False))
    else:
        html_parts.append("<p>No numeric columns found.</p>")

    html = "\n".join(html_parts)
    path = os.path.join(outdir, "report.html")
    with open(path, "w", encoding="utf-8") as f:
        f.write(html)
    return path


def main() -> None:
    parser = argparse.ArgumentParser(description="Pandas-based EDA for screener CSVs")
    parser.add_argument("--input", required=True, help="Input CSV path")
    parser.add_argument("--outdir", required=True, help="Output directory for EDA artifacts")
    args = parser.parse_args()

    ensure_outdir(args.outdir)
    df = read_csv(args.input)

    summary_df = summarize_columns(df)
    summary_df.to_csv(os.path.join(args.outdir, "summary_columns.csv"), index=False)

    anomalies_df = detect_anomalies(df)
    anomalies_df.to_csv(os.path.join(args.outdir, "anomalies.csv"), index=False)

    dist_df = distribution_stats(df)
    dist_df.to_csv(os.path.join(args.outdir, "distribution_stats.csv"), index=False)

    report_path = build_html_report(summary_df, anomalies_df, dist_df, args.outdir)
    print(f"EDA report written to: {report_path}")


if __name__ == "__main__":
    main()










