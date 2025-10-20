#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
analyze_openreview.py

Quick analysis for OpenReview crawl outputs (JSONL).
Reads JSONL files in --indir and writes CSVs + Markdown report to --outdir.

Outputs:
  - reviews_by_paper.csv
  - reviews_per_paper_distribution.csv
  - rating_summary.csv (if rating-like fields exist)
  - review_length_summary.csv
  - decision_breakdown.csv (if decisions exist)
  - reviews_enriched.csv (word count, reply flags)
  - threads_by_paper.csv (thread stats per paper)
  - sample_threads.md (example threads)
  - summary.md  (human-friendly report)
"""
from __future__ import annotations

import argparse
import os
import re
import sys
from collections import defaultdict, deque
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import pandas as pd


# ---------- utils ----------
def read_jsonl(p: Path) -> pd.DataFrame:
    if not p.exists():
        return pd.DataFrame()
    try:
        return pd.read_json(p, lines=True, dtype_backend="pyarrow")
    except Exception:
        return pd.read_json(p, lines=True)


def extract_numeric_rating(x) -> Optional[float]:
    """Extract leading numeric (e.g. 7 from '7: Accept')."""
    if pd.isna(x):
        return None
    if isinstance(x, (int, float)):
        try:
            return float(x)
        except Exception:
            return None
    s = str(x)
    m = re.search(r"([0-9]+(?:\.[0-9]+)?)", s)
    return float(m.group(1)) if m else None


def _safe_str(v) -> str:
    return "" if pd.isna(v) else str(v)


# ---------- threading helpers ----------
def build_threads(df: pd.DataFrame) -> Tuple[Dict[str, List[str]], Dict[str, int], Dict[str, int]]:
    """
    Build global child map & depth by BFS. Also compute max depth per forum.
    """
    if df.empty:
        return {}, {}, {}

    id_to_row = {str(r.get("id")): r for _, r in df.iterrows()}
    children: Dict[str, List[str]] = defaultdict(list)
    roots = set(id_to_row.keys())

    for _id, row in id_to_row.items():
        parent = row.get("replyto")
        if pd.notna(parent):
            p = str(parent)
            if p in id_to_row:
                children[p].append(str(_id))
                roots.discard(str(_id))

    depth: Dict[str, int] = {}
    for root in list(roots):
        if root not in id_to_row:
            continue
        q = deque([(root, 0)])
        while q:
            nid, d = q.popleft()
            if nid in depth:
                continue
            depth[nid] = d
            for ch in children.get(nid, []):
                q.append((ch, d + 1))

    forum_max_depth: Dict[str, int] = {}
    if "forum" in df.columns:
        for fid, group in df.groupby("forum"):
            md = 0
            for nid in group["id"].astype(str).tolist():
                md = max(md, depth.get(str(nid), 0))
            forum_max_depth[str(fid)] = md
    return children, depth, forum_max_depth


def summarize_threads(df_reviews: pd.DataFrame) -> Tuple[pd.DataFrame, str]:
    """
    Return per-paper thread stats and a small markdown with sample threads.
    """
    if df_reviews.empty:
        return pd.DataFrame(), "_No reviews to thread._\n"

    children, depth, forum_max_depth = build_threads(df_reviews)

    rows = []
    md_lines = ["## Sample threads (truncated)", ""]
    for forum, g in df_reviews.groupby("paper_forum"):
        ids = set(g["id"].astype(str).tolist())
        replyto_set = set(g["replyto"].dropna().astype(str).tolist()) if "replyto" in g.columns else set()
        roots = [i for i in ids if i not in replyto_set]
        max_depth = forum_max_depth.get(str(forum), 0)
        avg_depth = float(pd.Series([depth.get(i, 0) for i in ids]).mean()) if ids else 0.0
        rows.append({
            "paper_forum": forum,
            "n_reviews": len(ids),
            "n_roots": len(roots),
            "max_depth": max_depth,
            "avg_depth": round(avg_depth, 3),
        })

        # ----- sample thread -----
        md_lines.append(f"### Paper {forum}")
        if not roots:
            # pick any node as root if we don't detect a root
            roots = [next(iter(ids))]

        by_id = {str(r["id"]): r for _, r in g.iterrows()}

        def snip(s: str, n: int = 100) -> str:
            s = re.sub(r"\s+", " ", s).strip()
            return (s[:n] + "…") if len(s) > n else s

        def dfs(nid: str, indent: int = 0, limit_children: int = 8):
            r = by_id.get(nid)
            # ---- FIX for "truth value of a Series is ambiguous"
            if r is None or (hasattr(r, "empty") and r.empty):
                return
            text_fields = [k for k in r.keys() if str(k).startswith("content.") and isinstance(r[k], str)]
            sample = ""
            for k in text_fields:
                if r[k]:
                    sample = snip(str(r[k]))
                    if sample:
                        break
            md_lines.append("  " * indent + f"- `{nid}` depth={depth.get(nid,0)}  {sample}")
            # Only traverse children inside this forum
            for ch in children.get(nid, []):
                if ch in by_id:
                    dfs(ch, indent + 1, limit_children)

        dfs(str(roots[0]), 0, 8)
        md_lines.append("")
        if len(md_lines) > 60:  # keep markdown short
            break

    thread_df = pd.DataFrame(rows).sort_values(["n_reviews", "max_depth"], ascending=[False, False])
    return thread_df, "\n".join(md_lines) + "\n"


# ---------- main analysis ----------
def main(indir: str, outdir: str):
    IN = Path(indir)
    OUT = Path(outdir)
    OUT.mkdir(parents=True, exist_ok=True)

    subs  = read_jsonl(IN / "submissions.jsonl")
    revs  = read_jsonl(IN / "reviews.jsonl")
    metas = read_jsonl(IN / "meta_reviews.jsonl")
    decs  = read_jsonl(IN / "decisions.jsonl")

    n_sub, n_rev, n_meta, n_dec = map(len, (subs, revs, metas, decs))

    # reviews per paper
    if not revs.empty:
        by_forum = revs.groupby("paper_forum").size().rename("n_reviews").reset_index()
    else:
        by_forum = pd.DataFrame(columns=["paper_forum", "n_reviews"])

    # attach titles (if available)
    if "content.title" in subs.columns and "forum" in subs.columns:
        titles = subs[["forum", "content.title"]].rename(columns={"forum": "paper_forum", "content.title": "title"})
        by_forum = by_forum.merge(titles, on="paper_forum", how="left")

    # attach decisions (if available)
    if not decs.empty:
        dec_map = {}
        for _, r in decs.iterrows():
            fid = _safe_str(r.get("paper_forum") or r.get("forum") or r.get("id"))
            dec = r.get("content.decision") or r.get("content.Decision") or r.get("decision") or ""
            dec_map[fid] = dec
        by_forum["decision"] = by_forum["paper_forum"].astype(str).map(dec_map)

    by_forum.sort_values("n_reviews", ascending=False, inplace=True)
    by_forum.to_csv(OUT / "reviews_by_paper.csv", index=False)

    # distribution of #reviews per paper
    dist = by_forum["n_reviews"].value_counts().sort_index()
    dist.to_csv(OUT / "reviews_per_paper_distribution.csv")

    # rating summary (if any rating-like field exists)
    rating_cols = [c for c in revs.columns
                   if str(c).lower().startswith("content.")
                   and any(k in str(c).lower() for k in ["rating", "recommend", "score"])]
    if rating_cols:
        nums = []
        for col in rating_cols:
            series = revs[col].map(extract_numeric_rating).dropna()
            if not series.empty:
                nums.append(series.rename(col))
        if nums:
            pd.concat(nums, axis=0).astype(float).describe().to_csv(OUT / "rating_summary.csv")

    # review length (approx words) & simple enrichment
    if not revs.empty:
        def word_count_row(row) -> int:
            text_fields = [k for k in row.index if str(k).startswith("content.") and isinstance(row[k], str)]
            if not text_fields:
                return 0
            s = " ".join(str(row[k]) for k in text_fields if isinstance(row[k], str))
            return len(re.findall(r"\w+", s))

        revs["_word_count"] = revs.apply(word_count_row, axis=1)
        revs["_has_replyto"] = revs["replyto"].notna() if "replyto" in revs.columns else False
        revs[["_word_count", "paper_forum", "replyto"]].to_csv(OUT / "reviews_enriched.csv", index=False)
        revs["_word_count"].describe().to_csv(OUT / "review_length_summary.csv")

    # decision breakdown
    if "decision" in by_forum.columns:
        by_forum["decision"].fillna("").value_counts().sort_values(ascending=False).to_csv(OUT / "decision_breakdown.csv")

    # thread stats & sample threads
    threads_df, sample_threads_md = summarize_threads(revs)
    if not threads_df.empty:
        threads_df.to_csv(OUT / "threads_by_paper.csv", index=False)
    (OUT / "sample_threads.md").write_text(sample_threads_md, encoding="utf-8")

    # summary markdown
    lines = []
    lines.append(f"# OpenReview analysis for `{IN}`\n")
    lines.append(f"- **submissions**: {n_sub}")
    lines.append(f"- **reviews**: {n_rev}")
    lines.append(f"- **meta_reviews**: {n_meta}")
    lines.append(f"- **decisions**: {n_dec}\n")
    lines.append("## Reviews per paper (top 10)\n")
    if not by_forum.empty:
        cols = ["paper_forum", "n_reviews"]
        if "title" in by_forum.columns: cols.append("title")
        if "decision" in by_forum.columns: cols.append("decision")
        lines.append(by_forum[cols].head(10).to_csv(index=False))
    else:
        lines.append("_No reviews found._\n")
    (OUT / "summary.md").write_text("\n".join(lines), encoding="utf-8")

    print(f"Done. Report in: {OUT}")


def cli():
    ap = argparse.ArgumentParser()
    ap.add_argument("--indir", required=True, help="Input folder containing JSONL files")
    ap.add_argument("--outdir", default=None, help="Output folder (default: analysis/<indir_name>)")
    args = ap.parse_args()

    in_dir = Path(args.indir)
    if not in_dir.exists():
        print(f"[ERR] Input dir not found: {in_dir}", file=sys.stderr)
        sys.exit(2)

    out_dir = Path(args.outdir) if args.outdir else Path("analysis") / in_dir.name
    out_dir.mkdir(parents=True, exist_ok=True)
    main(str(in_dir), str(out_dir))


if __name__ == "__main__":
    cli()
