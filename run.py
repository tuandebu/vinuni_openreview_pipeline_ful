#!/usr/bin/env python3
import argparse, os, json, time, re
from pathlib import Path
from typing import Dict, Any, List
import requests
from dotenv import load_dotenv
import pandas as pd
from tqdm import tqdm

import openreview
from openreview import tools as or_tools

def env_login(baseurl: str) -> openreview.Client:
    load_dotenv()
    token = os.getenv("OPENREVIEW_TOKEN")
    user = os.getenv("OPENREVIEW_USERNAME")
    pwd = os.getenv("OPENREVIEW_PASSWORD")
    if token:
        return openreview.Client(baseurl=baseurl, token=token)
    if user and pwd:
        return openreview.Client(baseurl=baseurl, username=user, password=pwd)
    return openreview.Client(baseurl=baseurl)

def sanitize(s: str) -> str:
    s = s.strip().replace(" ", "_").replace("/", "_").replace("\\", "_")
    return re.sub(r"[^A-Za-z0-9_.-]", "_", s)

def ensure_dir(p: Path):
    p.mkdir(parents=True, exist_ok=True)

def flat_content(note: openreview.Note) -> Dict[str, Any]:
    c = {}
    if hasattr(note, "content") and isinstance(note.content, dict):
        for k,v in note.content.items():
            if isinstance(v, (str, int, float, bool)) or v is None:
                c[f"content.{k}"] = v
            else:
                try:
                    c[f"content.{k}"] = json.dumps(v, ensure_ascii=False)
                except Exception:
                    c[f"content.{k}"] = str(v)
    return c

def note_to_row(note: openreview.Note) -> Dict[str, Any]:
    base = {
        "id": note.id,
        "forum": getattr(note, "forum", None),
        "replyto": getattr(note, "replyto", None),
        "invitation": getattr(note, "invitation", None),
        "signatures": ",".join(getattr(note, "signatures", []) or []),
        "readers": ",".join(getattr(note, "readers", []) or []),
        "writers": ",".join(getattr(note, "writers", []) or []),
        "tcdate": int(getattr(note, "tcdate", 0) or 0),
        "tmdate": int(getattr(note, "tmdate", 0) or 0),
        "date": int(getattr(note, "cdate", 0) or 0),
    }
    base.update(flat_content(note))
    return base

def iter_submissions(client: openreview.Client, venue: str, inv_suffixes: List[str], limit: int) -> List[openreview.Note]:
    results = []
    seen_ids = set()
    for suf in inv_suffixes:
        invitation = f"{venue}/-/{suf}"
        try:
            for n in or_tools.iterget_notes(client, invitation=invitation, details="replies"):
                if n.id not in seen_ids:
                    results.append(n)
                    seen_ids.add(n.id)
                    if len(results) >= limit:
                        return results
        except openreview.OpenReviewException:
            continue
    return results

def fetch_children(client: openreview.Client, forum_id: str) -> List[openreview.Note]:
    try:
        return client.get_all_notes(forum=forum_id)
    except Exception:
        return []

def save_jsonl(path: Path, rows: List[Dict[str, Any]]):
    with path.open("w", encoding="utf-8") as f:
        for r in rows:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")

def download_pdf(note: openreview.Note, outdir: Path) -> bool:
    pdf_url = f"https://openreview.net/pdf?id={note.id}"
    outpath = outdir / f"{sanitize(note.id)}.pdf"
    try:
        r = requests.get(pdf_url, timeout=30)
        if r.status_code == 200 and r.headers.get("content-type","").lower().startswith("application/pdf"):
            outpath.write_bytes(r.content)
            return True
    except Exception:
        pass
    return False

def main():
    parser = argparse.ArgumentParser(description="Fetch submissions & reviews from OpenReview into JSONL/CSV.")
    parser.add_argument("--venue", help="OpenReview venue/group id, e.g. 'ICLR.cc/2024/Conference'")
    parser.add_argument("--paper-id", help="Fetch a single paper by its forum/id")
    parser.add_argument("--limit", type=int, default=50, help="Max number of submissions to fetch")
    parser.add_argument("--outdir", type=str, default="data/output", help="Directory to write outputs")
    parser.add_argument("--with-pdfs", action="store_true", help="Also download PDFs")
    parser.add_argument("--summary-csv", action="store_true", help="Write CSV summary per paper")
    parser.add_argument("--baseurl", type=str, default="https://api.openreview.net")
    parser.add_argument("--inv-suffix", type=str, default="Blind_Submission,Submission",
                        help="Comma-separated invitation suffixes to try, in order.")
    parser.add_argument("--review-names", type=str, default="Official_Review,Review",
                        help="Comma-separated substrings that identify review invitations.")
    parser.add_argument("--meta-names", type=str, default="Meta_Review,Meta-Review",
                        help="Comma-separated substrings that identify meta-review invitations.")
    parser.add_argument("--decision-names", type=str, default="Decision",
                        help="Comma-separated substrings that identify decision invitations.")
    args = parser.parse_args()

    if not args.venue and not args.paper_id:
        parser.error("Provide either --venue <group> or --paper-id <id>")

    inv_suffixes = [s.strip() for s in (args.inv_suffix or "").split(",") if s.strip()]
    review_frags = [s.strip() for s in (args.review_names or "").split(",") if s.strip()]
    meta_frags = [s.strip() for s in (args.meta_names or "").split(",") if s.strip()]
    decision_frags = [s.strip() for s in (args.decision_names or "").split(",") if s.strip()]

    outdir = Path(args.outdir)
    ensure_dir(outdir)
    pdf_dir = outdir / "pdfs"
    if args.with_pdfs:
        ensure_dir(pdf_dir)

    client = env_login(args.baseurl)

    # 1) Submissions
    if args.paper_id:
        print(f"[1/4] Fetching single paper id={args.paper_id} ...")
        sub = client.get_note(args.paper_id)
        subs = [sub]
        print(" -> Found 1 submission (single-paper mode)")
    else:
        print(f"[1/4] Searching submissions for venue={args.venue} (limit={args.limit}) ...")
        subs = iter_submissions(client, args.venue, inv_suffixes, args.limit)
        print(f" -> Found {len(subs)} submissions")

    # 2) Save submissions
    sub_rows = [note_to_row(n) for n in subs]
    save_jsonl(outdir / "submissions.jsonl", sub_rows)

    # 3) Thread children
    review_rows, meta_rows, decision_rows = [], [], []
    print("[2/4] Fetching reviews/meta/decisions ...")
    for n in tqdm(subs, ncols=80):
        children = fetch_children(client, n.forum or n.id)
        for ch in children:
            inv = (ch.invitation or "")
            row = note_to_row(ch)
            row["paper_forum"] = n.forum or n.id
            if any(frag in inv for frag in review_frags):
                review_rows.append(row)
            elif any(frag in inv for frag in meta_frags):
                meta_rows.append(row)
            elif any(frag in inv for frag in decision_frags):
                decision_rows.append(row)

    save_jsonl(outdir / "reviews.jsonl", review_rows)
    save_jsonl(outdir / "meta_reviews.jsonl", meta_rows)
    save_jsonl(outdir / "decisions.jsonl", decision_rows)

    # 4) PDFs
    if args.with_pdfs:
        print("[3/4] Downloading PDFs ...")
        ok_cnt = 0
        for n in tqdm(subs, ncols=80):
            if download_pdf(n, pdf_dir):
                ok_cnt += 1
        print(f" -> Saved {ok_cnt} PDFs")

    # 5) Summary CSV
    if args.summary_csv:
        print("[4/4] Building summary.csv ...")
        rv_by_forum, mr_by_forum, dec_by_forum = {}, {}, {}
        for r in review_rows:
            rv_by_forum[r["paper_forum"]] = rv_by_forum.get(r["paper_forum"], 0) + 1
        for r in meta_rows:
            mr_by_forum[r["paper_forum"]] = mr_by_forum.get(r["paper_forum"], 0) + 1
        for r in decision_rows:
            dec_by_forum[r["paper_forum"]] = r.get("content.decision") or r.get("content.Decision") or ""

        rows = []
        for s in sub_rows:
            fid = s.get("forum") or s.get("id")
            title = s.get("content.title") or s.get("content.Title") or ""
            authors = s.get("content.authors") or s.get("content.Authors") or ""
            rows.append({
                "forum": fid,
                "id": s.get("id",""),
                "title": title,
                "authors": authors,
                "n_reviews": rv_by_forum.get(fid, 0),
                "n_meta_reviews": mr_by_forum.get(fid, 0),
                "decision": dec_by_forum.get(fid, ""),
            })
        pd.DataFrame(rows).to_csv(outdir / "summary.csv", index=False)
        print(" -> Wrote summary.csv")

    (outdir / "log.json").write_text(json.dumps({
        "venue": args.venue,
        "paper_id": args.paper_id,
        "limit": args.limit,
        "outdir": str(outdir),
        "with_pdfs": bool(args.with_pdfs),
        "ts": int(time.time())
    }, indent=2))

    print("Done. Outputs are in:", outdir)

if __name__ == "__main__":
    main()
