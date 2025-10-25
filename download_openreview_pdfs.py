#!/usr/bin/env python3
"""
Download PDFs from OpenReview for a given venue.

Examples
--------
# Download the last 50 accepted papers from ICLR 2024
uv run python .\download_openreview_pdfs.py ^
  --venue "ICLR.cc/2024/Conference" ^
  --accepted-only ^
  --max-papers 50 ^
  --out .\data\pdfs
"""
from __future__ import annotations

import argparse
import os
import sys
import time
from typing import Dict, Optional

import requests
from slugify import slugify

try:
    from dotenv import load_dotenv
    load_dotenv()
except Exception:
    pass

try:
    import openreview
except Exception as e:
    print("Please install openreview-py first (see requirements.txt)", file=sys.stderr)
    raise

API_BASE = "https://api.openreview.net"

def _get_env(name: str) -> str:
    v = os.getenv(name)
    if not v:
        raise RuntimeError(f"Missing environment variable {name}. Set it in .env or your shell.")
    return v

def _client() -> openreview.api.OpenReviewClient:
    username = _get_env("OPENREVIEW_USERNAME")
    password = _get_env("OPENREVIEW_PASSWORD")
    return openreview.api.OpenReviewClient(
        baseurl=API_BASE,
        username=username,
        password=password,
    )

def _pick_submission_invitation(client, venue: str) -> str:
    """Try a few common invitations to find submissions."""
    candidates = [
        f"{venue}/-/Submission",
        f"{venue}/-/Blind_Submission",
        f"{venue}/-/Paper",
    ]
    for inv in candidates:
        try:
            notes = client.get_all_notes(invitation=inv, limit=1)
            if notes:
                return inv
        except Exception:
            pass
    raise RuntimeError(f"Cannot find a submission invitation under '{venue}'.")

def _fetch_decisions(client, venue: str) -> Dict[str, str]:
    """Return {forum_id: decision_text}. Missing forums -> ''."""
    decisions: Dict[str, str] = {}
    inv = f"{venue}/-/Decision"
    try:
        for n in client.get_all_notes(invitation=inv):
            decision = ""
            c = n.content or {}
            # various field names across conferences
            for key in ("decision", "Decision", "recommendation", "recommendation:"):
                if key in c:
                    val = c[key]
                    decision = val.get("value") if isinstance(val, dict) else str(val)
                    break
            decisions[n.forum] = decision or ""
    except Exception:
        # decision invitation may not exist
        pass
    return decisions

def _is_accepted(decision_text: str) -> bool:
    txt = (decision_text or "").lower()
    return txt.startswith("accept") or "accept" in txt

def _safe_stem(title: str, note_id: str) -> str:
    slug = slugify(title or "")[:120]
    if not slug:
        slug = note_id
    return slug

def _get_pdf_url(note) -> Optional[str]:
    # Typical: note.content["pdf"] == {"value": "/pdf/abcdef....pdf"}
    c = note.content or {}
    pdf = c.get("pdf")
    if isinstance(pdf, dict) and "value" in pdf:
        return API_BASE + pdf["value"]
    if isinstance(pdf, str):
        if pdf.startswith("/"):
            return API_BASE + pdf
        return pdf
    # Some venues use "files" or "file"
    for k in ("file", "files"):
        if k in c:
            val = c[k]
            if isinstance(val, dict) and "value" in val:
                v = val["value"]
                return API_BASE + v if v.startswith("/") else v
    return None

def download_file(session: requests.Session, url: str, out_path: str) -> bool:
    try:
        with session.get(url, stream=True, timeout=60) as r:
            r.raise_for_status()
            with open(out_path, "wb") as f:
                for chunk in r.iter_content(chunk_size=1024 * 64):
                    if chunk:
                        f.write(chunk)
        return True
    except Exception as e:
        print(f"WARNING: Download failed {url}: {e}")
        return False

def main():
    p = argparse.ArgumentParser(description="Download PDFs from OpenReview for a venue")
    p.add_argument("--venue", required=True, help='e.g., "ICLR.cc/2024/Conference"')
    p.add_argument("--accepted-only", action="store_true", help="Only download accepted papers")
    p.add_argument("--max-papers", type=int, default=50, help="Max papers to download")
    p.add_argument("--out", default=os.path.join("data", "pdfs"), help="Output folder for PDFs")
    args = p.parse_args()

    os.makedirs(args.out, exist_ok=True)
    client = _client()
    print(f"Login OK as: {client.profile.id if client.profile else 'anonymous'}")

    # pick submission invitation dynamically
    sub_inv = _pick_submission_invitation(client, args.venue)
    print(f"Using submission invitation: {sub_inv}")

    submissions = client.get_all_notes(invitation=sub_inv)
    print(f"Found {len(submissions)} submissions under {args.venue}")

    decisions_map = _fetch_decisions(client, args.venue) if args.accepted_only else {}
    count = 0

    # requests session with auth header
    session = requests.Session()
    if client.token:
        session.headers.update({"Authorization": f"Bearer {client.token}"})

    for n in submissions:
        if args.accepted_only:
            dec = decisions_map.get(n.forum, "")
            if not _is_accepted(dec):
                continue

        title = ""
        c = n.content or {}
        if "title" in c:
            val = c["title"]
            title = val.get("value") if isinstance(val, dict) else str(val)
        stem = _safe_stem(title, n.id)

        pdf_url = _get_pdf_url(n)
        if not pdf_url:
            print(f"SKIP (no PDF field): {stem}")
            continue

        out_path = os.path.join(args.out, f"{stem}.pdf")
        ok = download_file(session, pdf_url, out_path)
        if ok:
            count += 1
            print(f"Saved {count} -> {out_path}")
        if count >= args.max_papers:
            break

    print(f"Done. Saved {count} PDFs -> {args.out}")

if __name__ == "__main__":
    main()
