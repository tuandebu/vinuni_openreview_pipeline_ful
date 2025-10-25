
#!/usr/bin/env python3
"""
Run both steps:
  1) download_openreview_pdfs.py  -> saves PDFs to --out-pdfs
  2) grobid_parse_md.py           -> parses PDFs to TEI + Markdown via GROBID

Example (Windows PowerShell):
  uv run python .\run_download_and_parse.py `
    --venue "ICLR.cc/2024/Conference" `
    --accepted-only `
    --max-papers 50 `
    --out-pdfs .\data\pdfs `
    --grobid-url http://localhost:8070 `
    --tei-dir .\data\tei `
    --out-md .\out\md_grobid `
    --threads 2
"""
from __future__ import annotations

import argparse
import os
import sys
import subprocess
from pathlib import Path

def ensure_dir(p: str) -> str:
    Path(p).mkdir(parents=True, exist_ok=True)
    return p

def main():
    ap = argparse.ArgumentParser(description="Download PDFs from OpenReview and parse to Markdown via GROBID")
    ap.add_argument("--venue", required=True, help='e.g., "ICLR.cc/2024/Conference"')
    ap.add_argument("--accepted-only", action="store_true", help="Only accepted papers")
    ap.add_argument("--max-papers", type=int, default=50)
    ap.add_argument("--out-pdfs", default=os.path.join("data", "pdfs"))
    ap.add_argument("--grobid-url", default="http://localhost:8070")
    ap.add_argument("--tei-dir", default=os.path.join("data", "tei"))
    ap.add_argument("--out-md", default=os.path.join("out", "md_grobid"))
    ap.add_argument("--threads", type=int, default=2)
    ap.add_argument("--python-exe", default=sys.executable, help="Python executable to use")
    args = ap.parse_args()

    # Resolve scripts in the same directory as this runner
    here = Path(__file__).resolve().parent
    dl_script = here / "download_openreview_pdfs.py"
    parse_script = here / "grobid_parse_md.py"

    if not dl_script.exists():
        raise SystemExit(f"ERROR: {dl_script.name} not found next to this script.")
    if not parse_script.exists():
        raise SystemExit(f"ERROR: {parse_script.name} not found next to this script.")

    ensure_dir(args.out_pdfs)
    ensure_dir(args.tei_dir)
    ensure_dir(args.out_md)

    # Step 1: Download PDFs
    cmd1 = [
        args.python_exe, str(dl_script),
        "--venue", args.venue,
        "--max-papers", str(args.max_papers),
        "--out", args.out_pdfs,
    ]
    if args.accepted_only:
        cmd1.append("--accepted-only")

    print("=".ljust(80, "="))
    print("STEP 1/2: Download PDFs")
    print("Command:", " ".join(cmd1))
    print("=".ljust(80, "="))
    r1 = subprocess.run(cmd1, check=False)
    if r1.returncode != 0:
        raise SystemExit(f"download_openreview_pdfs.py failed with exit code {r1.returncode}")

    # Step 2: Parse to Markdown via GROBID
    cmd2 = [
        args.python_exe, str(parse_script),
        "--grobid-url", args.grobid_url,
        "--in-dir", args.out_pdfs,
        "--tei-dir", args.tei_dir,
        "--out-md", args.out_md,
        "--threads", str(args.threads),
    ]
    print("=".ljust(80, "="))
    print("STEP 2/2: Parse PDFs to TEI + Markdown via GROBID")
    print("Command:", " ".join(cmd2))
    print("=".ljust(80, "="))
    r2 = subprocess.run(cmd2, check=False)
    if r2.returncode != 0:
        raise SystemExit(f"grobid_parse_md.py failed with exit code {r2.returncode}")

    print("All done.")
    print(f"PDFs     -> {args.out_pdfs}")
    print(f"TEI XML  -> {args.tei_dir}")
    print(f"Markdown -> {args.out_md}")

if __name__ == "__main__":
    main()
