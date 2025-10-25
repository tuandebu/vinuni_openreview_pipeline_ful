#!/usr/bin/env python3
"""
Call a running GROBID server to parse PDFs, save TEI XML, then write simple Markdown.

Start GROBID (Java) locally:
  cd grobid
  ./gradlew run            # default port 8070
Or with Docker Desktop (fastest on Windows):
  docker run -t --rm -p 8070:8070 lfoppiano/grobid:0.8.0

Example:
uv run python .\grobid_parse_md.py ^
  --grobid-url http://localhost:8070 ^
  --in-dir .\data\pdfs ^
  --tei-dir .\data\tei ^
  --out-md .\out\md_grobid ^
  --threads 2
"""
from __future__ import annotations

import argparse
import os
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
from lxml import etree
from tqdm import tqdm

NS = {"tei": "http://www.tei-c.org/ns/1.0"}

def post_grobid(grobid_url: str, pdf_path: str, timeout: int = 180) -> bytes:
    """
    POST the PDF to /api/processFulltextDocument and return raw TEI bytes.
    """
    endpoint = grobid_url.rstrip("/") + "/api/processFulltextDocument"
    files = {"input": open(pdf_path, "rb")}
    data = {"consolidateCitations": "0"}
    try:
        with requests.post(endpoint, files=files, data=data, timeout=timeout) as r:
            r.raise_for_status()
            return r.content
    finally:
        files["input"].close()

def tei_to_markdown(tei_bytes: bytes) -> str:
    """
    A very simple TEI -> Markdown conversion:
    - extract title, headings, and paragraphs
    - join them as Markdown
    """
    md_lines = []
    parser = etree.XMLParser(recover=True)
    root = etree.fromstring(tei_bytes, parser=parser)

    # Title
    title = root.xpath("//tei:titleStmt/tei:title/text()", namespaces=NS)
    if title:
        md_lines.append(f"# {title[0].strip()}")
        md_lines.append("")

    # Sections: head + p
    for div in root.xpath("//tei:div", namespaces=NS):
        heads = div.xpath("./tei:head/text()", namespaces=NS)
        if heads:
            md_lines.append(f"## {heads[0].strip()}")
        # paragraphs
        for p in div.xpath(".//tei:p", namespaces=NS):
            txt = " ".join(p.itertext()).strip()
            if txt:
                md_lines.append(txt)
        md_lines.append("")

    md = "\n".join(md_lines).strip() + "\n"
    return md

def process_one(grobid_url: str, pdf_path: str, tei_dir: str, md_dir: str) -> str:
    stem = os.path.splitext(os.path.basename(pdf_path))[0]
    tei_path = os.path.join(tei_dir, f"{stem}.grobid.tei.xml")
    md_path = os.path.join(md_dir, f"{stem}.md")

    # Skip if already there
    if os.path.exists(md_path) and os.path.exists(tei_path):
        return md_path

    try:
        tei_bytes = post_grobid(grobid_url, pdf_path)
        os.makedirs(tei_dir, exist_ok=True)
        with open(tei_path, "wb") as f:
            f.write(tei_bytes)

        md = tei_to_markdown(tei_bytes)
        os.makedirs(md_dir, exist_ok=True)
        with open(md_path, "w", encoding="utf-8") as f:
            f.write(md)
        return md_path
    except Exception as e:
        raise RuntimeError(f"GROBID failed for {pdf_path}: {e}") from e

def main():
    p = argparse.ArgumentParser()
    p.add_argument("--grobid-url", default="http://localhost:8070", help="GROBID REST base URL")
    p.add_argument("--in-dir", default=os.path.join("data", "pdfs"))
    p.add_argument("--tei-dir", default=os.path.join("data", "tei"))
    p.add_argument("--out-md", default=os.path.join("out", "md_grobid"))
    p.add_argument("--threads", type=int, default=2)
    args = p.parse_args()

    pdfs = []
    for n in os.listdir(args.in_dir):
        if n.lower().endswith(".pdf"):
            pdfs.append(os.path.join(args.in_dir, n))

    if not pdfs:
        print(f"No PDFs found under {args.in_dir}")
        return

    print(f"GROBID server: {args.grobid_url}")
    print(f"Found {len(pdfs)} PDF(s) to process")

    failures = 0
    with ThreadPoolExecutor(max_workers=args.threads) as ex:
        futs = [ex.submit(process_one, args.grobid_url, p, args.tei_dir, args.out_md) for p in pdfs]
        for fut in tqdm(as_completed(futs), total=len(futs), desc="TEI ⇒ Markdown"):
            try:
                fut.result()
            except Exception as e:
                failures += 1
                print(f"ERROR: {e}")

    print(f"Done. Failures: {failures}")

if __name__ == "__main__":
    main()
