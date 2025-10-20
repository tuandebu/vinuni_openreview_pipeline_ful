import argparse, json, os
from pathlib import Path
import pandas as pd

def read_jsonl(p):
    if not Path(p).exists(): return pd.DataFrame()
    return pd.read_json(p, lines=True, dtype_backend="pyarrow")

def main(indir):
    out = Path("analysis"); out.mkdir(exist_ok=True)
    subs  = read_jsonl(Path(indir)/"submissions.jsonl")
    revs  = read_jsonl(Path(indir)/"reviews.jsonl")
    metas = read_jsonl(Path(indir)/"meta_reviews.jsonl")
    decs  = read_jsonl(Path(indir)/"decisions.jsonl")

    # Tổng quan
    overview = {
        "n_submissions": len(subs),
        "n_reviews": len(revs),
        "n_meta_reviews": len(metas),
        "n_decisions": len(decs),
    }

    # Reviews per paper
    if not revs.empty:
        grp = revs.groupby("paper_forum").size().reset_index(name="n_reviews")
    else:
        grp = pd.DataFrame(columns=["paper_forum","n_reviews"])

    # Ghép tiêu đề, tác giả (nếu có)
    title = subs[["forum","content.title"]].rename(columns={"forum":"paper_forum","content.title":"title"}) if "content.title" in subs.columns else pd.DataFrame()
    if not title.empty:
        grp = grp.merge(title, on="paper_forum", how="left")

    # Decision theo paper (nếu có)
    dec_map = {}
    if not decs.empty:
        for _,r in decs.iterrows():
            fid = r.get("paper_forum") or r.get("forum") or r.get("id")
            dec = r.get("content.decision") or r.get("content.Decision") or ""
            dec_map[fid]=dec
        grp["decision"]=grp["paper_forum"].map(dec_map)

    grp.sort_values("n_reviews", ascending=False, inplace=True)
    grp.to_csv(out/"reviews_by_paper.csv", index=False)

    # Phân phối số review/bài
    dist = grp["n_reviews"].value_counts(dropna=False).sort_index()
    dist.to_csv(out/"reviews_per_paper_distribution.csv")

    # Lưu overview + sample vài dòng
    with open(out/"summary.md","w",encoding="utf-8") as f:
        f.write("# OpenReview crawl — summary\n\n")
        for k,v in overview.items():
            f.write(f"- **{k}**: {v}\n")
        f.write("\n## Top papers by #reviews\n")
        if not grp.empty:
            f.write(grp[["paper_forum","n_reviews","title","decision"]].head(10).to_markdown(index=False))
        else:
            f.write("_No reviews found._\n")

if __name__=="__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--indir", required=True)
    args = ap.parse_args()
    main(args.indir)
