import json, pathlib as P
OUT = P.Path(r"data\demo50")
ids, seen = [], set()
for fn in ("submissions.jsonl","reviews.jsonl"):
    p = OUT / fn
    if not p.exists(): continue
    with open(p,"r",encoding="utf-8",errors="ignore") as f:
        for line in f:
            try: o = json.loads(line)
            except: continue
            fid = o.get("forum") or o.get("paper_forum")
            if fid and fid not in seen:
                seen.add(fid); ids.append(fid)
            if len(ids) >= 50: break
open("ids_link50.txt","w",encoding="utf-8").write("\n".join(ids))
open("links_link50.txt","w",encoding="utf-8").write("\n".join(f"https://openreview.net/forum?id={i}" for i in ids))
print("Wrote",len(ids),"ids -> ids_link50.txt")
