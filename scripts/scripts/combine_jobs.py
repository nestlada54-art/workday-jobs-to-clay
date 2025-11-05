import csv

INPUTS = ["workday_jobs.csv", "greenhouse_jobs.csv"]
OUT    = "combined_jobs.csv"

def read_rows(path):
    try:
        with open(path, newline="", encoding="utf-8") as f:
            return list(csv.DictReader(f))
    except FileNotFoundError:
        return []

def main():
    seen = set()
    out_rows = []
    for path in INPUTS:
        for row in read_rows(path):
            key = (
                (row.get("company","") or "").lower(),
                (row.get("title","") or "").lower(),
                (row.get("job_url","") or "").lower(),
            )
            if key in seen:
                continue
            seen.add(key)
            out_rows.append({
                "company": row.get("company",""),
                "title": row.get("title",""),
                "location": row.get("location",""),
                "posted_date": row.get("posted_date",""),
                "job_url": row.get("job_url",""),
                "source": row.get("source",""),
            })

    with open(OUT, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=["company","title","location","posted_date","job_url","source"])
        w.writeheader()
        w.writerows(out_rows)

    print(f"✅ Wrote {len(out_rows)} rows to {OUT}")

if __name__ == "__main__":
    main()
