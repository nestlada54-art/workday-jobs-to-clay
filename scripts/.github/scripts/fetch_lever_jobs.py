import csv, os, re, sys, time
from datetime import datetime, timedelta, timezone
import requests

LEVER_CSV = "lever_companies.csv"
OUT_CSV   = "lever_jobs.csv"

# Filters (same as other sources)
FUNCTION_RE   = re.compile(r"(accounting|finance|financial|it|information\s*technology|marketing|product)", re.I)
SENIORITY_RE  = re.compile(r"(manager|lead|head|director|vp|vice\s*president|sr\.|senior)", re.I)
EXCLUDE_RE    = re.compile(r"(recruiter|talent\s*acquisition|sourcer)", re.I)
US_RE         = re.compile(r"(United States|USA|U\.S\.)", re.I)

LOOKBACK_DAYS = 60  # use 60 for better coverage

def load_companies():
    rows = []
    if not os.path.exists(LEVER_CSV):
        print(f"⚠️ {LEVER_CSV} not found; nothing to fetch.", file=sys.stderr)
        return rows
    with open(LEVER_CSV, newline="", encoding="utf-8") as f:
        r = csv.DictReader(f)
        for row in r:
            slug = (row.get("lever_slug") or "").strip()
            label = (row.get("company") or slug).strip()
            if slug:
                rows.append((label, slug))
    return rows

def fetch_postings(slug):
    # Public Lever postings API
    url = f"https://api.lever.co/v0/postings/{slug}?mode=json"
    r = requests.get(url, timeout=60)
    r.raise_for_status()
    return r.json() if r.headers.get("content-type","").lower().startswith("application/json") else []

def created_within(post, days=LOOKBACK_DAYS):
    # Lever returns createdAt/updatedAt in ms since epoch
    ts = post.get("createdAt") or post.get("updatedAt")
    if not ts: 
        return True
    try:
        dt = datetime.fromtimestamp(int(ts)/1000, tz=timezone.utc)
        return (datetime.now(timezone.utc) - dt) <= timedelta(days=days)
    except Exception:
        return True

def main():
    companies = load_companies()
    rows, seen = [], set()

    for label, slug in companies:
        print(f"Fetching Lever postings: {slug}")
        try:
            posts = fetch_postings(slug)
        except Exception as e:
            print(f"⚠️ Error fetching {slug}: {e}", file=sys.stderr)
            time.sleep(1)
            continue

        for p in posts:
            title = (p.get("text") or "").strip()
            url = (p.get("hostedUrl") or "").strip()
            location = (p.get("categories", {}) or {}).get("location") or ""
            posted_date = ""
            if p.get("createdAt"):
                try:
                    posted_date = datetime.fromtimestamp(int(p["createdAt"])/1000, tz=timezone.utc).isoformat()
                except Exception:
                    posted_date = ""

            key = (label.lower(), title.lower(), url.lower())
            if key in seen:
                continue

            if not created_within(p): continue
            if not FUNCTION_RE.search(title): continue
            if not SENIORITY_RE.search(title): continue
            if EXCLUDE_RE.search(title): continue
            if not US_RE.search(location): continue

            seen.add(key)
            rows.append([label, title, location, posted_date, url, "Lever"])

        time.sleep(0.5)

    with open(OUT_CSV, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["company","title","location","posted_date","job_url","source"])
        w.writerows(rows)

    print(f"✅ Wrote {len(rows)} rows to {OUT_CSV}")

if __name__ == "__main__":
    main()
