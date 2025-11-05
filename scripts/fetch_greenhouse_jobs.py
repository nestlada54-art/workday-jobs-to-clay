import csv, os, re, sys, time
from datetime import datetime, timedelta, timezone
import requests

BOARDS_CSV = "greenhouse_boards.csv"
OUT_CSV    = "greenhouse_jobs.csv"

# Filters (same logic as Workday)
FUNCTION_RE   = re.compile(r"(accounting|finance|financial|it|information\s*technology|marketing|product)", re.I)
SENIORITY_RE  = re.compile(r"(manager|lead|head|director|vp|vice\s*president|sr\.|senior)", re.I)
EXCLUDE_RE    = re.compile(r"(recruiter|talent\s*acquisition|sourcer)", re.I)
US_RE         = re.compile(r"(United States|USA|U\.S\.)", re.I)

LOOKBACK_DAYS = 60  # Greenhouse doesn’t always expose posted dates; keep this broad

def load_boards():
    boards = []
    if not os.path.exists(BOARDS_CSV):
        print(f"⚠️ {BOARDS_CSV} not found; nothing to fetch.", file=sys.stderr)
        return boards
    with open(BOARDS_CSV, newline="", encoding="utf-8") as f:
        r = csv.DictReader(f)
        for row in r:
            token = (row.get("board_token") or "").strip()
            company = (row.get("company") or "").strip()
            if token:
                boards.append((company, token))
    return boards

def fetch_board_jobs(board_token):
    url = f"https://boards-api.greenhouse.io/v1/boards/{board_token}/jobs?content=true"
    r = requests.get(url, timeout=60)
    r.raise_for_status()
    data = r.json()
    return data.get("jobs", [])

def normalize_location(job):
    loc = job.get("location", {}).get("name") or ""
    return loc

def job_posted_recent(job):
    updated = job.get("updated_at")
    if not updated:
        return True
    try:
        dt = datetime.fromisoformat(updated.replace("Z","+00:00"))
        return (datetime.now(timezone.utc) - dt) <= timedelta(days=LOOKBACK_DAYS)
    except Exception:
        return True

def main():
    boards = load_boards()
    rows, seen = [], set()

    for company_label, token in boards:
        print(f"Fetching Greenhouse board: {token}")
        try:
            jobs = fetch_board_jobs(token)
        except Exception as e:
            print(f"⚠️ Error fetching {token}: {e}", file=sys.stderr)
            time.sleep(1)
            continue

        for j in jobs:
            title = (j.get("title") or "").strip()
            url = (j.get("absolute_url") or "").strip()
            company = (j.get("company") or company_label).strip()
            location = normalize_location(j)

            key = (company.lower(), title.lower(), url.lower())
            if key in seen:
                continue

            if not job_posted_recent(j): 
                continue
            if not FUNCTION_RE.search(title): 
                continue
            if not SENIORITY_RE.search(title): 
                continue
            if EXCLUDE_RE.search(title): 
                continue
            if not US_RE.search(location): 
                continue

            seen.add(key)
            rows.append([company, title, location, "", url, "Greenhouse"])

        time.sleep(0.5)

    with open(OUT_CSV, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["company","title","location","posted_date","job_url","source"])
        w.writerows(rows)

    print(f"✅ Wrote {len(rows)} rows to {OUT_CSV}")

if __name__ == "__main__":
    main()
