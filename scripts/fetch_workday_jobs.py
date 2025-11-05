import os, csv, re, sys, time
from datetime import datetime, timedelta, timezone
import requests

# --- CONFIG ---
RAPIDAPI_KEY = os.getenv("RAPIDAPI_KEY")
# Use the broader endpoint (not 24h-limited)
API_URL = "https://workday-jobs-api.p.rapidapi.com/jobs"

HEADERS = {
    "x-rapidapi-host": "workday-jobs-api.p.rapidapi.com",
    "x-rapidapi-key": RAPIDAPI_KEY or ""
}

# Ask for a 14-day lookback (the API will ignore unknown params gracefully)
LOOKBACK_DAYS = 14
LOCATION_FILTER = '"United States"'

# Seniority probes to increase recall from the API
TITLE_FILTERS = [
    '"manager"', '"senior manager"', '"director"', '"head"', '"lead"', '"vp"', '"vice president"'
]

# Functional focus
FUNCTION_RE   = re.compile(r"(accounting|finance|financial|it|information\s*technology|marketing|product)", re.I)
SENIORITY_RE  = re.compile(r"(manager|lead|head|director|vp|vice\s*president|sr\.|senior)", re.I)
EXCLUDE_RE    = re.compile(r"(recruiter|talent\s*acquisition|sourcer)", re.I)
US_RE         = re.compile(r"(United States|USA)", re.I)

OUT_CSV = "workday_jobs.csv"
COMPANY_CSV = "companies.csv"

def read_company_whitelist():
    names, domains = set(), set()
    if not os.path.exists(COMPANY_CSV):
        return names, domains
    with open(COMPANY_CSV, newline="", encoding="utf-8") as f:
        r = csv.DictReader(f)
        for row in r:
            c = (row.get("company") or "").strip().lower()
            d = (row.get("domain") or "").strip().lower()
            if c: names.add(c)
            if d: domains.add(d)
    return names, domains

def company_matches(company, url, names, domains):
    c = (company or "").strip().lower()
    u = (url or "").strip().lower()
    if not names and not domains:
        return True
    return (c in names) or any(d and d in u for d in domains)

def fetch_batch(title_filter):
    params = {
        "title_filter": title_filter,
        "location_filter": LOCATION_FILTER,
        # common patterns some RapidAPI “jobs” endpoints accept:
        "days": str(LOOKBACK_DAYS),          # preferred if supported
        "posted_within_days": str(LOOKBACK_DAYS)  # fallback if provider uses this name
    }
    resp = requests.get(API_URL, headers=HEADERS, params=params, timeout=60)
    resp.raise_for_status()
    data = resp.json()
    # Guard for provider naming
    return data.get("results") or data.get("jobs") or []

def main():
    if not RAPIDAPI_KEY:
        print("❌ Missing RAPIDAPI_KEY (Repo → Settings → Secrets → Actions).", file=sys.stderr)
        sys.exit(1)

    names, domains = read_company_whitelist()
    out_rows, seen = [], set()

    for t in TITLE_FILTERS:
        try:
            print(f"Fetching with title_filter={t} over last {LOOKBACK_DAYS} days…")
            results = fetch_batch(t)
        except requests.HTTPError as e:
            print(f"HTTP error: {e} body={(e.response and e.response.text) or ''}", file=sys.stderr)
            time.sleep(2)
            continue
        except Exception as e:
            print(f"Fetch error: {e}", file=sys.stderr)
            time.sleep(2)
            continue

        for j in results:
            company  = (j.get("company") or "").strip()
            title    = (j.get("title") or "").strip()
            location = (j.get("location") or "").strip()
            url      = (j.get("url") or j.get("job_url") or j.get("source_url") or "").strip()
            posted   = (j.get("posted_date") or j.get("date") or "").strip()

            key = (company.lower(), title.lower(), url.lower())
            if key in seen:
                continue

            if not company_matches(company, url, names, domains):
                continue
            if not FUNCTION_RE.search(title): continue
            if not SENIORITY_RE.search(title): continue
            if EXCLUDE_RE.search(title): continue
            if not US_RE.search(location): continue

            seen.add(key)
            out_rows.append([company, title, location, posted, url, "Workday"])

        time.sleep(1)

    with open(OUT_CSV, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["company","title","location","posted_date","job_url","source"])
        w.writerows(out_rows)

    print(f"✅ Wrote {len(out_rows)} rows to {OUT_CSV}")

if __name__ == "__main__":
    main()
