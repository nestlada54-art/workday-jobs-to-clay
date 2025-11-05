import csv
import os
import requests
from datetime import datetime

# Load your RapidAPI key from the GitHub secret
RAPIDAPI_KEY = os.getenv("RAPIDAPI_KEY")

# Workday Jobs API endpoint
WORKDAY_API_URL = "https://workday-jobs-api.p.rapidapi.com/active-ats-24h"

HEADERS = {
    "x-rapidapi-host": "workday-jobs-api.p.rapidapi.com",
    "x-rapidapi-key": RAPIDAPI_KEY
}

# Target keywords for filtering roles (Accounting, Finance, IT, Marketing, Product Management - Manager+)
TARGET_KEYWORDS = [
    "accounting", "finance", "financial", "it", "information technology",
    "marketing", "product management", "product manager", "product director",
    "manager", "director", "lead", "head", "vp", "chief"
]

EXCLUDE_KEYWORDS = [
    "recruiter", "talent acquisition", "sourcer", "hr intern", "human resources intern"
]

def load_companies(filename="companies.csv"):
    companies = []
    with open(filename, newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            companies.append({
                "company": row.get("company", "").strip(),
                "domain": row.get("domain", "").strip()
            })
    return companies


def fetch_jobs_for_company(company_name):
    """Fetch jobs from Workday API for a specific company."""
    params = {
        "title_filter": "",  # We'll filter later
        "location_filter": "United States"
    }

    try:
        response = requests.get(WORKDAY_API_URL, headers=HEADERS, params=params)
        response.raise_for_status()
        data = response.json()
        jobs = data.get("jobs", [])
        company_jobs = []

        for job in jobs:
            title = job.get("title", "").lower()
            location = job.get("location", "")
            url = job.get("url", "")
            company = job.get("company", "")

            # Only include US jobs with matching company name or domain in URL
            if (
                "united states" in location.lower()
                and (company_name.lower() in company.lower() or company_name.lower().replace(" ", "") in url.lower())
            ):
                company_jobs.append(job)

        return company_jobs

    except Exception as e:
        print(f"Error fetching for {company_name}: {e}")
        return []


def filter_jobs(jobs):
    """Filter jobs by target titles and exclude unwanted ones."""
    filtered = []
    for job in jobs:
        title = job.get("title", "").lower()
        url = job.get("url", "")
        company = job.get("company", "")
        location = job.get("location", "")

        if any(word in title for word in TARGET_KEYWORDS) and not any(bad in title for bad in EXCLUDE_KEYWORDS):
            filtered.append({
                "company": company,
                "title": job.get("title", ""),
                "location": location,
                "url": url
            })
    return filtered


def main():
    companies = load_companies()
    all_jobs = []

    print(f"Fetching jobs for {len(companies)} companies...")

    for company in companies:
        name = company["company"]
        print(f"→ Fetching jobs for {name}...")
        jobs = fetch_jobs_for_company(name)
        filtered = filter_jobs(jobs)
        all_jobs.extend(filtered)

    # Save to CSV
    output_file = "workday_jobs.csv"
    with open(output_file, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["company", "title", "location", "url"])
        writer.writeheader()
        writer.writerows(all_jobs)

    print(f"✅ Done. Saved {len(all_jobs)} filtered jobs to {output_file}")


if __name__ == "__main__":
    main()
