import requests
import pandas as pd
import time
import random
from bs4 import BeautifulSoup
from pathlib import Path

def run_scraper(**context):
    print("[scraper] start")

    # airflow root
    DATA_DIR = Path("/opt/airflow/data")
    DATA_DIR.mkdir(exist_ok=True)

    output_csv = DATA_DIR / "linkedin_jobs_indonesia.csv"

    # params from DAG
    max_pages = context["params"].get("max_pages", 2)

    search_query = "data scientist"
    location = "indonesia"

    base_url = "https://www.linkedin.com/jobs-guest/jobs/api/seeMoreJobPostings/search"
    headers = {
        "user-agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/120.0.0.0 Safari/537.36"
        )
    }

    def fetch_page(start):
        params = {
            "keywords": search_query,
            "location": location,
            "start": start
        }
        res = requests.get(base_url, params=params, headers=headers, timeout=10)
        res.raise_for_status()
        return res.text

    def parse_job_list(html):
        soup = BeautifulSoup(html, "html.parser")
        cards = soup.find_all("li")
        jobs = []

        for card in cards:
            title = card.find("h3", class_="base-search-card__title")
            company = card.find("h4", class_="base-search-card__subtitle")
            job_location = card.find("span", class_="job-search-card__location")
            link_tag = card.find("a", class_="base-card__full-link")

            jobs.append({
                "title": title.get_text(strip=True).lower() if title else None,
                "company": company.get_text(strip=True).lower() if company else None,
                "location": job_location.get_text(strip=True).lower() if job_location else None,
                "url": link_tag["href"] if link_tag else None
            })

        return jobs

    def fetch_job_description(url):
        if not url:
            return ""
        res = requests.get(url, headers=headers, timeout=10)
        soup = BeautifulSoup(res.text, "html.parser")
        desc = soup.find("div", class_="show-more-less-html__markup")
        return desc.get_text(" ", strip=True).lower() if desc else ""

    all_rows = []

    for page in range(max_pages):
        print(f"[scraper] page {page+1}/{max_pages}")
        html = fetch_page(page * 25)
        jobs = parse_job_list(html)

        for job in jobs:
            job["description"] = fetch_job_description(job["url"])
            time.sleep(random.uniform(1.0, 2.0))

        all_rows.extend(jobs)
        time.sleep(random.uniform(1.0, 2.5))

    df = pd.DataFrame(all_rows)
    df.drop_duplicates(subset=["url"], inplace=True)
    df.to_csv(output_csv, index=False, encoding="utf-8-sig")

    row_count = len(df)
    print(f"[scraper] saved {row_count} rows")

    return row_count