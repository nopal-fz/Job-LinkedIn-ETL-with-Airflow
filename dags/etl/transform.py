import pandas as pd
import re
from pathlib import Path

# config paths
DATA_DIR = Path("/opt/airflow/data")
DATA_DIR.mkdir(exist_ok=True)

input_csv = DATA_DIR / "linkedin_jobs_indonesia.csv"
output_csv = DATA_DIR / "linkedin_jobs_transformed.csv"
skills_path = DATA_DIR / "skills_dict.txt"

def load_skills(path):
    with open(path, "r", encoding="utf-8") as f:
        return [line.strip().lower() for line in f if line.strip()]

skills_vocab = load_skills(skills_path)

def extract_skills(text):
    if not isinstance(text, str):
        return ""
    text = text.lower()
    found = []
    for skill in skills_vocab:
        if re.search(r"\b" + re.escape(skill) + r"\b", text):
            found.append(skill)
    return ",".join(sorted(set(found)))

def parse_location(loc):
    if not isinstance(loc, str) or not loc.strip():
        return None, None

    loc = loc.lower().strip()

    if "remote" in loc:
        return "remote", "remote"

    parts = [p.strip() for p in loc.split(",") if p.strip()]

    if len(parts) == 1:
        return parts[0], "indonesia"

    city = parts[0]
    country = parts[-1]

    return city, country

def run_transform():
    print("[transform] start")

    df = pd.read_csv(input_csv)

    # normalize text
    for col in ["title", "company", "location", "description"]:
        if col in df.columns:
            df[col] = df[col].astype(str).str.lower().str.strip()

    # remove duplicate jobs
    if "url" in df.columns:
        df = df.drop_duplicates(subset=["url"])

    # parse location
    df[["city", "country"]] = df["location"].apply(
        lambda x: pd.Series(parse_location(x))
    )

    # extract skills
    df["skills"] = df["description"].apply(extract_skills)
    df["skill_count"] = df["skills"].apply(
        lambda x: len(x.split(",")) if x else 0
    )

    df.to_csv(output_csv, index=False, encoding="utf-8-sig")

    print(f"[transform] saved → {output_csv}")
    return str(output_csv)