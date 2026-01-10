import pandas as pd
import psycopg2
from pathlib import Path

# db config
host = "postgres_etl"
database = "airflow_db_linkedin"
user = "postgres"
password = "postgresql123"
port = 5432

schema = "linkedin"
table = "jobs"

DATA_DIR = Path("/opt/airflow/data")
DATA_DIR.mkdir(exist_ok=True)
input_csv = DATA_DIR / "linkedin_jobs_transformed.csv"

def connect_to_postgresql():
    try:
        return psycopg2.connect(
            host=host,
            database=database,
            user=user,
            password=password,
            port=port
        )
    except Exception as e:
        raise RuntimeError(f"PostgreSQL connection failed: {e}")

def create_schema_and_table(conn):
    sql = f"""
    CREATE SCHEMA IF NOT EXISTS {schema};

    CREATE TABLE IF NOT EXISTS {schema}.{table} (
        id SERIAL PRIMARY KEY,
        title TEXT,
        company TEXT,
        location TEXT,
        city TEXT,
        country TEXT,
        description TEXT,
        skills TEXT,
        skill_count INT,
        url TEXT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """
    cur = conn.cursor()
    cur.execute(sql)
    conn.commit()
    cur.close()

def truncate_table(conn):
    cur = conn.cursor()
    cur.execute(f"TRUNCATE TABLE {schema}.{table} RESTART IDENTITY;")
    conn.commit()
    cur.close()

def load_data(conn, df):
    insert_sql = f"""
    INSERT INTO {schema}.{table}
    (
        title, company, location, city, country,
        description, skills, skill_count, url
    )
    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
    """
    cur = conn.cursor()
    cur.executemany(insert_sql, df[[
        "title", "company", "location", "city", "country",
        "description", "skills", "skill_count", "url"
    ]].values.tolist())
    conn.commit()
    cur.close()

def run_load():
    print("[load] start")

    if not input_csv.exists():
        raise FileNotFoundError(f"file not found: {input_csv}")

    df = pd.read_csv(input_csv)

    conn = connect_to_postgresql()
    create_schema_and_table(conn)
    truncate_table(conn)
    load_data(conn, df)

    conn.close()
    print("[load] success")