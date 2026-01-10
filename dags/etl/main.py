from .scraper import run_scraper
from .transform import run_transform
from .load import run_load

def main():
    print("[pipeline] start etl pipeline")

    run_scraper()
    run_transform()
    run_load()

    print("[pipeline] etl pipeline finished")