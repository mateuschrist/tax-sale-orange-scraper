import os
import logging

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("scraper")

SCRAPERS = {
    "orange": "adapters.orange",
    "miami": "adapters.miami",
}

def run():
    county = os.getenv("COUNTY", "").lower()

    if county:
        run_single(county)
    else:
        run_all()


def run_single(county: str):
    if county not in SCRAPERS:
        raise ValueError(f"Unknown county: {county}")

    log.info(f"Running scraper for {county.upper()}")

    module = __import__(SCRAPERS[county], fromlist=["run"])
    module.run()


def run_all():
    for county in SCRAPERS:
        try:
            run_single(county)
        except Exception as e:
            log.error(f"Failed {county}: {e}")