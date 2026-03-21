import json
from scraper import run
from adapters.palm_beach import run_palm_beach
from adapters.miami import run_miami

COUNTY_RUNNERS = {
    "Orange": run,
    "PalmBeach": run_palm_beach,
    "MiamiDade": run_miami,
}

def main():
    with open("counties.json") as f:
        counties = json.load(f)

    for c in counties:
        if not c["enabled"]:
            continue

        name = c["name"]
        runner = COUNTY_RUNNERS.get(name)

        if not runner:
            print(f"No adapter for {name}")
            continue

        print(f"=== Running {name} ===")
        runner()

if __name__ == "__main__":
    main()
