import json
from adapters.orange import run_orange

COUNTY_RUNNERS = {
    "Orange": run_orange,
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