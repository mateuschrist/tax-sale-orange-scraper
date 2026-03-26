from __future__ import annotations

import importlib
import logging
import os
import sys
import time
from dataclasses import dataclass
from typing import Dict, List, Optional


# =========================================================
# LOGGING
# =========================================================
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()

logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    stream=sys.stdout,
)

log = logging.getLogger("scraper")


# =========================================================
# CONFIG
# =========================================================
@dataclass(frozen=True)
class ScraperConfig:
    key: str
    module_path: str
    enabled: bool = True


SCRAPERS: Dict[str, ScraperConfig] = {
    "orange": ScraperConfig(
        key="orange",
        module_path="adapters.orange",
        enabled=True,
    ),
    "miami": ScraperConfig(
        key="miami",
        module_path="adapters.miami",
        enabled=True,
    ),
    # futuros counties:
    # "broward": ScraperConfig("broward", "adapters.broward", True),
    # "palm_beach": ScraperConfig("palm_beach", "adapters.palm_beach", True),
}


# =========================================================
# HELPERS
# =========================================================
def clean_text(value: Optional[str]) -> str:
    return (value or "").strip()


def env_bool(name: str, default: bool = False) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in ("1", "true", "yes", "y", "on")


def parse_county_list(raw: str) -> List[str]:
    """
    Aceita formatos como:
      orange
      miami
      orange,miami
      orange miami
      orange;miami
      all
      *
    """
    raw = clean_text(raw).lower()

    if not raw:
        return []

    if raw in ("all", "*"):
        return list_enabled_scrapers()

    normalized = raw.replace(";", ",").replace("|", ",")
    parts = []

    for piece in normalized.split(","):
        piece = piece.strip()
        if not piece:
            continue

        # suporta "orange miami"
        subparts = [x.strip() for x in piece.split() if x.strip()]
        parts.extend(subparts)

    # remove duplicados preservando ordem
    seen = set()
    result = []
    for item in parts:
        if item not in seen:
            seen.add(item)
            result.append(item)

    return result


def list_enabled_scrapers() -> List[str]:
    return [key for key, cfg in SCRAPERS.items() if cfg.enabled]


def validate_counties(counties: List[str]) -> List[str]:
    invalid = [c for c in counties if c not in SCRAPERS]
    if invalid:
        valid = ", ".join(sorted(SCRAPERS.keys()))
        raise ValueError(
            f"Unknown county(s): {', '.join(invalid)}. Valid options: {valid}"
        )
    return counties


def resolve_target_counties() -> List[str]:
    """
    Prioridade:
      1. COUNTIES
      2. COUNTY
      3. todos habilitados
    """
    counties_raw = clean_text(os.getenv("COUNTIES"))
    county_raw = clean_text(os.getenv("COUNTY"))

    if counties_raw:
        counties = parse_county_list(counties_raw)
    elif county_raw:
        counties = parse_county_list(county_raw)
    else:
        counties = list_enabled_scrapers()

    counties = validate_counties(counties)

    # filtra disabled
    counties = [c for c in counties if SCRAPERS[c].enabled]

    if not counties:
        raise RuntimeError("No enabled counties selected to run.")

    return counties


def import_scraper_module(module_path: str):
    return importlib.import_module(module_path)


def run_scraper_module(county: str) -> dict:
    """
    Espera que o módulo tenha:
      - run()
    Opcionalmente pode retornar dict com resultado.
    """
    cfg = SCRAPERS[county]
    started_at = time.time()

    log.info("Starting county=%s module=%s", county, cfg.module_path)

    module = import_scraper_module(cfg.module_path)

    if not hasattr(module, "run"):
        raise AttributeError(f"Module '{cfg.module_path}' does not define run()")

    result = module.run()

    elapsed = round(time.time() - started_at, 2)

    if result is None:
        result = {}

    if not isinstance(result, dict):
        result = {"raw_result": result}

    final = {
        "county": county,
        "module": cfg.module_path,
        "success": True,
        "elapsed_seconds": elapsed,
        **result,
    }

    log.info(
        "Finished county=%s success=%s elapsed=%.2fs",
        county,
        True,
        elapsed,
    )
    return final


# =========================================================
# MAIN RUNNER
# =========================================================
def run() -> dict:
    """
    Orquestrador principal.

    ENV suportadas:
      COUNTY=orange
      COUNTIES=orange,miami
      CONTINUE_ON_ERROR=true
      FAIL_FAST=true
    """
    target_counties = resolve_target_counties()

    continue_on_error = env_bool("CONTINUE_ON_ERROR", True)
    fail_fast = env_bool("FAIL_FAST", False)

    # fail_fast tem prioridade prática
    if fail_fast:
        continue_on_error = False

    log.info("Target counties: %s", ", ".join(target_counties))
    log.info(
        "Execution mode: continue_on_error=%s fail_fast=%s",
        continue_on_error,
        fail_fast,
    )

    results = []
    failures = []
    started_at = time.time()

    for county in target_counties:
        try:
            result = run_scraper_module(county)
            results.append(result)

        except Exception as e:
            log.exception("County failed: %s", county)

            failure = {
                "county": county,
                "success": False,
                "error": str(e),
            }
            failures.append(failure)

            if not continue_on_error:
                total_elapsed = round(time.time() - started_at, 2)
                final = {
                    "success": False,
                    "mode": "fail_fast",
                    "counties_requested": target_counties,
                    "counties_completed": [r["county"] for r in results],
                    "results": results,
                    "failures": failures,
                    "elapsed_seconds": total_elapsed,
                }
                log.error("Execution aborted after failure: %s", county)
                return final

    total_elapsed = round(time.time() - started_at, 2)
    success = len(failures) == 0

    final = {
        "success": success,
        "counties_requested": target_counties,
        "counties_completed": [r["county"] for r in results],
        "results": results,
        "failures": failures,
        "elapsed_seconds": total_elapsed,
    }

    if success:
        log.info(
            "All counties finished successfully in %.2fs",
            total_elapsed,
        )
    else:
        log.warning(
            "Execution finished with failures. success_count=%s failure_count=%s elapsed=%.2fs",
            len(results),
            len(failures),
            total_elapsed,
        )

    return final


def main():
    result = run()
    print(result)


if __name__ == "__main__":
    main()