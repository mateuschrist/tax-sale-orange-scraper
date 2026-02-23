# adapters/orange.py
from __future__ import annotations

def run():
    # Importa e executa o runner do scraper principal
    from scraper import run as run_scraper
    run_scraper()