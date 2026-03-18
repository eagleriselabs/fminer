"""
mine_spielplan_urls.py
======================
Iteriert über alle Verbände, Bewerbe (Gruppen) und Ligen auf
https://www.oefb.at/bewerbe/ und sammelt sämtliche Spielplan-URLs.

Die Seite verwendet Custom-Dropdown-Filter (keine <select>-Elemente):
  - Column 0: Verband    → ul.DS-verband li a  (data-id, data-url)
  - Column 1: Gruppe     → ul.DS-gruppe  li a  (data-id, data-url)
  - Column 2: Bewerb     → ul.DS-bewerb  li a  (data-id, data-url)
  - Column 3: Runde      (nicht relevant)
  - Column 4: Saison     (nicht relevant)

Ablauf:
  1. Seite laden → alle Verbände aus ul.DS-verband lesen
  2. Für jeden Verband: zu data-url navigieren → Gruppen aus ul.DS-gruppe lesen
  3. Für jede Gruppe: zu data-url navigieren → Bewerbe aus ul.DS-bewerb lesen
  4. Bewerb-URLs sammeln und /Bewerb/{ID} → /Bewerb/Spielplan/{ID} transformieren

Ausgabe: Python-Liste URLS = [...] auf stdout + optional Datei.
"""

import os
import re
import sys
import time
import json
import random
import argparse
import contextlib
from pathlib import Path
from typing import List, Set, Optional, Dict

from selenium import webdriver
from selenium.common.exceptions import (
    TimeoutException,
    WebDriverException,
    NoSuchElementException,
    ElementClickInterceptedException,
    ElementNotInteractableException,
)
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager

BASE_URL = "https://www.oefb.at/bewerbe/"
# Bewerb-URL: /bewerbe/Bewerb/{ID}?Name  →  Spielplan: /bewerbe/Bewerb/Spielplan/{ID}?Name
BEWERB_URL_RE = re.compile(r"(https://www\.oefb\.at/[^/]+)/Bewerb/(\d+)\?(.+)")

# ÖFB-Bundesligen: nicht über Dropdown-Filter erreichbar, daher fest hinterlegt
OEFB_EXTRA_URLS = [
    {"verband": "ÖFB", "bewerb": "Bundesligen Herren", "liga": "ADMIRAL Bundesliga Grunddurchgang",
     "link": "https://www.oefb.at/bewerbe/Bewerb/Spielplan/227113?ADMIRAL-Bundesliga-Grunddurchgang"},
    {"verband": "ÖFB", "bewerb": "Bundesligen Herren", "liga": "ADMIRAL 2. Liga",
     "link": "https://www.oefb.at/bewerbe/Bewerb/Spielplan/227112?ADMIRAL-2-Liga"},
    {"verband": "ÖFB", "bewerb": "Bundesligen Frauen", "liga": "ADMIRAL Frauen Bundesliga Grunddurchgang",
     "link": "https://www.oefb.at/oefb/Bewerb/Spielplan/226894?ADMIRAL-Frauen-Bundesliga-Grunddurchgang"},
    {"verband": "ÖFB", "bewerb": "Bundesligen Frauen", "liga": "Frauen Future League",
     "link": "https://www.oefb.at/oefb/Bewerb/Spielplan/226892?Frauen-Future-League"},
    {"verband": "ÖFB", "bewerb": "Bundesligen Frauen", "liga": "2. Frauen Bundesliga",
     "link": "https://www.oefb.at/oefb/Bewerb/Spielplan/226893?2-Frauen-Bundesliga"},
]


# ─── Driver Setup ────────────────────────────────────────────────────────────

def make_driver(headless: bool = True, page_load_timeout: int = 60) -> webdriver.Chrome:
    chrome_options = Options()
    chrome_bin = os.environ.get("CHROME_BIN") or os.environ.get("GOOGLE_CHROME_SHIM")
    if chrome_bin:
        chrome_options.binary_location = chrome_bin
    if headless:
        chrome_options.add_argument("--headless=new")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--window-size=1920,1080")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--disable-blink-features=AutomationControlled")
    chrome_options.add_argument("--remote-allow-origins=*")
    chrome_options.add_argument("--lang=de-DE,de")
    chrome_options.add_argument(
        "--user-agent=Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/120.0 Safari/537.36"
    )
    print("[INIT] Chrome WebDriver wird gestartet …")
    driver = webdriver.Chrome(
        service=Service(ChromeDriverManager().install()),
        options=chrome_options,
    )
    driver.set_page_load_timeout(page_load_timeout)
    print("[INIT] Chrome WebDriver bereit.")
    return driver


# ─── Hilfs-Funktionen ────────────────────────────────────────────────────────

def safe_get(driver: webdriver.Chrome, url: str, attempts: int = 3) -> None:
    for i in range(1, attempts + 1):
        try:
            driver.get(url)
            return
        except (TimeoutException, WebDriverException) as e:
            print(f"[WARN] get({url}) Versuch {i}/{attempts} fehlgeschlagen: {e}",
                  file=sys.stderr)
            if i == attempts:
                raise
            time.sleep(2 * i)


def dismiss_cookie_banner(driver: webdriver.Chrome) -> None:
    """Cookie-Banner akzeptieren, falls vorhanden."""
    for sel in [
        "button#onetrust-accept-btn-handler",
        "button.onetrust-close-btn-handler",
        "button[title='Akzeptieren']",
    ]:
        try:
            btn = driver.find_element(By.CSS_SELECTOR, sel)
            btn.click()
            print("[COOKIE] Cookie-Banner geschlossen.")
            time.sleep(1)
            return
        except (NoSuchElementException, ElementClickInterceptedException,
                ElementNotInteractableException):
            continue


def wait_short(lo: float = 0.5, hi: float = 1.5) -> None:
    time.sleep(lo + random.random() * (hi - lo))


def read_dropdown_items(driver: webdriver.Chrome, ul_class: str) -> List[Dict[str, str]]:
    """
    Liest alle <a>-Einträge aus einer Dropdown-Liste (z.B. ul.DS-verband).
    Gibt [{title, data_id, data_url}, ...] zurück.
    """
    items = []
    try:
        links = driver.find_elements(By.CSS_SELECTOR, f"ul.{ul_class} li a")
    except Exception:
        return items
    for link in links:
        title = (link.get_attribute("title") or link.text or "").strip()
        data_id = link.get_attribute("data-id") or ""
        data_url = link.get_attribute("data-url") or ""
        if data_url:
            items.append({"title": title, "data_id": data_id, "data_url": data_url})
    return items


def bewerb_url_to_spielplan(url: str) -> Optional[str]:
    """
    Transformiert /Bewerb/{ID}?Name → /Bewerb/Spielplan/{ID}?Name.
    Gibt None zurück, wenn das Muster nicht passt.
    """
    m = BEWERB_URL_RE.match(url)
    if m:
        return f"{m.group(1)}/Bewerb/Spielplan/{m.group(2)}?{m.group(3)}"
    return None


# ─── Kern-Logik ──────────────────────────────────────────────────────────────

def mine_spielplan_urls(
    driver: webdriver.Chrome,
    verband_filter: Optional[str] = None,
    debug: bool = False,
    screenshot_dir: Optional[Path] = None,
) -> List[Dict[str, str]]:
    """
    Hauptfunktion: Iteriert über Verbände → Gruppen → Bewerbe
    und sammelt Spielplan-URLs via data-url Attribute der Custom-Dropdowns.

    Gibt Liste von Dicts zurück: [{verband, bewerb, liga, link}, ...]
    """
    rows: List[Dict[str, str]] = []
    seen_links: Set[str] = set()

    # ── Schritt 1: Startseite laden und Verbände lesen ──
    print(f"[NAV] Lade Startseite: {BASE_URL}")
    safe_get(driver, BASE_URL)
    wait_short(3, 5)
    dismiss_cookie_banner(driver)
    wait_short(1, 2)

    if debug and screenshot_dir:
        screenshot_dir.mkdir(parents=True, exist_ok=True)
        driver.save_screenshot(str(screenshot_dir / "00_initial.png"))

    verbaende = read_dropdown_items(driver, "DS-verband")
    print(f"[INFO] {len(verbaende)} Verbände gefunden:")
    for v in verbaende:
        print(f"  - {v['title']}")

    if not verbaende:
        print("[FEHLER] Keine Verbände gefunden. Seite hat sich geändert?")
        return []

    # Filter auf bestimmten Verband
    if verband_filter:
        verbaende = [v for v in verbaende if verband_filter.lower() in v["title"].lower()]
        if not verbaende:
            print(f"[FEHLER] Verband '{verband_filter}' nicht gefunden.")
            return []
        print(f"[FILTER] Nur Verband: {verbaende[0]['title']}")

    # ── Schritt 2: Für jeden Verband → Gruppen lesen ──
    for v_idx, verband in enumerate(verbaende):
        verband_name = verband["title"]
        print(f"\n{'='*60}")
        print(f"[VERBAND {v_idx+1}/{len(verbaende)}] {verband_name}")
        print(f"{'='*60}")

        # Navigiere zur Verband-Seite (data-url)
        print(f"[NAV] → {verband['data_url']}")
        safe_get(driver, verband["data_url"])
        wait_short(2, 4)
        dismiss_cookie_banner(driver)

        if debug and screenshot_dir:
            driver.save_screenshot(
                str(screenshot_dir / f"01_verband_{v_idx}.png")
            )

        gruppen = read_dropdown_items(driver, "DS-gruppe")
        print(f"[INFO] {len(gruppen)} Gruppen (Bewerbe) gefunden:")
        for g in gruppen:
            print(f"  - {g['title']}")

        if not gruppen:
            # Eventuell direkt Bewerbe (Ligen) sichtbar
            bewerbe = read_dropdown_items(driver, "DS-bewerb")
            for b in bewerbe:
                sp_url = bewerb_url_to_spielplan(b["data_url"])
                if sp_url and sp_url not in seen_links:
                    seen_links.add(sp_url)
                    rows.append({"verband": verband_name, "bewerb": "", "liga": b["title"], "link": sp_url})
                    print(f"  [+] {b['title']} → {sp_url}")
            continue

        # ── Schritt 3: Für jede Gruppe → Bewerbe (Ligen) lesen ──
        for g_idx, gruppe in enumerate(gruppen):
            gruppe_name = gruppe["title"]
            print(f"\n  [BEWERB {g_idx+1}/{len(gruppen)}] {gruppe_name}")

            # Navigiere zur Gruppe-Seite
            print(f"  [NAV] → {gruppe['data_url']}")
            safe_get(driver, gruppe["data_url"])
            wait_short(2, 3)
            dismiss_cookie_banner(driver)

            if debug and screenshot_dir:
                driver.save_screenshot(
                    str(screenshot_dir / f"02_gruppe_{v_idx}_{g_idx}.png")
                )

            bewerbe = read_dropdown_items(driver, "DS-bewerb")
            print(f"  [INFO] {len(bewerbe)} Ligen gefunden:")

            for b in bewerbe:
                sp_url = bewerb_url_to_spielplan(b["data_url"])
                if sp_url and sp_url not in seen_links:
                    seen_links.add(sp_url)
                    rows.append({"verband": verband_name, "bewerb": gruppe_name, "liga": b["title"], "link": sp_url})
                    print(f"    [+] {b['title']} → {sp_url}")
                elif not sp_url:
                    print(f"    [?] {b['title']} → {b['data_url']} (kein Standard-Spielplan-Muster)")

    # ÖFB-Bundesligen hinzufügen (nicht über Dropdowns erreichbar)
    for extra in OEFB_EXTRA_URLS:
        if extra["link"] not in seen_links:
            seen_links.add(extra["link"])
            rows.append(extra)
            print(f"[+ÖFB] {extra['liga']} → {extra['link']}")

    return rows


def format_as_python_list(urls: List[str]) -> str:
    """Formatiert URLs als Python-Liste für Copy-Paste."""
    if not urls:
        return "URLS = []\n"
    lines = ["URLS = ["]
    for url in urls:
        lines.append(f'    "{url}",')
    lines.append("]")
    return "\n".join(lines) + "\n"


# ─── Hauptprogramm ──────────────────────────────────────────────────────────

def save_csv(rows: List[Dict[str, str]], csv_path: Path) -> None:
    """Speichert die Ergebnisse als CSV mit sep=';'."""
    import csv
    csv_path.parent.mkdir(parents=True, exist_ok=True)
    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["verband", "bewerb", "liga", "link"], delimiter=";")
        writer.writeheader()
        writer.writerows(rows)
    print(f"[DATEI] CSV geschrieben: {csv_path} ({len(rows)} Zeilen)")


def main():
    parser = argparse.ArgumentParser(
        description="Sammelt Spielplan-URLs von https://www.oefb.at/bewerbe/"
    )
    parser.add_argument(
        "--outfile", "-o",
        default="results/spielplan_urls.csv",
        help="Ausgabe-CSV (Default: results/spielplan_urls.csv).",
    )
    parser.add_argument(
        "--verband",
        default="Wiener Fußballverband",
        help="Nur diesen Verband extrahieren (Teilstring reicht). Default: Wiener Fußballverband.",
    )
    parser.add_argument(
        "--no-headless",
        action="store_true",
        help="Browser sichtbar starten (zum Debugging).",
    )
    parser.add_argument(
        "--debug",
        action="store_true",
        help="Detailliertes Logging und Screenshots.",
    )
    parser.add_argument(
        "--screenshot-dir",
        default="screenshots",
        help="Verzeichnis für Debug-Screenshots (Default: screenshots/).",
    )
    args = parser.parse_args()

    driver = make_driver(headless=not args.no_headless)

    try:
        rows = mine_spielplan_urls(
            driver,
            verband_filter=args.verband,
            debug=args.debug,
            screenshot_dir=Path(args.screenshot_dir) if args.debug else None,
        )
    finally:
        with contextlib.suppress(Exception):
            driver.quit()
            print("[EXIT] WebDriver beendet.")

    # Ergebnis ausgeben
    print(f"\n{'='*60}")
    print(f"[ERGEBNIS] {len(rows)} Spielplan-URLs gefunden.")
    print(f"{'='*60}\n")

    for row in rows:
        print(f"  {row['verband']} | {row['bewerb']} | {row['liga']} | {row['link']}")

    # Python-Liste ausgeben
    output = format_as_python_list([r["link"] for r in rows])
    print(f"\n{output}")

    # CSV speichern
    out_path = Path(args.outfile)
    save_csv(rows, out_path)


if __name__ == "__main__":
    main()
