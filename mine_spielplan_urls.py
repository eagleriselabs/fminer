"""
mine_spielplan_urls.py
======================
Extrahiert alle Bewerb-Links aus dem Hovermenü "Ligen & Bewerbe"
auf https://wfv.at/wfv/ und sammelt sämtliche Spielplan-URLs.

Das Menü ist als verschachtelte <ul>/<li>-Struktur aufgebaut:
  - Kategorie (bewerb): <a class="has_drop"> z.B. "Bundesliga", "Regionalliga"
    - Liga:              <a href="/wfv/Bewerb/{ID}?{Name}"> z.B. "ADMIRAL BL - Meistergruppe"

Die Menüeinträge befinden sich immer im DOM (CSS-hidden bei Nicht-Hover),
daher reicht ein einziger Seitenaufruf + JavaScript-Extraktion.

Ausgabe: CSV mit Spalten verband;bewerb;liga;link
"""

import os
import sys
import csv
import time
import random
import argparse
import contextlib
from pathlib import Path
from typing import List, Set, Dict

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
from webdriver_manager.chrome import ChromeDriverManager

BASE_URL = "https://wfv.at/wfv/"
VERBAND = "Wiener Fußballverband"


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
    # XPath-Fallback für wfv.at Cookie-Banner
    try:
        btn = driver.find_element(
            By.XPATH, "//button[contains(text(), 'Alle Cookies akzeptieren')]"
        )
        btn.click()
        print("[COOKIE] Cookie-Banner geschlossen (XPath).")
        time.sleep(1)
    except (NoSuchElementException, ElementClickInterceptedException,
            ElementNotInteractableException):
        pass


def wait_short(lo: float = 0.5, hi: float = 1.5) -> None:
    time.sleep(lo + random.random() * (hi - lo))


# ─── Kern-Logik ──────────────────────────────────────────────────────────────

# JavaScript das die hierarchische Menüstruktur aus dem DOM extrahiert.
# Das Hovermenü "Ligen & Bewerbe" enthält:
#   <li>
#     <a class="root_url datenservice">Ligen & Bewerbe</a>
#     <div class="main_nav_drop main_nav_drop_datenservice">
#       <ul class="datenservice_drop_0">
#         <li>
#           <a class="has_drop" title="Bundesliga">Bundesliga</a>
#           <ul class="datenservice_drop_0_0">
#             <li><a href="/wfv/Bewerb/{ID}?{name}">Liga-Name</a></li>
#             ...
#           </ul>
#         </li>
#         ...
#       </ul>
#     </div>
#   </li>

EXTRACT_MENU_JS = r"""
var results = [];
var allAnchors = document.querySelectorAll('a');
var menuParent = null;

for (var i = 0; i < allAnchors.length; i++) {
    if (allAnchors[i].textContent.trim() === 'Ligen & Bewerbe') {
        menuParent = allAnchors[i].closest('li');
        break;
    }
}
if (!menuParent) return results;

// Kategorie-Einträge: <a class="has_drop"> als Bewerb-Kategorie
var categoryLinks = menuParent.querySelectorAll('a.has_drop');
categoryLinks.forEach(function(catA) {
    var bewerb = (catA.getAttribute('title') || catA.textContent || '').trim();
    var parentLi = catA.closest('li');
    if (!parentLi) return;

    // Alle Bewerb-Links innerhalb dieser Kategorie
    var subLinks = parentLi.querySelectorAll(':scope > ul a[href*="/Bewerb/"]');
    subLinks.forEach(function(a) {
        var href = a.href;
        if (!href.match(/\/Bewerb\/(Turniere\/)?\d+/)) return;
        results.push({
            bewerb: bewerb,
            liga: a.textContent.trim(),
            href: href
        });
    });
});
return results;
"""


def mine_spielplan_urls(
    driver: webdriver.Chrome,
    debug: bool = False,
    screenshot_dir: Path = None,
) -> List[Dict[str, str]]:
    """
    Lädt https://wfv.at/wfv/ und extrahiert alle Bewerb-Links
    aus dem Hovermenü "Ligen & Bewerbe" (ein einziger Seitenaufruf).

    Gibt Liste von Dicts zurück: [{verband, bewerb, liga, link}, ...]
    """
    rows: List[Dict[str, str]] = []
    seen_links: Set[str] = set()

    print(f"[NAV] Lade Startseite: {BASE_URL}")
    safe_get(driver, BASE_URL)
    wait_short(3, 5)
    dismiss_cookie_banner(driver)
    wait_short(1, 2)

    if debug and screenshot_dir:
        screenshot_dir.mkdir(parents=True, exist_ok=True)
        driver.save_screenshot(str(screenshot_dir / "00_initial.png"))

    # Menüstruktur via JavaScript extrahieren
    print("[INFO] Extrahiere Bewerb-Links aus Hovermenü 'Ligen & Bewerbe' …")
    menu_items = driver.execute_script(EXTRACT_MENU_JS)

    if not menu_items:
        print("[FEHLER] Keine Bewerb-Links im Hovermenü gefunden. Seitenstruktur geändert?")
        return []

    print(f"[INFO] {len(menu_items)} Bewerb-Links extrahiert.\n")

    for item in menu_items:
        link = item["href"]
        if link in seen_links:
            continue
        seen_links.add(link)
        row = {
            "verband": VERBAND,
            "bewerb": item["bewerb"],
            "liga": item["liga"],
            "link": link,
        }
        rows.append(row)
        print(f"  [+] {item['bewerb']:30s} | {item['liga']} → {link}")

    return rows


# ─── Hauptprogramm ──────────────────────────────────────────────────────────

def save_csv(rows: List[Dict[str, str]], csv_path: Path) -> None:
    """Speichert die Ergebnisse als CSV mit sep=';'."""
    csv_path.parent.mkdir(parents=True, exist_ok=True)
    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["verband", "bewerb", "liga", "link"], delimiter=";")
        writer.writeheader()
        writer.writerows(rows)
    print(f"[DATEI] CSV geschrieben: {csv_path} ({len(rows)} Zeilen)")


def main():
    parser = argparse.ArgumentParser(
        description="Sammelt Spielplan-URLs aus dem Hovermenü auf https://wfv.at/wfv/"
    )
    parser.add_argument(
        "--outfile", "-o",
        default="results/spielplan_urls.csv",
        help="Ausgabe-CSV (Default: results/spielplan_urls.csv).",
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

    # CSV speichern
    out_path = Path(args.outfile)
    save_csv(rows, out_path)


if __name__ == "__main__":
    main()
