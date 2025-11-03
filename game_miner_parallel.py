import os
import sys
import json
import time
import random
import argparse
import contextlib
from typing import Tuple, Optional, List, Dict

import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed

from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, WebDriverException, StaleElementReferenceException
from webdriver_manager.chrome import ChromeDriverManager

# ============== Helpers from original ==============

def get_coordinates(container) -> Tuple[Optional[float], Optional[float]]:
    driver = container.parent
    try:
        WebDriverWait(driver, 20).until(
            lambda d: d.execute_script("""
                const ap = window.SG?.container?.appPreloads;
                if (!ap) return false;
                for (const k in ap) {
                    const arr = Array.isArray(ap[k]) ? ap[k] : [ap[k]];
                    for (const obj of arr) {
                        if (obj && typeof obj.longitude === 'number' && typeof obj.latitude === 'number') {
                            if (!(obj.longitude === 0 && obj.latitude === 0)) return true;
                        }
                    }
                }
                return false;
            """)
        )
        result = driver.execute_script("""
            const ap = window.SG?.container?.appPreloads || {};
            for (const k in ap) {
                const arr = Array.isArray(ap[k]) ? ap[k] : [ap[k]];
                for (const obj of arr) {
                    if (obj && typeof obj.longitude === 'number' && typeof obj.latitude === 'number') {
                        if (!(obj.longitude === 0 && obj.latitude === 0)) {
                            return [obj.latitude, obj.longitude];
                        }
                    }
                }
            }
            return null;
        """)
        if result:
            return tuple(result)
        return None, None
    except Exception:
        return None, None

def get_bewerb(container) -> Optional[str]:
    driver = container.parent
    WebDriverWait(driver, 20).until(
        lambda d: d.execute_script("return !!(window.SG && SG.container && SG.container.appPreloads);")
    )
    bewerb = driver.execute_script("""
        const ap = window.SG.container.appPreloads || {};
        for (const k in ap) {
            const arr = Array.isArray(ap[k]) ? ap[k] : [ap[k]];
            for (const obj of arr) {
                if (obj && typeof obj.bewerb === 'string' && obj.bewerb.trim()) return obj.bewerb;
            }
        }
        return null;
    """)
    return bewerb

# ============== WebDriver utilities ==============

def build_chrome_options(headless: bool = True) -> webdriver.ChromeOptions:
    opts = webdriver.ChromeOptions()
    chrome_bin = os.environ.get("CHROME_BIN") or os.environ.get("GOOGLE_CHROME_SHIM")
    if chrome_bin:
        opts.binary_location = chrome_bin
    if headless:
        opts.add_argument("--headless=new")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--disable-gpu")
    opts.add_argument("--window-size=1280,900")
    opts.add_argument("--lang=de-DE,de")
    opts.add_argument("--user-agent=Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome Safari")
    opts.add_argument("--disable-blink-features=AutomationControlled")
    opts.add_argument("--remote-allow-origins=*")
    return opts

def open_driver(driver_path: str, headless: bool = True) -> webdriver.Chrome:
    options = build_chrome_options(headless=headless)
    driver = webdriver.Chrome(service=Service(driver_path), options=options)
    driver.set_page_load_timeout(60)
    return driver

def safe_get(driver: webdriver.Chrome, url: str, attempts: int = 3, backoff: float = 1.5) -> None:
    for i in range(1, attempts + 1):
        try:
            driver.get(url)
            return
        except (TimeoutException, WebDriverException) as e:
            print(f"[WARN] GET failed ({i}/{attempts}) for {url}: {e}", file=sys.stderr, flush=True)
            if i == attempts:
                raise
            time.sleep(backoff * i)

# ============== Core scraping ==============

def mine_game(weblink: str, driver_path: str, headless: bool = True) -> Dict:
    def get_text_safe(el):
        return el.text.strip() if el else ""

    driver = open_driver(driver_path, headless=headless)
    try:
        safe_get(driver, weblink, attempts=3)
        wait = WebDriverWait(driver, 25)
        container = wait.until(EC.visibility_of_element_located((By.CSS_SELECTOR, ".round_overview_container")))

        # basic top fields
        runde = get_text_safe(container.find_element(By.CSS_SELECTOR, ".round"))
        datum = get_text_safe(container.find_element(By.CSS_SELECTOR, ".date"))

        # teams
        team_links = container.find_elements(By.CSS_SELECTOR, ".teams > a")
        if len(team_links) < 2:
            wait.until(lambda d: len(container.find_elements(By.CSS_SELECTOR, ".teams > a")) >= 2)
            team_links = container.find_elements(By.CSS_SELECTOR, ".teams > a")

        heim_a, gast_a = team_links[0], team_links[1]
        heim_name = heim_a.get_attribute("title") or get_text_safe(heim_a)
        gast_name = gast_a.get_attribute("title") or get_text_safe(gast_a)
        heim_link = heim_a.get_attribute("href") or ""
        gast_link = gast_a.get_attribute("href") or ""

        liga = get_bewerb(container) or ""

        def value_after(label):
            xpath = f'.//div[@class="detail"]//span[normalize-space()="{label}"]/following-sibling::span[1]'
            try:
                return container.find_element(By.XPATH, xpath).text.strip()
            except Exception:
                return ""

        spielbeginn = value_after("Spielbeginn:")

        # Spielort & Adresse
        spielort_name, adresse = "", ""
        try:
            place_blocks = driver.find_elements(By.CSS_SELECTOR, ".game_place_content_1")
            for blk in place_blocks:
                try:
                    h4 = blk.find_element(By.TAG_NAME, "h4").text.strip().lower()
                except Exception:
                    continue
                if "adresse" in h4 and "anfahrt" in h4:
                    try:
                        spielort_name = blk.find_element(By.CSS_SELECTOR, "h5.highlight").text.strip()
                    except Exception:
                        spielort_name = ""
                    lines = [ln.strip() for ln in blk.text.splitlines() if ln.strip()]
                    if lines and lines[0].lower().startswith("adresse"):
                        lines = lines[1:]
                    if lines and spielort_name and lines[0] == spielort_name:
                        lines = lines[1:]
                    adresse = ", ".join(lines)
                    break
        except Exception:
            pass

        lat, lon = get_coordinates(container)

        # combine date+time
        datetime_str = f"{datum} {spielbeginn}".strip()
        datum_dt = pd.to_datetime(datetime_str, format="%d.%m.%Y %H:%M", errors="coerce")

        row = {
            "Datum": datum_dt,
            "Liga": liga,
            "Typ": "Frau" if (isinstance(liga, str) and "Frau" in liga) else "Mann",
            "Runde": runde,
            "Heim": heim_name,
            "Gast": gast_name,
            "Heim_Link": heim_link,
            "Gast_Link": gast_link,
            "Spielort_Name": spielort_name,
            "Adresse": adresse,
            "Latitude": lat,
            "Longitude": lon,
            "Quelle": weblink,
            "link": weblink.replace("/Spielbericht/", "/"),
        }
        return row

    except Exception as e:
        return {
            "Datum": None, "Liga": None, "Typ": None, "Runde": None,
            "Heim": None, "Gast": None, "Heim_Link": None, "Gast_Link": None,
            "Spielort_Name": None, "Adresse": None, "Latitude": None, "Longitude": None,
            "Quelle": weblink, "link": weblink.replace("/Spielbericht/", "/"),
            "error": str(e),
        }
    finally:
        with contextlib.suppress(Exception):
            driver.quit()

# ============== Controller ==============

def normalize_link(u: str) -> str:
    return (u or "").replace("/Spielbericht/", "/")

def load_links(input_csv: str) -> List[str]:
    df = pd.read_csv(input_csv, encoding="utf-8-sig")
    if "href" not in df.columns:
        raise ValueError(f"Input CSV {input_csv} must have an 'href' column (got columns: {list(df.columns)})")
    links = [normalize_link(x) for x in df["href"].astype(str).tolist()]
    return links

def run_parallel(
    input_csv: str,
    out_csv: str,
    max_workers: int = 6,
    flush_every: int = 25,
    headless: bool = True,
) -> None:

    # 1) Links laden
    links_all = load_links(input_csv)

    # 2) Bereits vorhandene Ergebnisse laden/merken (Resume)
    if os.path.exists(out_csv):
        df_existing = pd.read_csv(out_csv, encoding="utf-8-sig", sep=";")
        have = set(df_existing["link"].astype(str).tolist())
        results = df_existing.to_dict("records")
    else:
        have = set()
        results = []

    # 3) Übrig
    todo = [l for l in links_all if l not in have]
    if not todo:
        print("Alles erledigt – keine neuen Links.", flush=True)
        # trotzdem sicherstellen, dass Datei existiert:
        if not os.path.exists(out_csv):
            pd.DataFrame(results).to_csv(out_csv, index=False, encoding="utf-8-sig", sep=";")
        return

    # 4) Chromedriver nur einmal installieren
    driver_path = ChromeDriverManager().install()

    print(f"Starte parallel: {len(todo)} Seiten, max_workers={max_workers}", flush=True)
    completed_since_flush = 0

    def write_out():
        pd.DataFrame(results).to_csv(out_csv, index=False, encoding="utf-8-sig", sep=";")
        print(f"Zwischenspeicher: {len(results)} Einträge -> {out_csv}", flush=True)

    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        future_map = {ex.submit(mine_game, link, driver_path, headless): link for link in todo}
        for fut in as_completed(future_map):
            link = future_map[fut]
            try:
                row = fut.result()
            except Exception as e:
                row = {"Quelle": link, "link": link, "error": f"FutureException: {e}"}
            results.append(row)
            have.add(row.get("link", link))
            completed_since_flush += 1

            print(f"Fertig: {link}  ({len(have)}/{len(links_all)})", flush=True)

            if completed_since_flush >= flush_every:
                write_out()
                completed_since_flush = 0

    # final write
    write_out()
    print(f"Fertig! Gesamt: {len(results)} Einträge -> {out_csv}", flush=True)

# ============== CLI ==============

def main():
    parser = argparse.ArgumentParser(description="Paralleler Game-Miner für ÖFB-Spielseiten.")
    parser.add_argument("--in", dest="in_path", default="oefb_links_gesamt.csv",
                        help="Eingabe-CSV (von fminer.py) mit Spalte 'href'.")
    parser.add_argument("--out", dest="out_dir", default=".",
                        help="Ausgabe-Ordner (Datei 'spiel_infos.csv' wird dort angelegt).")
    parser.add_argument("--outfile", dest="out_file", default=None,
                        help="Optional: expliziter Ausgabedateiname (überschreibt --out).")
    parser.add_argument("--workers", type=int, default=10, help="Maximale Parallelität.")
    parser.add_argument("--flush-every", type=int, default=25, help="Nach wie vielen Ergebnissen Zwischen-Speichern.")
    parser.add_argument("--no-headless", action="store_true", help="Nicht headless (Debug).")

    args = parser.parse_args()

    in_csv = args.in_path
    if not os.path.exists(in_csv):
        # Fall back: common CI path from fminer step
        alt = os.path.join("results", "fminer", "oefb_links_gesamt.csv")
        if os.path.exists(alt):
            in_csv = alt
        else:
            raise FileNotFoundError(f"Input CSV not found: {args.in_path} (also checked {alt})")

    if args.out_file:
        out_csv = args.out_file
        os.makedirs(os.path.dirname(out_csv) or ".", exist_ok=True)
    else:
        out_dir = args.out_dir or "."
        os.makedirs(out_dir, exist_ok=True)
        out_csv = os.path.join(out_dir, "spiel_infos.csv")

    run_parallel(
        input_csv=in_csv,
        out_csv=out_csv,
        max_workers=max(1, args.workers),
        flush_every=max(1, args.flush_every),
        headless=not args.no_headless,
    )

if __name__ == "__main__":
    main()
