import csv
import os
import sys
import time
import random
import contextlib
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, List, Tuple, Set, Optional

from selenium import webdriver
from selenium.common.exceptions import TimeoutException, StaleElementReferenceException, WebDriverException
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager

# =========================
# Konfiguration
# =========================
DEFAULT_OUTFILE = "oefb_links_gesamt.csv"

URLS = [
    "https://www.oefb.at/bewerbe/Bewerb/Spielplan/227113?ADMIRAL-Bundesliga-Grunddurchgang",
    "https://www.oefb.at/bewerbe/Bewerb/Spielplan/227112?ADMIRAL-2-Liga",
    "https://www.oefb.at/bewerbe/Bewerb/Spielplan/226510?Regionalliga-Ost",
    "https://www.oefb.at/bewerbe/Bewerb/Spielplan/226635?Wiener-Stadtliga",
    "https://www.oefb.at/bewerbe/Bewerb/Spielplan/226684?2-Landesliga",
    "https://www.oefb.at/bewerbe/Bewerb/Spielplan/226695?Oberliga-A",
    "https://www.oefb.at/bewerbe/Bewerb/Spielplan/226591?Oberliga-B",
    "https://www.oefb.at/bewerbe/Bewerb/Spielplan/227122?Oberliga-A-Reserve",
    "https://www.oefb.at/bewerbe/Bewerb/Spielplan/227123?Oberliga-B-Reserve",
    "https://www.oefb.at/bewerbe/Bewerb/Spielplan/226633?1-Klasse-A",
    "https://www.oefb.at/bewerbe/Bewerb/Spielplan/226627?1-Klasse-B",
    "https://www.oefb.at/bewerbe/Bewerb/Spielplan/227124?1-Klasse-A-Reserve",
    "https://www.oefb.at/bewerbe/Bewerb/Spielplan/227125?1-Klasse-B-Reserve",
    "https://www.oefb.at/oefb/Bewerb/Spielplan/226894?ADMIRAL-Frauen-Bundesliga-Grunddurchgang",
    "https://www.oefb.at/oefb/Bewerb/Spielplan/226892?Frauen-Future-League",
    "https://www.oefb.at/oefb/Bewerb/Spielplan/226893?2-Frauen-Bundesliga",
    "https://www.oefb.at/bewerbe/Bewerb/Spielplan/226649?Wiener-Frauen-Landesliga",
    "https://www.oefb.at/bewerbe/Bewerb/Spielplan/226662?Frauen-1-Klasse",
    "https://www.oefb.at/bewerbe/Bewerb/Spielplan/226664?Frauen-2-Klasse-",
    "https://www.oefb.at/bewerbe/Bewerb/Spielplan/226672?Frauen-Newcomer-Liga",
    "https://www.oefb.at/bewerbe/Bewerb/Spielplan/226618?DSG-LIGA",
    "https://www.oefb.at/bewerbe/Bewerb/Spielplan/226621?DSG-Oberliga-A",
    "https://www.oefb.at/bewerbe/Bewerb/Spielplan/226646?DSG-Oberliga-B",
    "https://www.oefb.at/bewerbe/Bewerb/Spielplan/226631?DSG-Unterliga-A",
    "https://www.oefb.at/bewerbe/Bewerb/Spielplan/226682?DSG-Unterliga-B",
    "https://www.oefb.at/bewerbe/Bewerb/Spielplan/226610?DSG-1-Klasse-A",
    "https://www.oefb.at/bewerbe/Bewerb/Spielplan/226615?DSG-1-Klasse-B",
    "https://www.oefb.at/bewerbe/Bewerb/Spielplan/226694?DSG-2-Klasse-A",
    "https://www.oefb.at/bewerbe/Bewerb/Spielplan/226678?DSG-2-Klasse-B",
    "https://www.oefb.at/bewerbe/Bewerb/Spielplan/227359?DSG-Reserve",
    "https://www.oefb.at/bewerbe/Bewerb/Spielplan/226697?DSG-Cup",
    "https://www.oefb.at/bewerbe/Bewerb/Spielplan/226690?DSG-Frauen-Maedchen",
    "https://www.oefb.at/bewerbe/Bewerb/Spielplan/226665?DSG-Senioren-1A",
    "https://www.oefb.at/bewerbe/Bewerb/Spielplan/226673?DSG-Senioren-1B",
    "https://www.oefb.at/bewerbe/Bewerb/Spielplan/226652?DSG-Senioren-2"
]


@dataclass
class ScrapeResult:
    url: str
    items: List[Tuple[str, str]]  # (text, href)


# =========================
# Setup & Utilities
# =========================
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
    chrome_options.add_argument("--user-agent=Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome Safari")

    print("[INIT] Chrome WebDriver wird gestartet …")
    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=chrome_options)
    driver.set_page_load_timeout(page_load_timeout)
    print("[INIT] Chrome WebDriver bereit.")
    return driver


def wait_for_schedule_table(driver: webdriver.Chrome, timeout: int = 30) -> None:
    print(f"[WAIT] Warte bis 'schedule_table' geladen ist (Timeout {timeout}s) …")
    WebDriverWait(driver, timeout).until(
        EC.presence_of_element_located((By.CSS_SELECTOR, "div.schedule_table"))
    )
    print("[WAIT] 'schedule_table' gefunden.")


def collect_links_from_page(driver: webdriver.Chrome, source_url: str) -> List[Tuple[str, str, str, str]]:
    print(f"[SCRAPE] Sammle Links von Seite: {source_url}")
    link_elements = driver.find_elements(By.CSS_SELECTOR, "div.schedule_table a[href]")
    out: List[Tuple[str, str, str, str]] = []
    seen_local: Set[str] = set()
    now_iso = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())

    print(f"[SCRAPE] {len(link_elements)} Link-Elemente gefunden. Filtere …")

    for el in link_elements:
        href = el.get_attribute("href") or ""
        # Änderung: '/Spielbericht/' entfernen
        href = href.replace("/Spielbericht/", "/")
        text = (el.text or "").strip()
        if not href or href in seen_local:
            continue
        if "verein" in href.lower():
            continue
        if "oefb.at" not in href:
            continue
        seen_local.add(href)
        out.append((text, href, source_url, now_iso))
    print(f"[SCRAPE] {len(out)} gültige Links gesammelt (nach Filterung).")
    return out


def load_existing_hrefs(csv_path: Path) -> Set[str]:
    existing: Set[str] = set()
    if not csv_path.exists():
        print(f"[CSV] {csv_path} existiert noch nicht, keine bestehenden Links geladen.")
        return existing
    try:
        with open(csv_path, "r", newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                href = (row.get("href") or "").strip()
                if href:
                    existing.add(href)
        print(f"[CSV] {len(existing)} bestehende hrefs aus {csv_path} geladen.")
    except Exception as e:
        print(f"[WARN] Konnte bestehende CSV nicht lesen: {e}")
        return set()
    return existing


def ensure_csv_with_header(csv_path: Path) -> None:
    csv_path.parent.mkdir(parents=True, exist_ok=True)
    if not csv_path.exists() or csv_path.stat().st_size == 0:
        print(f"[CSV] Neue Datei mit Header wird erstellt: {csv_path}")
        with open(csv_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(["href", "source_url", "first_seen_utc"])
    else:
        print(f"[CSV] Datei {csv_path} existiert bereits – Header bleibt unverändert.")


def append_row_immediate(fh, writer: csv.writer, row: Tuple[str, str, str]) -> None:
    writer.writerow(row)
    fh.flush()
    try:
        os.fsync(fh.fileno())
    except OSError:
        pass
    print(f"[WRITE] Neuer Eintrag: {row[0]}")


@contextlib.contextmanager
def file_lock(lock_path: Path, wait_secs: int = 30):
    start = time.time()
    print(f"[LOCK] Warte auf Lock-Datei: {lock_path}")
    while lock_path.exists():
        if time.time() - start > wait_secs:
            try:
                if time.time() - lock_path.stat().st_mtime > 3600:
                    lock_path.unlink(missing_ok=True)
                    print("[LOCK] Alte Lock-Datei entfernt (älter als 1h).")
                    break
            except Exception:
                pass
            time.sleep(1)
        else:
            time.sleep(0.5)
    try:
        lock_path.write_text(str(os.getpid()))
        print("[LOCK] Lock gesetzt.")
    except Exception:
        pass
    try:
        yield
    finally:
        with contextlib.suppress(Exception):
            lock_path.unlink(missing_ok=True)
            print("[LOCK] Lock entfernt.")


# =========================
# Kernlogik (Streaming)
# =========================
def safe_get(driver: webdriver.Chrome, url: str, attempts: int = 3) -> None:
    for i in range(1, attempts + 1):
        try:
            print(f"[NAV] Öffne URL (Versuch {i}/{attempts}): {url}")
            driver.get(url)
            return
        except (TimeoutException, WebDriverException) as e:
            print(f"[WARN] get({url}) Versuch {i}/{attempts} fehlgeschlagen: {e}", file=sys.stderr)
            if i == attempts:
                raise
            time.sleep(1.5 * i)


def click_more_until_stable(driver: webdriver.Chrome, max_clicks: int = 10_000) -> None:
    clicks = 0
    print("[CLICK] Suche nach 'Mehr'-Buttons …")
    while clicks < max_clicks:
        try:
            btn = WebDriverWait(driver, 2).until(
                EC.element_to_be_clickable(
                    (By.XPATH,
                     "//button[contains(., 'Mehr') or contains(., 'Load') or contains(., 'Show') or contains(., 'Weitere')]")
                )
            )
            btn.click()
            clicks += 1
            print(f"[CLICK] 'Mehr'-Button geklickt ({clicks}).")
            time.sleep(0.8 + random.random() * 0.4)
        except TimeoutException:
            print("[CLICK] Keine weiteren Buttons gefunden.")
            break
        except StaleElementReferenceException:
            time.sleep(0.5)
            continue


def stream_links_from_url(driver: webdriver.Chrome, url: str, seen: Set[str], fh, writer: csv.writer) -> Tuple[int, int]:
    print(f"[RUN] Beginne Streaming für {url}")
    safe_get(driver, url, attempts=3)
    wait_for_schedule_table(driver, timeout=45)

    total_found = 0
    total_new = 0

    items = collect_links_from_page(driver, source_url=url)
    total_found = max(total_found, len(items))
    for text, href, src, ts in items:
        if href not in seen:
            seen.add(href)
            append_row_immediate(fh, writer, (href, src, ts))
            total_new += 1

    click_more_until_stable(driver)
    items = collect_links_from_page(driver, source_url=url)
    total_found = max(total_found, len(items))
    new_round = 0
    for text, href, src, ts in items:
        if href not in seen:
            seen.add(href)
            append_row_immediate(fh, writer, (href, src, ts))
            total_new += 1
            new_round += 1
    if new_round:
        print(f"[STREAM] {url}: +{new_round} neu (ins CSV geschrieben).")

    print(f"[DONE] {url}: sichtbar {total_found} Links, {total_new} neue.")
    return total_found, total_new


def scrape_multiple_urls_streaming(urls: Iterable[str], outfile: Path, headless: bool = True, already_seen: Optional[Set[str]] = None) -> None:
    csv_path = Path(outfile)
    ensure_csv_with_header(csv_path)
    global_seen: Set[str] = set(already_seen or set())
    driver = make_driver(headless=headless)

    try:
        lock_path = csv_path.with_suffix(csv_path.suffix + ".lock")
        with file_lock(lock_path):
            with open(csv_path, "a", newline="", encoding="utf-8") as fh:
                writer = csv.writer(fh)
                for url in urls:
                    try:
                        stream_links_from_url(driver, url, global_seen, fh, writer)
                    except Exception as e:
                        print(f"[FEHLER] {url}: {e}", file=sys.stderr)
    finally:
        with contextlib.suppress(Exception):
            driver.quit()
            print("[EXIT] WebDriver beendet.")


# =========================
# CLI-Einstiegspunkt
# =========================
if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Sammelt Links aus mehreren ÖFB-Planungsseiten und schreibt sie in Echtzeit ins CSV.")
    parser.add_argument("--urls", nargs="*", default=URLS)
    parser.add_argument("--outfile", default=None)
    parser.add_argument("--out", default=None)
    parser.add_argument("--no-headless", action="store_true")
    parser.add_argument("--fresh", action="store_true")
    args = parser.parse_args()

    if args.outfile:
        out_path = Path(args.outfile)
    else:
        out_dir = Path(args.out or ".")
        out_dir.mkdir(parents=True, exist_ok=True)
        out_path = out_dir / DEFAULT_OUTFILE

    if args.fresh:
        existing_hrefs = set()
        if out_path.exists():
            out_path.unlink()
        print(f"[INFO] Fresh-Run: {out_path} wird neu angelegt.")
    else:
        existing_hrefs = load_existing_hrefs(out_path)
        print(f"[INFO] Resume: {len(existing_hrefs)} vorhandene Links werden übersprungen.")

    print("[MAIN] Starte Scraping-Prozess …")
    scrape_multiple_urls_streaming(
        args.urls,
        outfile=out_path,
        headless=not args.no_headless,
        already_seen=existing_hrefs,
    )

    print("[MAIN] Fertig.")
