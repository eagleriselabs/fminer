import csv
import os
import sys
import time
import random
import contextlib
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, List, Tuple, Set, Optional
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed

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
DEFAULT_SPIELPLAN_CSV = os.path.join(os.path.dirname(__file__), "results", "spielplan_urls.csv")


def load_urls_from_csv(csv_path: str) -> List[str]:
    """Liest Spielplan-URLs aus der von mine_spielplan_urls.py erzeugten CSV."""
    urls: List[str] = []
    with open(csv_path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f, delimiter=";")
        for row in reader:
            link = (row.get("link") or "").strip()
            if link:
                urls.append(link)
    print(f"[CSV] {len(urls)} Spielplan-URLs aus {csv_path} geladen.")
    return urls


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


def wait_for_schedule_table(driver: webdriver.Chrome, url: str = "", timeout: int = 30) -> None:
    tag = f" ({url.split('?')[-1]})" if url else ""
    print(f"[WAIT]{tag} Warte bis 'schedule_table' geladen ist (Timeout {timeout}s) \u2026")
    WebDriverWait(driver, timeout).until(
        EC.presence_of_element_located((By.CSS_SELECTOR, "div.schedule_table"))
    )
    print(f"[WAIT]{tag} 'schedule_table' gefunden.")


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
        if "/Spielplan/" in href or "/Bewerb/" in href:
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
            time.sleep(0.3 + random.random() * 0.2)
        except TimeoutException:
            print("[CLICK] Keine weiteren Buttons gefunden.")
            break
        except StaleElementReferenceException:
            time.sleep(0.5)
            continue


def scrape_multiple_urls_streaming(urls: Iterable[str], outfile: Path, headless: bool = True, already_seen: Optional[Set[str]] = None, max_workers: int = 4) -> None:
    csv_path = Path(outfile)
    ensure_csv_with_header(csv_path)
    global_seen: Set[str] = set(already_seen or set())

    local = threading.local()
    drivers: list = []
    drivers_lock = threading.Lock()

    def get_driver():
        if not hasattr(local, 'driver') or local.driver is None:
            d = make_driver(headless=headless)
            local.driver = d
            with drivers_lock:
                drivers.append(d)
        return local.driver

    def process_url(url: str):
        driver = get_driver()
        tag = url.split('?')[-1] if '?' in url else url.split('/')[-1]
        try:
            safe_get(driver, url, attempts=3)
            wait_for_schedule_table(driver, url=url, timeout=45)
            click_more_until_stable(driver)
            return url, collect_links_from_page(driver, source_url=url)
        except Exception as e:
            print(f"[FEHLER] {tag}: {e}", file=sys.stderr)
            return url, []

    try:
        lock_path = csv_path.with_suffix(csv_path.suffix + ".lock")
        with file_lock(lock_path):
            with open(csv_path, "a", newline="", encoding="utf-8") as fh:
                writer = csv.writer(fh)
                url_list = list(urls)
                with ThreadPoolExecutor(max_workers=max_workers) as ex:
                    futures = {ex.submit(process_url, url): url for url in url_list}
                    for fut in as_completed(futures):
                        try:
                            url, items = fut.result()
                        except Exception as e:
                            print(f"[FEHLER] {futures[fut]}: {e}", file=sys.stderr)
                            continue
                        new_count = 0
                        for _, href, src, ts in items:
                            if href not in global_seen:
                                global_seen.add(href)
                                writer.writerow((href, src, ts))
                                new_count += 1
                        if new_count:
                            fh.flush()
                        print(f"[DONE] {url}: {new_count} neue Links")
    finally:
        for d in drivers:
            with contextlib.suppress(Exception):
                d.quit()
        print(f"[EXIT] {len(drivers)} WebDriver beendet.")


# =========================
# CLI-Einstiegspunkt
# =========================
if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Sammelt Links aus mehreren ÖFB-Planungsseiten und schreibt sie in Echtzeit ins CSV.")
    parser.add_argument("--csv", default=DEFAULT_SPIELPLAN_CSV,
                        help="Pfad zur spielplan_urls.csv (Standard: results/spielplan_urls.csv)")
    parser.add_argument("--outfile", default=None)
    parser.add_argument("--out", default=None)
    parser.add_argument("--no-headless", action="store_true")
    parser.add_argument("--fresh", action="store_true")
    parser.add_argument("--workers", type=int, default=4, help="Anzahl paralleler Browser-Instanzen.")
    args = parser.parse_args()

    urls = load_urls_from_csv(args.csv)

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
        urls,
        outfile=out_path,
        headless=not args.no_headless,
        already_seen=existing_hrefs,
        max_workers=max(1, args.workers),
    )

    print("[MAIN] Fertig.")
