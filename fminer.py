"""fminer – Sammelt Spiel-Links aus ÖFB-Spielplan-Seiten via REST-API.

Liest Spielplan-URLs aus results/spielplan_urls.csv, holt pro Bewerb
alle Runden über die REST-API und extrahiert die Spiel-/Spielbericht-Links
nach results/fminer/oefb_links_gesamt.csv.

Benötigt: aiohttp, pandas (optional für Analyse).
"""

import asyncio
import csv
import json
import os
import re
import sys
import time
import urllib.parse
from pathlib import Path
from typing import List, Set, Tuple

import aiohttp

# =========================
# Konfiguration
# =========================
DEFAULT_OUTFILE = "oefb_links_gesamt.csv"
DEFAULT_SPIELPLAN_CSV = os.path.join(os.path.dirname(__file__), "results", "spielplan_urls.csv")

_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/131.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "de-DE,de;q=0.9",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}

_API_ACCEPT_HEADERS = {
    "Accept": "application/json, text/plain, */*",
}

_MAX_WORKERS = 10          # parallele API-Aufrufe
_DELAY_BETWEEN = 0.12      # Sekunden zwischen API-Anfragen
_API_TIMEOUT = 30           # Sekunden Timeout pro Request
_MAX_RETRIES = 3            # Wiederholungsversuche

# =========================
# CSV-Helfer
# =========================

def load_urls_from_csv(csv_path: str) -> List[dict]:
    """Liest Spielplan-URLs aus der von mine_spielplan_urls.py erzeugten CSV."""
    rows: List[dict] = []
    with open(csv_path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f, delimiter=";")
        for row in reader:
            link = (row.get("link") or "").strip()
            if link:
                rows.append(row)
    print(f"[CSV] {len(rows)} Spielplan-URLs aus {csv_path} geladen.")
    return rows


def load_existing_hrefs(csv_path: Path) -> Set[str]:
    existing: Set[str] = set()
    if not csv_path.exists():
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
    return existing


def ensure_csv_with_header(csv_path: Path) -> None:
    csv_path.parent.mkdir(parents=True, exist_ok=True)
    if not csv_path.exists() or csv_path.stat().st_size == 0:
        with open(csv_path, "w", newline="", encoding="utf-8") as f:
            csv.writer(f).writerow(["href", "source_url", "first_seen_utc"])


# =========================
# API-Helfer
# =========================

def _extract_bewerb_id(url: str) -> str | None:
    """Extrahiert die Bewerb-ID aus einer wfv.at-URL."""
    m = re.search(r"/Bewerb/(?:Spielplan/)?(\d+)", url)
    return m.group(1) if m else None


async def _get_page_config(session: aiohttp.ClientSession, page_url: str) -> Tuple[str, str, str]:
    """Holt Proxy-Pfad und Project-OID aus einer Spielplan-Seite."""
    spielplan_url = page_url.replace("/Bewerb/", "/Bewerb/Spielplan/", 1) if "/Spielplan/" not in page_url else page_url
    async with session.get(spielplan_url, timeout=aiohttp.ClientTimeout(total=_API_TIMEOUT)) as resp:
        html = await resp.text()

    parsed = urllib.parse.urlparse(spielplan_url)
    base_url = f"{parsed.scheme}://{parsed.netloc}"

    proxy_match = re.search(r'"path"\s*:\s*"(/proxy/[^"]+)"', html)
    proxy_path = proxy_match.group(1) if proxy_match else "/proxy/wfv3"

    project_match = re.search(r'"oid"\s*:\s*"(\d+)"', html)
    project_oid = project_match.group(1) if project_match else ""

    return base_url, proxy_path, project_oid


def _build_api_url(base_url: str, proxy_path: str, project_oid: str, bewerb_id: str, runde: int) -> str:
    internal = (
        f"http://portale-datenservice:8080/datenservice/rest/oefb/spielbetrieb/"
        f"spielplanBewerbByPublicUid/{bewerb_id};runde={runde}"
    )
    cache_key = f"spielbetrieb_spielplanBewerbByPublicUid_{bewerb_id}_{runde}"
    return f"{base_url}{proxy_path}/{project_oid}_{cache_key}?proxyUrl={urllib.parse.quote(internal, safe='')}"


def _extract_links_from_entries(entries: list) -> List[str]:
    """Extrahiert oefb.at-Spiellinks aus spiele/ergebnisse-Einträgen."""
    links: list[str] = []
    for entry in entries:
        for link_obj in entry.get("links", []):
            link = link_obj.get("link", "")
            if link and "/bewerbe/" in link.lower():
                # /Spielbericht/ => / normalisieren
                link = link.replace("/Spielbericht/", "/")
                links.append(link)
    return links


async def _fetch_bewerb_links(
    session: aiohttp.ClientSession,
    base_url: str,
    proxy_path: str,
    project_oid: str,
    bewerb_id: str,
    sem: asyncio.Semaphore,
) -> List[str]:
    """Holt alle Spiel-Links für einen Bewerb über alle Runden."""
    all_links: list[str] = []
    timeout = aiohttp.ClientTimeout(total=_API_TIMEOUT)

    async def fetch_json(url: str) -> dict | None:
        for attempt in range(1, _MAX_RETRIES + 1):
            try:
                async with sem:
                    async with session.get(url, timeout=timeout, headers=_API_ACCEPT_HEADERS) as resp:
                        return await resp.json(content_type=None)
            except Exception as e:
                if attempt == _MAX_RETRIES:
                    print(f"[WARN] API-Fehler nach {_MAX_RETRIES} Versuchen für {bewerb_id}: {e}", file=sys.stderr)
                    return None
                await asyncio.sleep(1.0 * attempt)

    # Runde 0 = aktuelle Woche, gleichzeitig Runden-Liste holen
    url0 = _build_api_url(base_url, proxy_path, project_oid, bewerb_id, 0)
    data = await fetch_json(url0)
    if not data:
        return []

    runden = data.get("runden", [])
    all_links.extend(_extract_links_from_entries(data.get("spiele", [])))
    all_links.extend(_extract_links_from_entries(data.get("ergebnisse", [])))

    # Verbleibende Runden sequenziell abrufen (schont den Server)
    for runde_info in runden:
        runde_nr = runde_info.get("runde", 0)
        url = _build_api_url(base_url, proxy_path, project_oid, bewerb_id, runde_nr)
        rdata = await fetch_json(url)
        if rdata:
            all_links.extend(_extract_links_from_entries(rdata.get("spiele", [])))
            all_links.extend(_extract_links_from_entries(rdata.get("ergebnisse", [])))
        await asyncio.sleep(_DELAY_BETWEEN)

    # Deduplizieren, Reihenfolge beibehalten
    return list(dict.fromkeys(all_links))


# =========================
# Hauptlogik
# =========================

async def scrape_all(
    urls: List[dict],
    outfile: Path,
    already_seen: Set[str] | None = None,
    max_workers: int = _MAX_WORKERS,
) -> None:
    ensure_csv_with_header(outfile)
    global_seen: Set[str] = set(already_seen or set())
    sem = asyncio.Semaphore(max_workers)

    conn = aiohttp.TCPConnector(limit=max_workers + 5)
    async with aiohttp.ClientSession(headers=_HEADERS, connector=conn) as session:
        # Proxy-Konfiguration von erster Seite holen
        first_link = urls[0]["link"].strip()
        base_url, proxy_path, project_oid = await _get_page_config(session, first_link)
        print(f"[CONFIG] Base={base_url}  Proxy={proxy_path}  OID={project_oid}")

        if not project_oid:
            print("[FEHLER] Keine Project-OID gefunden – Abbruch.", file=sys.stderr)
            return

        now_iso = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())

        # Alle Bewerbe parallel abrufen (Semaphore begrenzt Gleichzeitigkeit)
        async def fetch_one(row: dict) -> Tuple[str, List[str]]:
            link = row["link"].strip()
            bewerb_id = _extract_bewerb_id(link)
            if not bewerb_id:
                return link, []
            return link, await _fetch_bewerb_links(
                session, base_url, proxy_path, project_oid, bewerb_id, sem
            )

        tasks = [fetch_one(row) for row in urls]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        total_new = 0
        with open(outfile, "a", newline="", encoding="utf-8") as fh:
            writer = csv.writer(fh)

            for i, result in enumerate(results, 1):
                if isinstance(result, Exception):
                    tag = urls[i - 1]["link"].split("?")[-1]
                    print(f"[FEHLER] {tag}: {result}", file=sys.stderr)
                    continue

                link, links = result
                tag = link.split("?")[-1] if "?" in link else link

                new_count = 0
                for href in links:
                    if href not in global_seen:
                        global_seen.add(href)
                        writer.writerow((href, link, now_iso))
                        new_count += 1

                total_new += new_count
                print(f"[{i}/{len(urls)}] {tag}: {len(links)} Links ({new_count} neu)")

            fh.flush()

    print(f"[FERTIG] {total_new} neue Links gesamt → {outfile}")


# =========================
# CLI
# =========================
if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="Sammelt Spiel-Links aus ÖFB-Spielplänen via REST-API."
    )
    parser.add_argument(
        "--csv", default=DEFAULT_SPIELPLAN_CSV,
        help="Pfad zur spielplan_urls.csv (Standard: results/spielplan_urls.csv)",
    )
    parser.add_argument("--outfile", default=None)
    parser.add_argument("--out", default="results/fminer")
    parser.add_argument(
        "--workers", type=int, default=_MAX_WORKERS,
        help=f"Max. parallele API-Anfragen (Standard: {_MAX_WORKERS}).",
    )
    args = parser.parse_args()

    urls = load_urls_from_csv(args.csv)
    if not urls:
        print("[FEHLER] Keine URLs gefunden – Abbruch.", file=sys.stderr)
        sys.exit(1)

    if args.outfile:
        out_path = Path(args.outfile)
    else:
        out_dir = Path(args.out or ".")
        out_dir.mkdir(parents=True, exist_ok=True)
        out_path = out_dir / DEFAULT_OUTFILE

    existing_hrefs = load_existing_hrefs(out_path)
    print(f"[INFO] Resume: {len(existing_hrefs)} vorhandene Links werden übersprungen.")

    asyncio.run(scrape_all(urls, out_path, already_seen=existing_hrefs, max_workers=args.workers))
