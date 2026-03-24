import os
import sys
import json
import time
import re
import asyncio
import argparse
from datetime import datetime, timezone
from typing import Optional, List, Dict

import pandas as pd
import aiohttp

# ============== Konstanten ==============

_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                   "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    "Accept-Language": "de-DE,de;q=0.9,en-US;q=0.8,en;q=0.7",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}

_PRELOADS_RE = re.compile(
    r"SG\.container\.appPreloads\['[^']+'\]\s*=\s*(\[.*?\])\s*;",
    re.DOTALL,
)


# ============== HTML → JSON-Extraktion ==============

def _extract_preloads(html: str) -> List[dict]:
    """Extrahiert alle appPreloads-JSON-Blöcke aus dem HTML."""
    objects: List[dict] = []
    for m in _PRELOADS_RE.finditer(html):
        try:
            arr = json.loads(m.group(1))
            if isinstance(arr, list):
                for obj in arr:
                    if isinstance(obj, dict):
                        objects.append(obj)
        except (json.JSONDecodeError, ValueError):
            pass
    return objects


def _find_game_data(preloads: List[dict]) -> Optional[dict]:
    """Findet den Block mit Spielinfo.

    Format A: hat 'spielUid' + 'heimMannschaft' (detaillierte Spielinfo).
    Format B: hat 'heimMannschaft' + 'start' ohne 'spielUid' (Besetzungs-Block).
    """
    # Bevorzuge Format A (enthält runde, datum, heimUrl)
    for obj in preloads:
        if "spielUid" in obj and "heimMannschaft" in obj:
            return obj
    # Fallback: Format B (enthält start, heimMannschaftLink, kein spielUid)
    for obj in preloads:
        if "heimMannschaft" in obj and "start" in obj and "bewerb" in obj:
            return obj
    return None


def _find_venue_data(preloads: List[dict]) -> Optional[dict]:
    """Findet den Block mit Spielort-Koordinaten (hat 'latitude'+'bezeichnung')."""
    for obj in preloads:
        lat = obj.get("latitude")
        lon = obj.get("longitude")
        if isinstance(lat, (int, float)) and isinstance(lon, (int, float)):
            if lat != 0 or lon != 0:
                return obj
    return None


def _find_bewerb(preloads: List[dict]) -> str:
    """Holt den Bewerb-String aus den Preloads."""
    for obj in preloads:
        b = obj.get("bewerb")
        if isinstance(b, str) and b.strip():
            return b.strip()
    return ""


def _epoch_ms_to_datetime(epoch_ms) -> Optional[str]:
    """Konvertiert einen Epoch-Millisekunden-Wert in 'YYYY-MM-DD HH:MM:SS' (Europe/Vienna)."""
    if not isinstance(epoch_ms, (int, float)):
        return None
    try:
        dt = datetime.fromtimestamp(epoch_ms / 1000, tz=timezone.utc)
        # oefb.at timestamps are CET/CEST; offset +1/+2
        # Use a fixed +1h offset as a simple fallback (matches the original scraper behaviour
        # which read local-time text from the page).
        from zoneinfo import ZoneInfo
        dt = dt.astimezone(ZoneInfo("Europe/Vienna"))
        return dt.strftime("%Y-%m-%d %H:%M:%S")
    except (OSError, ValueError):
        return None


# ============== Core scraping (HTTP-only) ==============

def _parse_game(html: str, weblink: str) -> Dict:
    """Parst Spieldaten aus dem HTML einer Spielseite."""
    preloads = _extract_preloads(html)
    game = _find_game_data(preloads)
    venue = _find_venue_data(preloads)
    liga = _find_bewerb(preloads)

    if not game:
        return {
            "Datum": None, "Liga": None, "Typ": None, "Runde": None,
            "Heim": None, "Gast": None, "Heim_Link": None, "Gast_Link": None,
            "Spielort_Name": None, "Adresse": None, "Latitude": None, "Longitude": None,
            "Quelle": weblink, "link": weblink.replace("/Spielbericht/", "/"),
            "error": "Keine Spielinfo in appPreloads gefunden",
        }

    # Format A: 'datum', Format B: 'start'
    datum_str = _epoch_ms_to_datetime(game.get("datum") or game.get("start"))

    spielort_name = ""
    adresse = ""
    lat, lon = None, None
    if venue:
        spielort_name = venue.get("bezeichnung", "")
        parts = [venue.get("strasseHausnummer", ""), venue.get("plzOrt", "")]
        anfahrt = venue.get("anfahrtPKW") or ""
        parts = [p for p in parts if p]
        if anfahrt:
            parts.append(anfahrt.replace("\r\n", ", ").replace("\n", ", "))
        adresse = ", ".join(parts)
        lat = venue.get("latitude")
        lon = venue.get("longitude")

    # Format A: heimUrl/gastUrl, Format B: heimMannschaftLink/gastMannschaftLink
    heim_link = game.get("heimUrl") or game.get("heimMannschaftLink", "")
    gast_link = game.get("gastUrl") or game.get("gastMannschaftLink", "")

    row = {
        "Datum": datum_str,
        "Liga": liga,
        "Typ": "Frau" if (isinstance(liga, str) and "Frau" in liga) else "Mann",
        "Runde": game.get("runde", ""),
        "Heim": game.get("heimMannschaft", ""),
        "Gast": game.get("gastMannschaft", ""),
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


async def _fetch_and_parse(
    session: aiohttp.ClientSession,
    url: str,
    semaphore: asyncio.Semaphore,
    retries: int = 4,
) -> Dict:
    """Lädt eine Spielseite per HTTP und parst die JSON-Daten.

    Enthält Retry-Logik: Wenn der CDN eine gekürzte Seite ohne Spieldaten
    liefert, wird der Request mit Backoff wiederholt.
    """
    for attempt in range(1, retries + 1):
        try:
            async with semaphore:
                async with session.get(url, timeout=aiohttp.ClientTimeout(total=20)) as resp:
                    if resp.status != 200:
                        raise aiohttp.ClientError(f"HTTP {resp.status}")
                    html = await resp.text()
                # Kleine Pause innerhalb des Semaphors, um Burst-Traffic zu vermeiden
                await asyncio.sleep(0.15)
            row = _parse_game(html, url)
            # Wenn Spieldaten fehlen (CDN hat gekürzte Seite geliefert) → retry
            if "error" in row and attempt < retries:
                await asyncio.sleep(2.0 * attempt)
                continue
            return row
        except Exception as e:
            if attempt == retries:
                return {
                    "Datum": None, "Liga": None, "Typ": None, "Runde": None,
                    "Heim": None, "Gast": None, "Heim_Link": None, "Gast_Link": None,
                    "Spielort_Name": None, "Adresse": None, "Latitude": None, "Longitude": None,
                    "Quelle": url, "link": url.replace("/Spielbericht/", "/"),
                    "error": f"HTTP-Fehler ({attempt}/{retries}): {e}",
                }
            await asyncio.sleep(1.5 * attempt)


# ============== Controller ==============

def normalize_link(u: str) -> str:
    return (u or "").replace("/Spielbericht/", "/")


def load_links(input_csv: str) -> List[str]:
    df = pd.read_csv(input_csv, encoding="utf-8-sig")
    if "href" not in df.columns:
        raise ValueError(f"Input CSV {input_csv} must have an 'href' column (got columns: {list(df.columns)})")
    links = [normalize_link(x) for x in df["href"].astype(str).tolist()]
    return links


async def _run_async(
    todo: List[str],
    results: List[dict],
    have: set,
    links_all: List[str],
    out_csv: str,
    max_workers: int,
    flush_every: int,
) -> None:
    semaphore = asyncio.Semaphore(max_workers)
    connector = aiohttp.TCPConnector(limit=max_workers, limit_per_host=max_workers)
    completed_since_flush = 0

    def write_out():
        pd.DataFrame(results).to_csv(out_csv, index=False, encoding="utf-8-sig", sep=";")
        print(f"Zwischenspeicher: {len(results)} Einträge -> {out_csv}", flush=True)

    async with aiohttp.ClientSession(headers=_HEADERS, connector=connector) as session:
        tasks = [_fetch_and_parse(session, url, semaphore) for url in todo]
        for coro in asyncio.as_completed(tasks):
            row = await coro
            link = row.get("link", "")
            results.append(row)
            have.add(link)
            completed_since_flush += 1
            print(f"Fertig: {link}  ({len(have)}/{len(links_all)})", flush=True)
            if completed_since_flush >= flush_every:
                write_out()
                completed_since_flush = 0

    write_out()


def run_parallel(
    input_csv: str,
    out_csv: str,
    max_workers: int = 10,
    flush_every: int = 50,
    headless: bool = True,  # kept for CLI compat, unused now
) -> None:

    # 1) Links laden
    links_all = [l for l in load_links(input_csv) if "/Spielplan/" not in l]

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
        if not os.path.exists(out_csv):
            pd.DataFrame(results).to_csv(out_csv, index=False, encoding="utf-8-sig", sep=";")
        return

    print(f"Starte async: {len(todo)} Seiten, max_workers={max_workers}", flush=True)
    asyncio.run(_run_async(todo, results, have, links_all, out_csv, max_workers, flush_every))
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
    parser.add_argument("--workers", type=int, default=10, help="Maximale Parallelität (async HTTP-Requests).")
    parser.add_argument("--flush-every", type=int, default=50, help="Nach wie vielen Ergebnissen Zwischen-Speichern.")
    parser.add_argument("--no-headless", action="store_true", help="Ignoriert (kein Browser mehr nötig).")

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
