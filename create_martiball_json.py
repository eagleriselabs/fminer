#!/usr/bin/env python3
import argparse
import json
import math
import os
from typing import Any, Dict, List, Optional

import pandas as pd


def to_float_or_none(v) -> Optional[float]:
    try:
        x = float(v)
        if math.isnan(x):
            return None
        return x
    except Exception:
        return None


def to_int_or_none(v) -> Optional[int]:
    try:
        if pd.isna(v):
            return None
        return int(str(v).strip())
    except Exception:
        return None


def build_record(row: pd.Series) -> Dict[str, Any]:
    # Datum -> ISO 8601 ohne Zeitzone, z.B. "2025-08-01T18:00:00"
    dt = pd.to_datetime(row.get("Datum"), errors="coerce")
    spieldatum = dt.strftime("%Y-%m-%dT%H:%M:%S") if not pd.isna(dt) else None

    rec = {
        "Spieldatum": spieldatum,
        "Heimmannschaft": (row.get("Heim") or "") if isinstance(row.get("Heim"), str) else str(row.get("Heim") or ""),
        "Gastmannschaft": (row.get("Gast") or "") if isinstance(row.get("Gast"), str) else str(row.get("Gast") or ""),
        "Typ": (row.get("Typ") or "") if isinstance(row.get("Typ"), str) else str(row.get("Typ") or ""),
        "Liga": (row.get("Liga") or "") if isinstance(row.get("Liga"), str) else str(row.get("Liga") or ""),
        "Spielort": (row.get("Spielort_Name") or "") if isinstance(row.get("Spielort_Name"), str) else str(row.get("Spielort_Name") or ""),
        "Spielort Straße": (row.get("Straße") or "") if isinstance(row.get("Straße"), str) else str(row.get("Straße") or ""),
        "Spielort PLZ": to_int_or_none(row.get("PLZ")),
        "Spielort Ort": (row.get("Ort") or "") if isinstance(row.get("Ort"), str) else str(row.get("Ort") or ""),
        "Latitude": to_float_or_none(row.get("Latitude")),
        "Longitude": to_float_or_none(row.get("Longitude")),
    }
    return rec


def main():
    ap = argparse.ArgumentParser(description="Erzeuge martiball_spiele.json aus martiballtermine_wien.csv")
    ap.add_argument("--in", dest="in_csv", default="results/martiballtermine_wien.csv",
                    help="Eingabe-CSV (default: results/martiballtermine_wien.csv)")
    ap.add_argument("--out", dest="out_json", default="results/martiball_spiele.json",
                    help="Ausgabe-JSON (default: results/martiball_spiele.json)")
    args = ap.parse_args()

    in_path = args.in_csv
    out_path = args.out_json

    if not os.path.exists(in_path):
        raise FileNotFoundError(f"Eingabe nicht gefunden: {in_path}")

    # CSV laden (Semikolon, BOM-safe)
    df = pd.read_csv(in_path, sep=";", encoding="utf-8-sig")

    # Stabil sortieren: nach Datum (aufsteigend), dann Liga, Heim
    if "Datum" in df.columns:
        df["Datum"] = pd.to_datetime(df["Datum"], errors="coerce")
        df = df.sort_values(["Datum", "Liga", "Heim"], na_position="last").reset_index(drop=True)

    # Records bauen
    records: List[Dict[str, Any]] = [build_record(row) for _, row in df.iterrows()]

    payload = {"martiballtermine_wien": records}

    # Schreiben (schön formatiert, Umlaute erlauben)
    os.makedirs(os.path.dirname(out_path) or ".", exist_ok=True)
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)

    print(f"[json] input:  {in_path}")
    print(f"[json] output: {out_path}")
    print(f"[json] rows:   {len(records)}")


if __name__ == "__main__":
    main()
