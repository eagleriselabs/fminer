import argparse
import os
import re
import sys
from typing import Optional

import pandas as pd


def extract_address_parts(addr: Optional[str]) -> pd.Series:
    """Extract Straße, PLZ, Ort from a free-form address string."""
    if addr is None or (isinstance(addr, float) and pd.isna(addr)):
        return pd.Series({"Straße": None, "PLZ": None, "Ort": None})
    if not isinstance(addr, str):
        addr = str(addr)

    parts = [p.strip() for p in addr.split(",") if p.strip()]
    street = parts[0] if parts else None

    # prefer the second part (often "PLZ Ort"); fallback to full text (minus street)
    plz_ort_source = parts[1] if len(parts) > 1 else (addr.replace(street, "", 1) if street else addr)

    # 4–5 digit PLZ + city (no comma)
    pat = r"\b(\d{4,5})\s+([A-Za-zÄÖÜäöüß\.\- ]+)\b"

    m = re.search(pat, plz_ort_source)
    if m:
        plz = m.group(1).strip()
        ort = m.group(2).strip()
    else:
        m2 = re.search(pat, addr)
        if m2:
            plz = m2.group(1).strip()
            ort = m2.group(2).strip()
        else:
            plz, ort = None, None

    return pd.Series({"Straße": street, "PLZ": plz, "Ort": ort})


def ensure_columns(df: pd.DataFrame, cols):
    for c in cols:
        if c not in df.columns:
            df[c] = pd.NA
    return df


def main():
    p = argparse.ArgumentParser(description="Postprocessing für Spiel-Infos (nur Wien).")
    p.add_argument("--in", dest="in_csv", default="results/game_miner/spiel_infos.csv",
                   help="Eingabe-CSV von game_miner_parallel.py (default: results/game_miner/spiel_infos.csv)")
    p.add_argument("--out", dest="out_dir", default="results/post_processing",
                   help="Ausgabe-Ordner (default: results/post_processing)")
    p.add_argument("--outfile", dest="out_file", default=None,
                   help="Optional: expliziter Pfad für martiballtermine_wien.csv")
    p.add_argument("--fails", dest="fails_file", default=None,
                   help="Optional: expliziter Pfad für fails.csv")
    args = p.parse_args()

    in_csv = args.in_csv
    if not os.path.exists(in_csv):
        raise FileNotFoundError(f"Input CSV not found: {in_csv}")

    out_dir = args.out_dir or "."
    os.makedirs(out_dir, exist_ok=True)

    # defaults for outputs
    output_path = args.out_file or os.path.join(out_dir, "martiballtermine_wien.csv")
    fails_path = args.fails_file or os.path.join(out_dir, "fails.csv")

    # --- Load ---
    df = pd.read_csv(in_csv, encoding="utf-8-sig", sep=";")

    # --- Normalize fields used later ---
    # Map Bundesliga label + gender
    if "Liga" in df.columns:
        df["Liga"] = df["Liga"].replace({"Österreichische Fußball-Bundesliga": "ADMIRAL Bundesliga"})
    if "Typ" in df.columns:
        df["Typ"] = df["Typ"].replace({"Mann": "Männer", "Frau": "Frauen"})

    # --- Address parsing ---
    if "Adresse" not in df.columns:
        df["Adresse"] = pd.NA
    address_parts = df["Adresse"].apply(extract_address_parts)
    df = pd.concat([df, address_parts], axis=1)

    # --- Select only Vienna (Ort contains 'wien') ---
    df["Ort"] = df["Ort"].astype("string")
    df_wien = df[df["Ort"].str.contains("wien", case=False, na=False)].copy()

    # --- Ensure required columns exist and order them ---
    wanted_cols = [
        "Datum", "Liga", "Typ", "Runde", "Heim", "Gast",
        "Spielort_Name", "Straße", "PLZ", "Ort",
        "Latitude", "Longitude", "Quelle"
    ]
    df_wien = ensure_columns(df_wien, wanted_cols)
    df_out = df_wien[wanted_cols].copy()

    # --- Datetime, sort ---
    df_out["Datum"] = pd.to_datetime(df_out["Datum"], errors="coerce")
    df_out = df_out.sort_values("Datum")

    # --- Korrekturen ---
    mask_gersthof = df_out["Heim"] == "Gersthofer SV"
    if mask_gersthof.any():
        df_out.loc[mask_gersthof, ["Latitude", "Longitude"]] = [48.225324870552456, 16.328420452719126]

    # --- Fails (missing coords) ---
    df_nan = df_out[df_out["Latitude"].isna() & df_out["Longitude"].isna()].copy()
    # keep fails CSV even if empty for debugging consistency
    df_nan.to_csv(fails_path, sep=";", index=False, encoding="utf-8-sig")

    # --- Save main output ---
    df_out.to_csv(output_path, sep=";", index=False, encoding="utf-8-sig")

    # --- Logging summary ---
    print(f"[post] input: {in_csv}")
    print(f"[post] rows in: {len(df)}")
    print(f"[post] rows in Vienna: {len(df_out)}")
    print(f"[post] saved: {output_path}")
    print(f"[post] fails (no coords): {len(df_nan)} -> {fails_path}")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"[post][ERROR] {e}", file=sys.stderr)
        raise
