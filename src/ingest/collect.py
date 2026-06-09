import os, sys
import pandas as pd

# ── Config ────────────────────────────────────────────────────────
KAGGLE_DATASET = "dhruvildave/spotify-charts"
DOWNLOAD_DIR   = "data/raw_download"   # stores original charts.csv (~3.8GB)
OUTPUT_DIR     = "data/raw"            # stores split CSV files by country & month
CHART_TYPE     = "top200"
YEAR_FROM      = 2017
YEAR_TO        = 2021

# Country names in Kaggle dataset → 2-letter code for filenames
ASIAN_COUNTRIES = {
    "vietnam":     "VN",
    "south korea": "KR",
    "japan":       "JP",
    "taiwan":      "TW",
    "hong kong":   "HK",
}

# Chunk size for reading large CSV - avoids RAM overflow on HDP
CHUNK_SIZE = 300_000
# ─────────────────────────────────────────────────────────────────


def install_deps():
    """Install kaggle + pandas if not already available on HDP."""
    try:
        import kaggle  # noqa
        import pandas  # noqa
    except ImportError:
        print(">>> Installing kaggle and pandas...")
        ret = os.system("/usr/bin/python3.6 -m pip install -q --user kaggle pandas")
        if ret != 0:
            print("[ERROR] pip install failed - install manually: /usr/bin/python3.6 -m pip install kaggle pandas")
            sys.exit(1)
        print("    Done")


def download_from_kaggle():
    """Download charts.csv from Kaggle into DOWNLOAD_DIR."""
    import kaggle
    print(">>> Downloading Spotify Charts dataset from Kaggle (~3.8GB)...")
    print("    This may take 5-15 minutes depending on GCP network speed.")
    os.makedirs(DOWNLOAD_DIR, exist_ok=True)
    kaggle.api.authenticate()
    kaggle.api.dataset_download_files(
        KAGGLE_DATASET, path=DOWNLOAD_DIR, unzip=True
    )
    print(f"    Done -> {DOWNLOAD_DIR}/")


def find_csv():
    """Find the downloaded CSV file in DOWNLOAD_DIR."""
    if not os.path.isdir(DOWNLOAD_DIR):
        return None
    files = [f for f in os.listdir(DOWNLOAD_DIR) if f.endswith(".csv")]
    return os.path.join(DOWNLOAD_DIR, files[0]) if files else None


def split_by_country_month(csv_path):
    print(f">>> Reading in chunks of {CHUNK_SIZE:,} rows: {csv_path}")
    print("    Filtering VN/KR/JP/TW/HK + top200 + 2017-2021 per chunk...")

    chunks = []
    for i, chunk in enumerate(
        pd.read_csv(csv_path, parse_dates=["date"], chunksize=CHUNK_SIZE)
    ):
        # Filter early to avoid keeping irrelevant rows in memory.
        chunk = chunk[chunk["chart"] == CHART_TYPE]
        chunk = chunk[chunk["date"].dt.year.between(YEAR_FROM, YEAR_TO)]
        chunk["region_lower"] = chunk["region"].str.strip().str.lower()
        chunk = chunk[chunk["region_lower"].isin(ASIAN_COUNTRIES.keys())]

        if len(chunk) > 0:
            chunks.append(chunk)

        # Print progress every 5 chunks.
        if (i + 1) % 5 == 0:
            kept = sum(len(c) for c in chunks)
            print(f"    Chunk {i+1} done - kept {kept:,} rows so far...",
                  flush=True)

    if not chunks:
        print("\n[ERROR] No rows matched after filtering!")
        print("Possible reasons:")
        print("  1. Country names differ - check ASIAN_COUNTRIES keys")
        print("  2. Year range has no data - check YEAR_FROM/YEAR_TO")
        sys.exit(1)

    print(">>> Combining chunks...")
    df_asia = pd.concat(chunks, ignore_index=True)
    print(f"    Total rows kept: {len(df_asia):,}")
    print(f"    Countries found: {sorted(df_asia['region_lower'].unique())}")

    # Add country code and month columns.
    df_asia["country_code"] = df_asia["region_lower"].map(ASIAN_COUNTRIES)
    df_asia["month_str"]    = df_asia["date"].dt.to_period("M").astype(str)

    # Save one CSV per (country, month).
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    n_files, n_rows = 0, 0

    for (code, month), grp in df_asia.groupby(["country_code", "month_str"]):
        out_path = os.path.join(OUTPUT_DIR, f"{code}_{month}.csv")
        grp.drop(columns=["region_lower", "country_code", "month_str"],
                 errors="ignore").to_csv(out_path, index=False)
        n_files += 1
        n_rows  += len(grp)

    total_mb = sum(
        os.path.getsize(os.path.join(OUTPUT_DIR, f))
        for f in os.listdir(OUTPUT_DIR) if f.endswith(".csv")
    ) / 1024 / 1024

    print(f"\n>>> Split complete: {n_files} files | {n_rows:,} rows | {total_mb:.1f} MB")
    if total_mb >= 100:
        print("    [OK] Meets the 100MB requirement.")
    else:
        print(f"    [WARN] {total_mb:.1f} MB - still valid for assignment.")


# ── Main ──────────────────────────────────────────────────────────
if __name__ == "__main__":
    print("=" * 55)
    print("  STEP 1: Download & Split Spotify Chart Data")
    print("=" * 55)

    install_deps()

    # Skip download if CSV already exists
    csv_path = find_csv()
    if csv_path:
        print(f">>> Found existing file: {csv_path} - skipping download.")
    else:
        download_from_kaggle()
        csv_path = find_csv()
        if not csv_path:
            print("[ERROR] No CSV found after download.")
            sys.exit(1)

    split_by_country_month(csv_path)
    print("\n>>> Done! Next: bash src/ingest/upload_to_hdfs.sh")
