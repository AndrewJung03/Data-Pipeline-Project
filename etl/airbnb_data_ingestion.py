import os
import time
from datetime import datetime

import pandas as pd

# ============================================================
# PATHS
# ============================================================
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, "..", "data")

INPUT_FILE = os.path.join(DATA_DIR, "listings.csv")
OUTPUT_FILE = os.path.join(DATA_DIR, "listings_clean.csv")
REJECT_FILE = os.path.join(DATA_DIR, "listings_rejects.csv")


# ============================================================
# REQUIRED COLUMNS
# ============================================================
required_columns = [
    "id",
    "name",
    "host_id",
    "host_name",
    "neighbourhood_group",
    "neighbourhood",
    "latitude",
    "longitude",
    "room_type",
    "price",
    "minimum_nights",
    "number_of_reviews",
    "last_review",
    "reviews_per_month",
    "calculated_host_listings_count",
    "availability_365",
    "number_of_reviews_ltm",
    "license",
]


# ============================================================
# 1. READER LAYER
# ============================================================
def read_data(csv_path=INPUT_FILE):
    if not os.path.exists(csv_path):
        raise FileNotFoundError(f"Input file not found: {csv_path}")

    df = pd.read_csv(csv_path)
    df.columns = df.columns.str.strip()
    return df


# ============================================================
# 2. VALIDATOR LAYER
# ============================================================
def validate_columns(df):
    missing = [c for c in required_columns if c not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns: {missing}")
    return df


# ============================================================
# 3. COLUMN CLEANING FUNCTIONS (ONE PER COLUMN)
# ============================================================


# ---- ID Fields ----
def clean_listing_id(col):
    return pd.to_numeric(col, errors="coerce").astype("Int64")


def clean_host_id(col):
    return pd.to_numeric(col, errors="coerce").astype("Int64")


# ---- Strings ----
def clean_name(col):
    return col.fillna("Unknown").astype(str).str.strip()


def clean_host_name(col):
    return col.fillna("Unknown").astype(str).str.strip()


def clean_room_type(col):
    return col.fillna("Unknown").astype(str).str.strip()


def clean_neighbourhood_group(col):
    return col.fillna("Unknown").astype(str).str.strip()


def clean_neighbourhood(col):
    return col.fillna("Unknown").astype(str).str.strip()


def clean_license(col):
    return col.fillna("Not available").astype(str).str.strip()


# ---- Coordinates ----
def clean_latitude(col):
    return pd.to_numeric(col, errors="coerce")


def clean_longitude(col):
    return pd.to_numeric(col, errors="coerce")


# ---- Numeric Monies ----
def clean_price(col):
    cleaned = (
        col.astype(str)
        .str.replace(r"[$,]", "", regex=True)
        .str.replace(r"[^0-9.\-]", "", regex=True)
    )
    return pd.to_numeric(cleaned, errors="coerce")


def clean_minimum_nights(col):
    return pd.to_numeric(col, errors="coerce")


def clean_num_reviews(col):
    return pd.to_numeric(col, errors="coerce")


def clean_reviews_per_month(col):
    return pd.to_numeric(col, errors="coerce")


def clean_calculated_host_count(col):
    return pd.to_numeric(col, errors="coerce")


def clean_availability_365(col):
    return pd.to_numeric(col, errors="coerce")


def clean_num_reviews_ltm(col):
    return pd.to_numeric(col, errors="coerce")


# ---- Dates ----
def clean_last_review(col):
    return pd.to_datetime(col, errors="coerce")


# ============================================================
# 4. CLEANER LAYER (APPLIES ALL COLUMN CLEANERS)
# ============================================================
def clean_data(df):
    cleaners = {
        "id": clean_listing_id,
        "name": clean_name,
        "host_id": clean_host_id,
        "host_name": clean_host_name,
        "neighbourhood_group": clean_neighbourhood_group,
        "neighbourhood": clean_neighbourhood,
        "latitude": clean_latitude,
        "longitude": clean_longitude,
        "room_type": clean_room_type,
        "price": clean_price,
        "minimum_nights": clean_minimum_nights,
        "number_of_reviews": clean_num_reviews,
        "last_review": clean_last_review,
        "reviews_per_month": clean_reviews_per_month,
        "calculated_host_listings_count": clean_calculated_host_count,
        "availability_365": clean_availability_365,
        "number_of_reviews_ltm": clean_num_reviews_ltm,
        "license": clean_license,
    }

    # Apply each column cleaner
    for col, cleaner_fn in cleaners.items():
        df[col] = cleaner_fn(df[col])

    return df


# ============================================================
# 5. HANDLE REJECTIONS (invalid price, invalid IDs)
# ============================================================
def reject_bad_rows(df):
    rejects = []

    # Invalid price rows → REJECT ENTIRE ROW
    bad_price_mask = df["price"].isna()

    for idx in df[bad_price_mask].index:
        row = df.loc[idx].to_dict()
        row["reject_reason"] = "Invalid price"
        rejects.append(row)

    df_clean = df[~bad_price_mask].copy()
    rejects_df = pd.DataFrame(rejects)

    return df_clean, rejects_df


# ============================================================
# 6. WRITE OUTPUT FILES
# ============================================================
def write_output(df_clean, df_rejects):
    os.makedirs(DATA_DIR, exist_ok=True)

    df_clean.to_csv(OUTPUT_FILE, index=False)
    df_rejects.to_csv(REJECT_FILE, index=False)


# ============================================================
# 7. INGESTION PIPELINE WRAPPER
# ============================================================
def ingest_listings():
    print("Starting Airbnb listings ingestion…")
    start = time.time()

    df = read_data()
    validate_columns(df)
    df = clean_data(df)

    df_clean, df_rejects = reject_bad_rows(df)
    write_output(df_clean, df_rejects)

    end = time.time()

    print("\n========== AIRBNB INGESTION REPORT ==========")
    print(f"Raw rows:               {len(df)}")
    print(f"Rows after cleaning:    {len(df_clean)}")
    print(f"Rows rejected:          {len(df_rejects)}")
    print(f"Output saved to:        {OUTPUT_FILE}")
    print(f"Rejects saved to:       {REJECT_FILE}")
    print(f"Time taken:             {round(end - start, 2)} seconds")
    print("==============================================\n")

    return df_clean, df_rejects


# ============================================================
# Run if executed directly
# ============================================================
if __name__ == "__main__":
    ingest_listings()
