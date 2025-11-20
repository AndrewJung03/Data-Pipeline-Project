import os
import time
from datetime import datetime

import pandas as pd

# Paths
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, "..", "data")

INPUT_FILE = os.path.join(DATA_DIR, "listings.csv")
OUTPUT_FILE = os.path.join(DATA_DIR, "listings_clean.csv")
REJECT_FILE = os.path.join(DATA_DIR, "listings_rejects.csv")

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


def read_data(csv_path=INPUT_FILE):
    """
    Read the data in the CSV
    """
    if not os.path.exists(csv_path):
        raise FileNotFoundError(f"Input file not found: {csv_path}")

    df = pd.read_csv(csv_path)
    df.columns = df.columns.str.strip()
    return df


def validate_columns(df):
    """
    Makes sure that all the columns exist
    """
    missing = [c for c in required_columns if c not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns: {missing}")
    return df


# Cleaming each column

# INTS


def clean_listing_id(col):
    """
    Clean the 'id' column for listings.

    This function attempts to convert all values in the column to numeric
    listing IDs. Non-numeric or invalid values (e.g., strings, symbols,
    empty values) are coerced into <NA>.
    """
    return pd.to_numeric(col, errors="coerce").astype("Int64")


def clean_host_id(col):
    """
    Clean the 'host_id' column for listings.

    This function attempts to convert all values in the column to numeric
    listing IDs. Non-numeric or invalid values (e.g., strings, symbols,
    empty values) are coerced into <NA>.
    """
    return pd.to_numeric(col, errors="coerce").astype("Int64")


# STRINGS
def clean_name(col):
    """
    Clean the 'name' column for listings.

    This function fills missing values with the placeholder string
    "Unknown", converts all values to strings, and strips leading
    and trailing whitespace. It does not perform any validation
    beyond basic normalization.

    """
    return col.fillna("Unknown").astype(str).str.strip()


def clean_host_name(col):
    """
    Clean the 'host_name' column for listings.

    This function fills missing values with the placeholder string
    "Unknown", converts all values to strings, and strips leading
    and trailing whitespace. It does not perform any validation
    beyond basic normalization.

    """
    return col.fillna("Unknown").astype(str).str.strip()


def clean_room_type(col):
    """
    Clean the 'room_type' column for listings.

    This function fills missing values with the placeholder string
    "Unknown", converts all values to strings, and strips leading
    and trailing whitespace. It does not perform any validation
    beyond basic normalization.

    """
    return col.fillna("Unknown").astype(str).str.strip()


def clean_neighbourhood_group(col):
    """
    Clean the 'neighbourhood_group' column for listings.

    This function fills missing values with the placeholder string
    "Unknown", converts all values to strings, and strips leading
    and trailing whitespace. It does not perform any validation
    beyond basic normalization.

    """
    return col.fillna("Unknown").astype(str).str.strip()


def clean_neighbourhood(col):
    """
    Clean the 'neighbourhood' column for listings.

    This function fills missing values with the placeholder string
    "Unknown", converts all values to strings, and strips leading
    and trailing whitespace. It does not perform any validation
    beyond basic normalization.

    """
    return col.fillna("Unknown").astype(str).str.strip()


def clean_license(col):
    """
    Clean the 'license' column for listings.

    This function replaces missing license values with the placeholder
    string "Not available", converts all entries to strings, and strips
    leading and trailing whitespace. Since license information may be
    legitimately missing or inconsistently formatted, this function does
    not attempt to validate the content of the license beyond ensuring
    it is a clean, string-typed field.
    """
    return col.fillna("Not available").astype(str).str.strip()


# COORDS
def clean_latitude(col):
    """
    Clean the 'latitude' column for listings.

    This function attempts to convert all values in the latitude column
    into numeric form. Any non-numeric, malformed, or missing values are
    coerced into NaN. No additional validation (e.g., range checking for
    valid latitude coordinates) is performed here, keeping this function
    focused solely on numeric conversion.
    """

    return pd.to_numeric(col, errors="coerce")


def clean_longitude(col):
    """
    Clean the 'longitude' column for listings.

    This function converts all values in the longitude column into
    numeric form, coercing invalid or unparseable values into NaN using
    `errors="coerce"`. Geographic validation (e.g., ensuring longitude is
    between -180 and 180) is intentionally not performed here; the goal
    is strictly to normalize the data type.
    """
    return pd.to_numeric(col, errors="coerce")


# INT MONEY
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


# Use cleaning functions
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

    for col, cleaner_fn in cleaners.items():
        df[col] = cleaner_fn(df[col])

    return df


# Rejects
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


# Write out
def write_output(df_clean, df_rejects):
    os.makedirs(DATA_DIR, exist_ok=True)

    df_clean.to_csv(OUTPUT_FILE, index=False)
    df_rejects.to_csv(REJECT_FILE, index=False)


# function to do
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


# run
if __name__ == "__main__":
    ingest_listings()
