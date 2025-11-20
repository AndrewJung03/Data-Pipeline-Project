import pandas as pd

from etl.airbnb_data_ingestion import (
    clean_listing_id,
    clean_name,
    clean_license,
    clean_latitude,
    clean_longitude,
    clean_price,
    clean_minimum_nights,
    clean_num_reviews,
    clean_reviews_per_month,
    clean_calculated_host_count,
    clean_availability_365,
    clean_num_reviews_ltm,
    clean_last_review,
    validate_columns,
    reject_bad_rows
)

def test_validate_columns():
    df = pd.DataFrame({
        "id": [1],
        "name": ["test"]
    })

    try:
        validate_columns(df)
        assert False 
    except ValueError:
        assert True


def test_clean_listing_id():
    df = pd.DataFrame({"listing_id": ["10", "abc"]})
    cleaned = clean_listing_id(df["listing_id"])

    assert cleaned.iloc[0] == 10
    assert pd.isna(cleaned.iloc[1])


def test_clean_name():
    df = pd.DataFrame({"name": ["  Loft  ", None]})
    cleaned = clean_name(df["name"])

    assert cleaned.iloc[0] == "Loft"
    assert cleaned.iloc[1] == "Unknown"


def test_clean_license():
    df = pd.DataFrame({"license": [None]})
    cleaned = clean_license(df["license"])

    assert cleaned.iloc[0] == "Not available"

def test_clean_latitude():
    df = pd.DataFrame({"lat": ["40.7", "bad"]})
    cleaned = clean_latitude(df["lat"])

    assert cleaned.iloc[0] == 40.7
    assert pd.isna(cleaned.iloc[1])


def test_clean_longitude():
    df = pd.DataFrame({"lon": ["-73.9", None]})
    cleaned = clean_longitude(df["lon"])

    assert cleaned.iloc[0] == -73.9
    assert pd.isna(cleaned.iloc[1])

def test_clean_price():
    df = pd.DataFrame({"price": ["$1,200.50", "abc"]})
    cleaned = clean_price(df["price"])

    assert cleaned.iloc[0] == 1200.50
    assert pd.isna(cleaned.iloc[1])

def test_basic_numeric_cleaners():
    df = pd.DataFrame({
        "min_nights": ["10"],
        "reviews": ["5"],
        "reviews_month": ["2.5"],
        "host_count": ["3"],
        "avail": ["150"],
        "reviews_ltm": ["12"]
    })

    assert clean_minimum_nights(df["min_nights"]).iloc[0] == 10
    assert clean_num_reviews(df["reviews"]).iloc[0] == 5
    assert clean_reviews_per_month(df["reviews_month"]).iloc[0] == 2.5
    assert clean_calculated_host_count(df["host_count"]).iloc[0] == 3
    assert clean_availability_365(df["avail"]).iloc[0] == 150
    assert clean_num_reviews_ltm(df["reviews_ltm"]).iloc[0] == 12


def test_clean_last_review():
    df = pd.DataFrame({"date": ["2023-01-01", "not a date"]})
    cleaned = clean_last_review(df["date"])

    assert str(cleaned.iloc[0].date()) == "2023-01-01"
    assert pd.isna(cleaned.iloc[1])


def test_reject_bad_rows():
    df = pd.DataFrame({
        "id": [1, 2],
        "price": ["100", "abc"]
    })

    df["price"] = clean_price(df["price"])
    clean_df, reject_df = reject_bad_rows(df)

    assert len(clean_df) == 1
    assert len(reject_df) == 1
    assert reject_df.iloc[0]["reject_reason"] == "Invalid price"
